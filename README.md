# Durable Local Task Queue（可持久化本地任务队列）

一个可本地运行的任务队列服务：通过 HTTP 对外提供能力，用 **SQLite（WAL 模式）**
模拟消息代理，提供：

- 任务提交与**幂等去重**（相同幂等键 + 相同载荷不产生新任务；键相同载荷不同/
  载荷非法/超限分别给出可区分的拒绝原因）；
- 多消费者**并发领取与租约管理**（同一时刻一个任务最多一个有效租约；租约超时或
  消费者崩溃后自动重投；不会重复成功、不会丢任务）；
- **失败分类与重试终态**（可重试失败指数退避重试，达上限进死信；不可重试失败
  直接终态；重启后状态与尝试次数不丢、不重复计数）；
- 每次投递都有独立历史记录，查询结果可解释。

## 架构

```
HTTP API (FastAPI)
   └── TaskQueue            提交去重 / 租约 / 完成 / 失败 / 回收 / 查询
          └── TaskStore     SQLite，WAL + synchronous=FULL，进程锁串行化事务
```

- 所有状态迁移在同一把进程锁 + 一个 `BEGIN IMMEDIATE` 事务内完成，因此即使多个
  消费者线程并发竞争，"同一任务同时最多一个有效租约"也严格成立。
- 每次 `COMMIT` 都落盘（`synchronous=FULL`），进程重启后状态、尝试次数、投递
  历史完整保留。
- 后台**租约回收线程**（间隔 `reaper_interval`）扫描过期租约；也可调用
  `POST /tasks/reclaim` 立即回收（测试/运维用）。

## 任务状态机

| 状态 | 含义 |
| --- | --- |
| `pending` | 已提交，等待首次投递 |
| `inflight` | 已授出租约，等待消费者回报 |
| `retrying` | 可重试失败后等待退避窗口结束再投递 |
| `succeeded` | 终态：恰好成功一次 |
| `dead` | 终态：不可重试失败，或重试次数耗尽（死信） |

```
pending ──lease──▶ inflight ──complete──────────────▶ succeeded (终态)
                      │  ▲                            
                      │  └────────── heartbeat（续租） 
                      ├─ fail(permanent) ───────────▶ dead (终态)
                      ├─ fail(retryable), 未达上限 ─▶ retrying ──到期 lease──▶ inflight
                      ├─ fail(retryable), 已达上限 ─▶ dead (终态, retry_exhausted)
                      └─ 租约超时回收, 未达上限 ───▶ retrying
                         租约超时回收, 已达上限 ───▶ dead (终态, timeout_exhausted)
```

## 投递语义（Delivery Semantics）

**至少一次投递 + 单次有效租约确认（at-least-once with lease ack）。**

1. 领取任务时服务端生成唯一 `lease_id`，记录租约归属消费者与过期时间，任务进入
   `inflight`，`attempt_count += 1`，并追加一条 `deliveries` 历史。
2. 消费者必须凭**当前有效的** `lease_id` 回报成功/失败/心跳：
   - 租约 ID 不匹配（旧租约、伪造租约）→ `409 lease_conflict`；
   - 租约已过期并被回收后再回报 → 同样 `409`。
   因此"崩溃后复活的僵尸消费者"用旧租约 ack 不会导致重复成功。
3. 租约到期未回报：该次投递记为 `timeout`/`timeout_exhausted`，尝试次数照常
   计数；未达上限任务回到可投递队列（立即可再领取），达到上限进入 `dead`。
4. 心跳 `heartbeat` 会把过期时间向后顺延一个 `visibility_timeout`。
5. 成功只有一次：任务一旦 `succeeded` 或 `dead` 不会再被领取。

> 语义边界：进程内多线程并发由进程锁严格串行化保证；多进程同时写同一个数据库
> 文件不在本地单机模拟范围内（SQLite WAL 仍保证不损坏，但跨进程领取竞争依赖文件
> 锁，生产级场景请使用真正的 broker）。

## 幂等提交与拒绝原因

- 请求携带 `idempotency_key` 时按"键 + 载荷哈希（SHA-256）"判定：
  - 键不存在 → 创建任务，`201 {"created": true}`；
  - 键存在且**载荷完全相同** → 直接返回原任务，`200 {"created": false}`，
    不新增任务、不增加尝试次数；
  - 键存在但**载荷不同** → 拒绝 `409`，`reason=idempotency_key_payload_mismatch`。
- 载荷本身必须是 **JSON 对象**（非空）：
  - 非法 JSON / 非对象 / 空 → `422 reason=invalid_payload`；
  - 超过 `max_payload_bytes` → `422 reason=payload_too_large`。
- 不传 `idempotency_key` 时服务端自动生成，等价于每次都创建新任务。

## 失败分类与重试规则

| 回报 `kind` | 条件 | 结果 |
| --- | --- | --- |
| `permanent` | 任意次数 | 直接 `dead`，历史记 `permanent_failure` |
| `retryable` | `attempt_count < max_attempts` | 进入 `retrying`，历史记 `retryable_failure` |
| `retryable` | `attempt_count == max_attempts` | 进入 `dead`，历史记 `retry_exhausted` |
| （消费者无回报） | 租约超时，未达上限 | 回到队列，历史记 `timeout` |
| （消费者无回报） | 租约超时，已达上限 | `dead`，历史记 `timeout_exhausted` |

退避（仅主动回报的可重试失败）：

```
delay = retry_backoff_base * 2 ** (attempt - 1)      # attempt 从 1 开始
```

超时回收不做退避（崩溃场景应尽快重投）。每次投递（含超时）都消耗一次尝试预算，
这保证"消费者反复崩溃"不会造成无限重投。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务；body：`{"payload": {...}, "idempotency_key": "可选"}` |
| POST | `/tasks/lease` | 领取任务；body：`{"consumer":"可选","wait_timeout":0}`，无任务返回 `204` |
| POST | `/tasks/{id}/complete` | 回报成功；body：`{"lease_id":"..."}` |
| POST | `/tasks/{id}/fail` | 回报失败；body：`{"lease_id":"...","kind":"retryable|permanent","error":"可选"}` |
| POST | `/tasks/{id}/heartbeat` | 续租；body：`{"lease_id":"..."}` |
| POST | `/tasks/reclaim` | 立即执行一次过期租约回收，返回 `{"reclaimed": n}` |
| GET  | `/tasks/{id}` | 查询完整状态与投递历史 |
| GET  | `/tasks/key/{idempotency_key}` | 按幂等键查询 |
| GET  | `/healthz` | 健康检查 |

任务视图包含 `state`、`attempt_count`、`attempts_allowed`、`available_at`、
当前 `lease` 以及 `history`（每次投递的 attempt、lease_id、consumer、时间、
result、failure_kind、error）。

## 配置项

| 配置 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `db_path` | `TASKQUEUE_DB_PATH` | `taskqueue.db` | SQLite 文件路径 |
| `visibility_timeout` | `TASKQUEUE_VISIBILITY_TIMEOUT` | `30.0` | 租约有效期（秒） |
| `max_attempts` | `TASKQUEUE_MAX_ATTEMPTS` | `3` | 最大投递尝试次数 |
| `retry_backoff_base` | `TASKQUEUE_RETRY_BACKOFF_BASE` | `1.0` | 指数退避基数（秒） |
| `max_payload_bytes` | `TASKQUEUE_MAX_PAYLOAD_BYTES` | `65536` | 载荷大小上限（字节） |
| `reaper_interval` | `TASKQUEUE_REAPER_INTERVAL` | `5.0` | 后台回收扫描间隔（秒） |

## 本地运行与验证

前置：Python 3.14 + [uv](https://docs.astral.sh/uv/)（首次运行会自动装依赖）。

```bash
# 1) 启动服务（短租约便于观察，交互式文档 http://127.0.0.1:8000/docs）
uv run python main.py --visibility-timeout 2 --max-attempts 3 --backoff-base 1

# 2) 提交任务（幂等键 order-1）
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"payload":{"order":"o-123"},"idempotency_key":"order-1"}'

# 3) 消费者领取（拿到 lease_id）
curl -s -X POST localhost:8000/tasks/lease \
  -H 'Content-Type: application/json' -d '{"consumer":"worker-1"}'

# 4) 回报成功（把 <id> / <lease_id> 替换为上一步返回值）
curl -s -X POST localhost:8000/tasks/<id>/complete \
  -H 'Content-Type: application/json' -d '{"lease_id":"<lease_id>"}'

# 4') 或回报可重试失败：
curl -s -X POST localhost:8000/tasks/<id>/fail \
  -H 'Content-Type: application/json' \
  -d '{"lease_id":"<lease_id>","kind":"retryable","error":"transient"}'

# 5) 查询状态与完整投递历史
curl -s localhost:8000/tasks/<id>
curl -s localhost:8000/tasks/key/order-1

# 6) 重复提交相同载荷（200, created=false）/ 不同载荷（409 拒绝）
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8000/tasks \
  -H 'Content-Type: application/json' -d '{"payload":{"order":"o-123"},"idempotency_key":"order-1"}'

# 7) 不回报、等待超过 visibility-timeout，租约会被后台线程自动回收并重投；
#    达到 max-attempts 后进入 dead。删除 db 文件即可重置全部数据。
```

### 运行测试

```bash
uv run pytest -s -v          # -s 可看到幂等键/租约/尝试次数日志
```

测试覆盖：

- `tests/test_idempotency.py`：重复提交去重、载荷不一致/非法/超限的可区分拒绝；
- `tests/test_lease.py`：租约排他、过期重投、心跳续租、旧租约 ack 拒绝；
- `tests/test_retry.py`：失败分类、退避重试、重试耗尽入死信、不可重试直接终态、
  重启后尝试次数不丢不重；
- `tests/test_concurrency.py`：6 消费者并发领取 50 个任务（每任务恰好一次）、
  8 线程并发重复提交（只创建一个任务）、多消费者多轮重投竞争（尝试次数严格一致）。

单测运行时会打印形如：

```
[lease] idempotency_key=concurrent-0 lease_id=fb0c... consumer=c5 attempt=1 expires_at=...
[fail]  idempotency_key=... lease_id=... kind=retryable attempt=2/3 state=retrying backoff=0.02
[reap]  idempotency_key=... lease_id=... attempt=3/3 state=dead
```

## 数据存储

SQLite 文件（默认 `taskqueue.db`，附带 `-wal`/`-shm`）包含两张表：

- `tasks`：当前状态、幂等键、载荷及其 SHA-256 哈希、尝试次数、租约字段；
- `deliveries`：每次投递的不可变记录（领取/心跳/成功/失败/超时）。

仅追加投递记录、任务行随状态迁移更新，因此历史始终可解释。
