# Durable Task Queue (Python)

一套可本地运行、可持久化的任务队列服务。通过 HTTP 接口对外提供能力，使用本地
SQLite（WAL 模式 + 事务）模拟消息代理，保证多个消费者并发取任务时投递、重试与
去重的结果一致且可解释。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务，监听 127.0.0.1:8000
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务，Body: `{"idempotency_key": "...", "payload": {...}}` |
| POST | `/tasks/acquire` | 领取下一个可投递任务（无任务时返回 204） |
| POST | `/tasks/{task_id}/ack` | 确认成功，Body: `{"lease_id": "..."}` |
| POST | `/tasks/{task_id}/fail` | 报告失败，Body: `{"lease_id": "...", "retryable": true}` |
| GET  | `/tasks/{task_id}` | 查询任务状态与尝试次数 |
| GET  | `/tasks/{task_id}/history` | 查询该任务的完整事件历史 |

## 投递语义

- **至少一次投递（at-least-once）**：任务被领取后进入 `leased` 状态并绑定唯一
  `lease_id`；只有持有效租约的消费者才能 `ack`/`fail`。
- **同一时刻最多一个有效租约**：租约的获取、确认与失败都在
  `BEGIN IMMEDIATE` 事务中完成，并发消费者不会领到同一任务的同一批投递。
- **租约过期重投**：消费者超时或异常退出（未 ack/fail）时，租约到期后任务自动
  变为可再次投递，事件历史记录 `lease_expired`。过期租约的 ack/fail 会被拒绝
  （HTTP 409，`lease_invalid`），因此不会出现重复成功或任务丢失。
- **尝试计数**：每次投递（领取）`attempts` 加 1，持久化保存，重启后不丢失、
  不重复计数。

## 去重与提交校验

提交时按幂等键去重，拒绝原因可区分（响应体含 `error` 字段）：

| 情况 | 结果 |
| --- | --- |
| 相同幂等键 + 相同载荷 | 返回已有任务，HTTP 200，`created=false`，不产生新任务 |
| 相同幂等键 + 不同载荷 | HTTP 409，`error=payload_conflict` |
| 幂等键为空或非字符串 | HTTP 422，`error=invalid_idempotency_key` |
| 载荷无法 JSON 序列化 | HTTP 422，`error=invalid_payload` |
| 载荷超过大小限制 | HTTP 413，`error=payload_too_large` |

载荷以规范 JSON（键排序）存储后比较，保证语义相同的载荷判定为重复。

## 失败分类、重试调度与终态

任务状态机：

```
pending ─► leased ─► succeeded
             │
             ├─ 不可重试失败 ─► failed（终态）
             ├─ 可重试失败且 attempts 达到上限 ─► dead（终态）
             └─ 可重试失败且仍可重试 ─► waiting
                      （not_before 到期后留在 waiting 但进入可投递集合，
                        被领取时直接转为 leased，无需后台任务搬运）
```

- **可重试失败带等待期（退避调度）**：`retryable=true` 且未达上限时，任务进入
  `waiting` 状态并记录 `not_before`（下次可领取时间戳）。在该时间之前任何消费者都
  领不到；到期后自动重新变为可投递，下一次领取照常把 `attempts` 加 1。
- **逐渐变长、但有上限的等待**：第 `n` 次可重试失败后的等待时长为

  ```
  delay(n) = min(retry_initial_delay * retry_backoff_factor ** (n - 1),
                 retry_max_delay)
  ```

  即初始时长与上限均可配置，默认 `initial=0s`、`factor=2`、`max=60s`，等待序列
  为 0、0、…（见下方兼容性说明）。配置为例如 `initial=1s、factor=2、max=10s` 时，
  各次失败后等待约 1s、2s、4s、8s、10s、10s……
- **等待不阻塞新任务**：领取按 `created_at` 顺序在“当前可投递集合”中选择，
  可投递集合 = `pending`（新提交、或初始延迟为 0 的立即重试）∪
  `waiting 且 not_before 已到期` ∪ `leased 且租约已过期`。到期的 waiting 任务在被
  领取时直接转为 `leased`（不依赖后台扫描搬运）；还在等待期的任务会被跳过，不会
  把排在它后面、已经可以投递的新任务挡在身后。
- **不可重试失败**（`retryable=false`）：直接进入终态 `failed`，规则不变。
- **达到最大尝试次数**：仍在第 `max_attempts` 次投递失败时进入死信 `dead`，
  规则不变（等待期只影响“两次投递之间”的间隔，不增加尝试次数）。
- **终态**：`succeeded`、`failed`、`dead`，终态任务不再被投递。

### 等待期可观测

- `GET /tasks/{task_id}` 在任务处于 `waiting` 时返回
  `"state": "waiting"` 与绝对时间戳 `"not_before"`；`attempts` 即当前已失败/
  已投递次数。
- `GET /tasks/{task_id}/history` 中：
  - `failed_retryable` 事件详情包含 `failure_count`（第几次失败）、
    `delay`（本次调度等待时长）、`not_before`（下次可领时间）；
  - 等待到期后重新投递产生的 `leased` 事件详情包含
    `retry_after_failure`、`scheduled_delay`（计划等待）与 `waited`
    （从失败到再次被领取的实际耗时，集中到期竞争时可能略大于计划值），
    方便排查“任务为什么迟迟没动”。
- 完整事件类型：`submitted`、`leased`、`lease_expired`、`succeeded`、
  `failed_retryable`、`failed_non_retryable`、`dead_lettered`。

### 并发、重复上报与重启一致性

- 到期瞬间多个消费者同时领取，仍然由 `BEGIN IMMEDIATE` 事务串行化，同一任务只会
  被一个消费者领到，`attempts` 只加一次。
- 同一次失败被重复上报、或租约过期之后才上报：由于上报时旧租约已失效，`fail`
  被拒绝（HTTP 409 `lease_invalid`），不会重复计数，也不会把同一次失败重复排队。
- 等待进度（`state=waiting` 与 `not_before`）持久化在 SQLite 中，服务重启后既不
  会提前也不会推迟投递；集中到期的大量任务结果与不重启时一致。

## 配置项

均可通过环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TQ_DB_PATH` | `taskqueue.db` | SQLite 数据库文件路径 |
| `TQ_LEASE_SECONDS` | `30` | 租约时长（秒） |
| `TQ_MAX_ATTEMPTS` | `3` | 最大投递尝试次数，达到后进入死信 |
| `TQ_MAX_PAYLOAD_BYTES` | `65536` | 载荷大小上限（字节） |
| `TQ_RETRY_INITIAL_DELAY` | `0` | 第 1 次可重试失败后的等待秒数（初始退避） |
| `TQ_RETRY_MAX_DELAY` | `60` | 单次等待秒数上限（退避封顶） |
| `TQ_RETRY_BACKOFF_FACTOR` | `2` | 退避乘数，每多失败一次等待时长乘以此系数（≥1） |

### 与原有行为的兼容范围

- **默认配置即旧行为**：`TQ_RETRY_INITIAL_DELAY=0` 时，可重试失败的等待时长为 0，
  任务直接回到 `pending`、立即重新可投递（与升级前完全一致，不经过 `waiting`）；
  既有用例（立即重试、租约过期重投、死信、不可重试终态）全部保持通过。
- 开启等待期后，变化仅限于**两次投递之间的间隔**与可观测字段：任务会先进入
  `waiting`，任务视图新增 `not_before`，失败/重投事件详情新增等待信息。
  幂等去重、租约语义、`attempts` 计数口径、终态判定（`failed`/`dead`）均不变。
- 旧版本数据库可直接启动：启动时自动为 `tasks` 表补齐 `not_before` 列
  （存量行该列为空，视为立即可投递），无需手工迁移。
- 等待时长必须满足 `0 ≤ initial ≤ max` 且 `factor ≥ 1`，非法配置在构造
  `QueueConfig` 时即抛出 `ValueError`。

## 本地验证

运行测试（覆盖重复提交、租约过期重投、多消费者并发竞争、重试死信、
重启持久化；以及等待期内不投递、到期投递、退避递增与封顶、等待不挡新任务、
集中到期并发领取、失败重复上报、租约过期后上报、重启后等待进度保持；
日志打印幂等键、租约、尝试次数与等待时长）：

```bash
uv run pytest tests/ -v --log-cli-level=INFO
```

等待/退避调度用的是可控时钟（`tests/test_retry_schedule.py`），不依赖真实
sleep，运行快速且确定。

手动端到端验证：

```bash
uv run python main.py &
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"demo-1","payload":{"n":1}}'
curl -X POST localhost:8000/tasks/acquire
# 用返回的 lease_id 确认：
curl -X POST localhost:8000/tasks/<task_id>/ack \
     -H 'content-type: application/json' -d '{"lease_id":"<lease_id>"}'
curl localhost:8000/tasks/<task_id>/history
```

手动验证等待期重试（短延迟，便于观察）：

```bash
TQ_RETRY_INITIAL_DELAY=2 TQ_RETRY_MAX_DELAY=8 TQ_RETRY_BACKOFF_FACTOR=2 \
    TQ_DB_PATH=/tmp/tq-demo.db uv run python main.py &

curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"backoff-demo","payload":{"n":1}}'
curl -X POST localhost:8000/tasks/acquire           # 第 1 次领取 attempts=1
curl -X POST localhost:8000/tasks/<task_id>/fail \
     -H 'content-type: application/json' \
     -d '{"lease_id":"<lease_id>","retryable":true}'
# 响应中 state="waiting"、not_before 为约 2 秒后的时间戳
curl -X POST localhost:8000/tasks/acquire           # 2 秒内返回 204
curl localhost:8000/tasks/<task_id>                 # 可查 waiting 与 not_before
# 等待 2 秒后：
curl -X POST localhost:8000/tasks/acquire           # 到期重投 attempts=2
curl localhost:8000/tasks/<task_id>/history         # 可见 failure_count/delay/waited
```
