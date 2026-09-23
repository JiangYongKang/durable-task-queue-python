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

## 失败分类、重试与终态

任务状态机：`pending → leased → (succeeded | pending | failed | dead)`

- **可重试失败**（`retryable=true`）：回到 `pending` 等待重投；当
  `attempts` 达到 `max_attempts` 时进入 **死信** `dead`。
- **不可重试失败**（`retryable=false`）：直接进入终态 `failed`。
- **终态**：`succeeded`、`failed`、`dead`，终态任务不再被投递。
- 每次投递与失败都会写入事件历史（`submitted`、`leased`、`lease_expired`、
  `succeeded`、`failed_retryable`、`failed_non_retryable`、`dead_lettered`），
  通过 `/tasks/{task_id}/history` 可解释地查看全过程。

## 配置项

均可通过环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TQ_DB_PATH` | `taskqueue.db` | SQLite 数据库文件路径 |
| `TQ_LEASE_SECONDS` | `30` | 租约时长（秒） |
| `TQ_MAX_ATTEMPTS` | `3` | 最大投递尝试次数，达到后进入死信 |
| `TQ_MAX_PAYLOAD_BYTES` | `65536` | 载荷大小上限（字节） |

## 本地验证

运行测试（覆盖重复提交、租约过期重投、多消费者并发竞争、重试死信、
重启持久化；日志打印幂等键、租约与尝试信息）：

```bash
uv run pytest tests/ -v --log-cli-level=INFO
```

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
