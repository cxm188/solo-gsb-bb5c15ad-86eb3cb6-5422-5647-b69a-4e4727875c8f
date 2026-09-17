# Rolling Quota API（多桶滚动配额判定服务）

基于 **Python + FastAPI + SQLite** 的本地配额判定服务：一次请求可携带多个桶键，
在**同一事务**中完成全部桶的检查与扣减——任一超限则全部不扣减，并返回受限桶与最早重试时间。

## 核心特性

- **滚动窗口**：每个桶独立配置限额与窗口（秒），窗口内用量随事件过期滚动释放
- **原子扣减**：单事务内检查 + 扣减全部桶，任一超限则整体不扣减（all-or-nothing）
- **幂等判定**：`request_id` 首次出现时固化载荷与判定；相同内容重放原样返回
  （响应头 `X-Idempotent-Replay: true`），内容变化返回 **409**
- **原子释放**：`POST /v1/quota/releases` 在单事务内撤销一次已放行消费对**所有桶**
  的用量贡献；同一消费只能由一个 `release_id` 释放，重放原样返回
- **并发安全**：SQLite `BEGIN IMMEDIATE` 串行化写事务，并发请求不会突破限额
- **用量查询**：随时查询桶当前窗口用量
- **哈希审计链**：首次判定（放行/拒绝）与首次成功释放均在各自原事务内追加
  一条 SHA-256 哈希链记录，幂等重放不重复记录；提供按 `seq` 翻页查询与全链重算校验
- **配置模拟**：`POST /v1/quota/simulations` 在隔离内存中回放历史消费，
  评估候选桶配置（未覆盖桶沿用当前配置）下的判定翻转与峰值，纯只读、不写审计

## 快速启动

```bash
docker compose up --build
```

单条命令完成构建、依赖安装与初始化（建表 + 从 `buckets.json` 播种桶配置），
服务监听 **8000** 端口，数据持久化在 `quota-data` 卷。

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 桶配置

启动时从 `buckets.json`（可用环境变量 `QUOTA_BUCKETS_CONFIG` 替换）播种，同名桶按文件内容更新：

```json
{
  "buckets": [
    {"key": "api-requests", "limit": 100, "window_seconds": 60},
    {"key": "llm-tokens",   "limit": 10000, "window_seconds": 60},
    {"key": "heavy-jobs",   "limit": 5,   "window_seconds": 300}
  ]
}
```

运行时也可动态创建/更新：`PUT /v1/buckets/{key}`，请求体 `{"limit": 100, "window_seconds": 60}`。

## 示例请求

### 1. 配额判定（检查并扣减）

```bash
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'
```

放行（`200`）：

```json
{"request_id": "req-001", "allowed": true,
 "granted": [{"key": "api-requests", "amount": 1},
             {"key": "llm-tokens", "amount": 500}],
 "decided_at": 1789657657.47}
```

超限（`429`，含 `Retry-After` 响应头；所有桶均未扣减）：

```json
{"request_id": "req-002", "allowed": false,
 "limited_buckets": [{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
                      "used": 9950, "requested": 500,
                      "retry_at": 1789657660.12, "retry_after_seconds": 2.65}],
 "retry_at": 1789657660.12, "retry_after_seconds": 2.65,
 "decided_at": 1789657657.47}
```

> `retry_at` / `retry_after_seconds` 为 `null` 表示请求量本身超过限额，永远不可能放行。

### 2. 幂等语义

```bash
# 重复提交相同内容 → 原样返回首次判定，不重复扣减
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 1},
                   {"key": "llm-tokens",  "amount": 500}]}'

# 相同 request_id 但内容变化 → 409 Conflict
curl -X POST http://localhost:8000/v1/quota/consume \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-001",
       "buckets": [{"key": "api-requests", "amount": 2}]}'
```

### 3. 释放已放行的消费（恢复用量）

```bash
curl -X POST http://localhost:8000/v1/quota/releases \
  -H 'Content-Type: application/json' \
  -d '{"release_id": "release-001", "request_id": "req-001"}'
```

成功（`200`）：在**单个事务**内删除 `req-001` 对所有桶的扣减事件，用量即刻恢复；
不会出现部分桶已恢复、部分未恢复：

```json
{"release_id": "release-001", "request_id": "req-001",
 "released": true,
 "released_buckets": [{"key": "api-requests", "amount": 1},
                      {"key": "llm-tokens", "amount": 500}],
 "events_reverted": 2,
 "released_at": 1789657690.12}
```

释放语义：

| 场景 | 结果 |
| --- | --- |
| 相同 `release_id` + 相同 `request_id` 重放 | `200`，原样返回首次释放结果，响应头 `X-Idempotent-Replay: true`，不重复恢复 |
| 相同 `release_id` 改指其他 `request_id` | `409` |
| 同一消费请求用另一个 `release_id` 再次释放（含并发竞争落败方） | `409` |
| 原 `request_id` 不存在 | `404` |
| 原消费判定为拒绝（`429`，从未扣减） | `409` |

释放后重放原 `consume` 仍返回其已固化的判定（带 `X-Idempotent-Replay: true`），
**不会再次扣减**；恢复出的额度可供新的 `request_id` 使用。

### 4. 查询桶当前窗口用量

```bash
curl http://localhost:8000/v1/buckets/llm-tokens/usage
```

```json
{"key": "llm-tokens", "limit": 10000, "window_seconds": 60,
 "used": 500, "remaining": 9500,
 "window_start": 1789657597.55, "as_of": 1789657657.55,
 "oldest_event_expires_at": 1789657717.47}
```

### 5. 哈希审计链

每次**首次消费判定**（放行 `200` 或拒绝 `429`）与**首次成功释放**，都在
各自的原写事务内（同提交、同回滚）向仅追加的 `audit_log` 追加一条记录；
幂等重放直接返回固化结果，**不重复记录**；失败的释放（`404`/`409`）
事务回滚，也不会留痕。

链规则：

- 记录使用全局递增 `seq`（从 1 开始）；
- 首条记录的 `prev_hash` 为 64 个 `0`，其后每条引用上一条的 `hash`；
- `hash = SHA-256(prev_hash 的原始 32 字节 + UTF-8 canonical JSON 事件)`；
- 事件 canonical JSON 键排序、无多余空白，固定包含：

| 字段 | 含义 |
| --- | --- |
| `op` | 操作类型：`consume`（消费判定）/ `release`（成功释放） |
| `business_id` | 业务 ID：`consume` 取 `request_id`，`release` 取 `release_id` |
| `result` | 结果：`allowed` / `denied` / `released` |
| `occurred_at` | 发生时间（epoch 秒，与判定/释放时刻一致） |

按 `seq` 翻页查询（游标为上次最后一条的 `seq`，响应同时给出当前链头）：

```bash
curl "http://localhost:8000/v1/audit?after_seq=0&limit=100"
```

```json
{
  "records": [
    {"seq": 1,
     "prev_hash": "0000000000000000000000000000000000000000000000000000000000000000",
     "hash": "c979be39953f1ec4c07b26cbb9247815e7cb7584089d4ab5d33b401068203a4d",
     "event": {"business_id": "req-001", "occurred_at": 1789657657.47,
               "op": "consume", "result": "allowed"}},
    {"seq": 2,
     "prev_hash": "c979be39953f1ec4c07b26cbb9247815e7cb7584089d4ab5d33b401068203a4d",
     "hash": "5e8de1e930da96b784b8bb76a8a1bd9d4c5d1e18ecae2977401cbc5b60fce505",
     "event": {"business_id": "release-001", "occurred_at": 1789657690.12,
               "op": "release", "result": "released"}}
  ],
  "head": {"seq": 2, "hash": "5e8de1e930da96b784b8bb76a8a1bd9d4c5d1e18ecae2977401cbc5b60fce505"},
  "next_after_seq": 2
}
```

- `after_seq`：仅返回 `seq` 大于该值的记录，默认 `0`，必须 `>= 0`；
- `limit`：每页条数，默认 `100`，范围 `1..1000`；
- 空链的链头为 `{"seq": 0, "hash": "0000…0000"}`（64 个 0）。

全链重算校验（逐条核对 `seq` 连续性、`prev_hash` 链接与 SHA-256 重算值）：

```bash
curl http://localhost:8000/v1/audit/verify
```

```json
{"valid": true, "checked_count": 2,
 "head_hash": "5e8de1e930da96b784b8bb76a8a1bd9d4c5d1e18ecae2977401cbc5b60fce505",
 "first_invalid_seq": null}
```

发现篡改/断链时 `valid` 为 `false`，`first_invalid_seq` 给出最早失配记录的 `seq`。

### 6. 配额配置模拟（What-if 回放，不写库）

`POST /v1/quota/simulations` 对一段历史时间做"假如当时使用候选桶配置"的
离线推演：在**隔离内存**中按 `(created_at, request_id)` 稳定顺序回放区间前
的预热消费与区间内的首次消费，比较每条消费的**原始固化判定**与**模拟重算
判定**。全程只读数据库，**不写任何数据、不追加审计链**；release 不回放
（已释放的消费视为从未占额）。

请求体：

| 字段 | 说明 |
| --- | --- |
| `from` / `to` | epoch 秒半开区间 `[from, to)`；`from >= to` 或跨度超过 **7 天** 返回 `422` |
| `buckets` | 候选配置数组，每项 `{"key", "limit", "window_seconds"}`；**未覆盖桶沿用当前配置**；未知桶、同桶重复给出候选返回 `422` |

预热期：从 `from` 向前取"各桶（候选覆盖后的）窗口最大值"作为预热长度，
`[from - max_window, from)` 内的消费只用于建立滚动窗口状态，**不计任何统计**。

```bash
curl -X POST http://localhost:8000/v1/quota/simulations \
  -H 'Content-Type: application/json' \
  -d '{"from": 1789650000, "to": 1789653600,
       "buckets": [{"key": "llm-tokens", "limit": 5000, "window_seconds": 60}]}'
```

```json
{
  "from": 1789650000,
  "to": 1789653600,
  "warmup_from": 1789649700,
  "transitions": {
    "allowed_to_allowed": 132,
    "allowed_to_denied": 7,
    "denied_to_allowed": 2,
    "denied_to_denied": 1
  },
  "peak_usage": [
    {"key": "api-requests", "limit": 100, "window_seconds": 60, "peak_used": 84},
    {"key": "llm-tokens", "limit": 5000, "window_seconds": 60, "peak_used": 4950}
  ],
  "flip_count": 9,
  "flip_details": [
    {"request_id": "req-021", "created_at": 1789650612.83,
     "original": "allowed", "simulated": "denied",
     "buckets": [{"key": "llm-tokens", "amount": 500}]},
    {"request_id": "req-044", "created_at": 1789651220.07,
     "original": "denied", "simulated": "allowed",
     "buckets": [{"key": "llm-tokens", "amount": 200}]}
  ]
}
```

- `transitions`：按 `allowed→allowed`、`allowed→denied`、`denied→allowed`、
  `denied→denied` 顺序给出区间内消费的原始/模拟判定计数；
- `peak_usage`：每桶在模拟世界中的区间滚动窗口峰值用量（仅列出区间内
  被请求过的桶）；
- `flip_details`：判定发生翻转的消费明细，按回放顺序（`created_at`、再
  `request_id` 升序）截取的**前 100 条**；`flip_count` 为翻转总数（可能大于
  返回条数）。多桶请求仍遵循 all-or-nothing：任一桶超限则全部不放行。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/quota/consume` | 多桶检查 + 扣减；`200` 放行 / `429` 超限 / `409` 幂等冲突 / `404` 桶未配置 |
| `POST` | `/v1/quota/releases` | 释放一次已放行消费；`200` 成功或重放 / `409` 冲突 / `404` 原请求不存在 |
| `POST` | `/v1/quota/simulations` | 候选桶配置的区间历史回放（只读内存模拟）；`200` 结果 / `422` 未知桶、非法范围、跨度超 7 天或候选重复 |
| `GET` | `/v1/buckets/{key}/usage` | 查询桶当前滚动窗口用量 |
| `PUT` | `/v1/buckets/{key}` | 创建或更新桶（`limit`、`window_seconds`） |
| `GET` | `/v1/buckets` | 列出全部桶配置 |
| `GET` | `/v1/audit?after_seq=&limit=` | 按 `seq` 升序返回审计记录与链头 |
| `GET` | `/v1/audit/verify` | 全链重算，返回 `valid`/`checked_count`/`head_hash`/`first_invalid_seq` |
| `GET` | `/healthz` | 健康检查 |

交互式文档：`http://localhost:8000/docs`

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `QUOTA_DB_PATH` | `quota.db`（镜像内 `/data/quota.db`） | SQLite 数据库文件路径 |
| `QUOTA_BUCKETS_CONFIG` | `buckets.json`（镜像内 `/app/buckets.json`） | 启动时播种的桶配置文件，置空跳过 |
| `QUOTA_BUSY_TIMEOUT_MS` | `30000` | SQLite 写事务排队等待毫秒数 |

## 实现说明

- **原子性**：`consume` 在单个 `BEGIN IMMEDIATE` 事务内完成"读窗口用量 → 判定 →
  写扣减事件 → 固化判定"，任一环节失败整体回滚。`release` 同理：在单个写事务内
  完成"校验原请求 → 删除其全部 `usage_events` → 标记 `requests.release_id`
  → 写入 `releases` 释放记录"，保证所有桶同时恢复。
- **并发**：`BEGIN IMMEDIATE` 使写事务在数据库级串行化；WAL 模式保证读（用量查询）不阻塞。
  多个不同 `release_id` 并发释放同一消费时，事务串行化 + 占用标记保证仅一个成功，
  其余返回 `409`。
- **幂等**：`requests` 表以 `request_id` 为主键，固化载荷 SHA-256 与判定 JSON；
  重放命中时直接返回，哈希不一致返回 409。`releases` 表以 `release_id` 为主键固化
  目标 `request_id` 与释放结果：同目标重放原样返回并带 `X-Idempotent-Replay: true`，
  改指其他目标返回 409。
- **审计链**：`audit_log` 为仅追加表，`seq` 由 SQLite AUTOINCREMENT 全局递增。
  首次判定在固化 `requests` 之后、首次成功释放在写入 `releases` 之后，于**同一
  `BEGIN IMMEDIATE` 事务**内追加审计记录——业务回滚则审计不生效，二者不可能分离；
  幂等重放走不写入审计的独立分支。追加时先读链尾 `hash` 作为 `prev_hash`，按
  `SHA-256(bytes.fromhex(prev_hash) + canonical_json(event))` 计算当前 `hash`，
  写事务串行化保证链头读取与追加之间无并发分叉。`GET /v1/audit/verify` 从 64 个 0
  的创世前序哈希起逐条重算，同时核对 `seq` 连续性，报告首个失配位置。
- **最早重试时间**：按窗口内事件过期时刻累计释放量，找到最早可容纳本次请求的过期点；
  多桶受限时取各桶的最大值。
- **配置模拟**：`POST /v1/quota/simulations` 不开写事务，只用只读连接读取
  `buckets` 与 `requests`（含固化载荷、原始判定、`release_id`），在
  `app/simulation.py` 的纯内存状态机中重算滚动过期与多桶 all-or-nothing
  判定。预热起点取所有已配置桶候选/当前窗口的最大值，预热记录只更新状态、
  不计统计；`release_id` 非空的已释放消费跳过（release 不回放）。接口对
  数据库与审计链零写入，重复调用结果一致。

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/ -q
```

覆盖：原子扣减、幂等重放（含拒绝判定重放）、409 冲突、滚动窗口过期恢复、
50 并发不超限额、同 `request_id` 并发只扣减一次，以及释放接口的多桶原子恢复、
幂等重放、改指冲突、重复释放冲突、404/409 分支、释放后 consume 重放不再次扣减、
20 并发释放仅一个成功；此外覆盖哈希审计链的哈希规则纯函数、首条锚定 64 个 0、
放行/拒绝/成功释放仅记一次且重放不重复、失败释放与 409/404 不留痕、分页与链头、
全链校验有效与篡改检出（事件内容/前序指针）等用例；`tests/test_simulation.py`
另覆盖配置模拟的 422 校验（未知桶/非法区间/超 7 天/候选重复）、空区间、
收紧/放宽限额带来的四类判定转移、预热期只建状态不计统计与边界过期、
区间前记录忽略、模拟内滚动过期、多桶全放行/全拒绝、未覆盖桶沿用当前配置、
翻转明细按 `(created_at, request_id)` 排序并截断 100 条、已释放消费不回放，
以及重复模拟不写库/不追加审计链，共 56 个用例。
