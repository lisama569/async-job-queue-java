# Async Job Queue（异步任务处理队列 · Spring Boot）

一个可本地跑起来、整条链路可观测可验证的异步任务处理服务：

**提交 → 排队 → 分组串行调度 → 执行 → 失败指数退避重试 → 重试耗尽进入死信 → 进程崩溃重启恢复**

- 提交 / 查询状态 / 取消
- 幂等键去重：同一幂等键只产生一次执行
- 分组串行：同一 `groupKey` 内严格按提交顺序串行，不同分组并行
- 有限次数 + 递增间隔自动重试，耗尽后进入死信并保留可区分原因
- 持久化（H2 文件库）：进程重启后排队任务不丢，中断的运行中任务重投（at-least-once）
- 真实生效的并发上限与队列容量；打满时给调用方明确背压（HTTP 503），不静默丢任务
- 所有状态迁移、重试原因、任务标识均打印日志；测试断言最终状态与真实执行次数

## 技术栈

- Java 21（虚拟线程承载调度协程）、Spring Boot 3.4、Spring Web、Spring JDBC
- H2 数据库（本地文件库，零外部依赖；可平滑换成其它 JDBC 库）
- JUnit 5、AssertJ、Awaitility

## 快速开始

```bash
mvn spring-boot:run
# 或
mvn -DskipTests package && java -jar target/async-job-queue-java-1.0-SNAPSHOT.jar
```

服务默认监听 `8080`，H2 文件库位于 `./data/asyncjob*`（删除该目录即可重置所有任务）。

## HTTP 接口

所有任务围绕 `/api/jobs`。

### 1. 提交任务

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "order-123-pay",
    "groupKey": "order-123",
    "jobType": "echo",
    "payload": "hello",
    "maxAttempts": 4
  }'
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `idempotencyKey` | 否 | 幂等键。相同键的重复提交返回同一个任务，`duplicate=true`，**绝不产生第二次执行**。并发同键提交由数据库唯一索引兜底折叠为一条。 |
| `groupKey` | 是 | 分组键。同组严格按提交顺序串行执行，不同分组可并行。 |
| `jobType` | 是 | 处理器类型，内置示例处理器为 `echo`。 |
| `payload` | 否 | 业务负载（字符串），原样传给处理器。 |
| `maxAttempts` | 否 | 最大执行次数（含首次）。缺省取 `async-job.default-max-attempts`。 |

响应：首次提交 `201 Created`；幂等命中 `200 OK` 且 `duplicate=true`。

内置 `echo` 处理器的 payload 约定（便于本地验证）：

- `always-fail:<任意>`：每次都失败（验证重试与死信）
- `fail-2:<任意>`：前 2 次失败、第 3 次成功（验证重试后成功）
- `sleep:<毫秒>`：睡眠指定毫秒后成功（制造在途任务，验证背压 / 重启恢复）
- 其它：直接成功

### 2. 查询状态

```bash
curl http://localhost:8080/api/jobs/{id}
```

返回字段含 `status`、`attempts`、`maxAttempts`、`nextRetryInMillis`、`lastError`、
`failureReason`、`version`、各阶段时间戳。不存在返回 `404`。

### 3. 取消任务

```bash
curl -X POST http://localhost:8080/api/jobs/{id}/cancel
```

仅 `PENDING`（排队中）和 `RETRY_WAITING`（重试等待中）可取消，取消后进入终态 `CANCELLED`。
`RUNNING` 不抢占（业务副作用不可知），返回当前状态，最终结果由执行决定。
不存在返回 `404`。

### 4. 死信列表

```bash
curl 'http://localhost:8080/api/jobs/dead?limit=50'
```

列出进入死信的任务及其 `failureReason`。

### 错误与背压

- 容量打满：`503 Service Unavailable`，响应体 `{"error":"QUEUE_FULL","retryable":true,...}`，
  调用方应稍后重试；任务**不会**被静默接受后丢弃。
- 参数非法：`400 Bad Request`。
- 任务不存在：`404 Not Found`。

## 状态流转规则

```
                取消
PENDING ───────────────────────► CANCELLED（终态）
  │  ▲                            
  │  │ 进程重启：上次中断的 RUNNING 退回 PENDING 重投
  ▼  │
RUNNING ──成功──────────────────► SUCCEEDED（终态）
  │
  ├─失败且未达上限─► RETRY_WAITING ──退避到点─► RUNNING
  │        │
  │        └──取消─► CANCELLED（终态）
  │
  └─失败且达上限 / 不可重试错误─► DEAD（终态，进入死信）
```

- `attempts` 在每次进入 `RUNNING` 时 +1，即“已经发生的执行次数”。
- 所有迁移都是带 `version` 乐观锁的 CAS：`UPDATE ... WHERE id=? AND version=? AND status IN (?)`，
  成功后 `version+1`。因此并发提交 / 查询 / 取消 / 关闭重叠时，**不会状态回退、不会丢失更新**。
- 终态（SUCCEEDED / CANCELLED / DEAD）不可逆。

## 重试与死信策略

- 最大执行次数 `maxAttempts`（含首次执行）。第 1 次执行失败后，只要
  `attempts < maxAttempts` 就进入 `RETRY_WAITING`；否则进入 `DEAD`。
- 递增退避（指数退避，封顶）：

  ```
  第 1 次重试等待 = backoff-initial-millis
  之后每次        = min(上次等待 × backoff-multiplier, backoff-max-millis)
  ```

  默认 200ms 起、倍率 2.0、封顶 5000ms。
- 重试由独立的 `RetryScheduler` 按 `retry-tick-millis` 周期扫描到点任务拉起。
- 进入死信时保留：
  - `failureReason`：人类可读、可区分原因（如“重试耗尽(4/4)…”或“不可重试错误，直接进入死信…”）；
  - `lastError`：最后一次异常的类型与消息（如 `IllegalStateException: ...`）。
- 未知 `jobType` 属于不可重试错误，首次执行即进入死信。

## 并发与背压语义

- `async-job.max-concurrency`：全局同时处于 `RUNNING` 的任务上限，用公平 `Semaphore` 实现，**真实生效**。
- `async-job.max-concurrency` 与 `async-job.queue-capacity` 共同决定系统在途容量。
  一个原子计数器统计“已接受但未终态（PENDING + RUNNING + RETRY_WAITING）”的任务数；
  达到容量后新提交立即得到 `QueueFullException`（HTTP 503）。
- 分组实现：每个 `groupKey` 一条虚拟线程协程，组内用对象锁 + 数据库
  `SELECT ... FOR UPDATE` 领取该组最早的 PENDING，保证**严格串行、按提交顺序**；
  不同分组协程互不阻塞、共享全局并发许可，因此**组间并行**。
- 锁顺序固定为「全局并发许可 → 组锁」，避免死锁。
- 任务进入终态 / 被取消后归还容量名额，队列排空后系统恢复可提交。

## 重启恢复语义（投递保证：at-least-once）

任务状态全部持久化在 `async_job` 表。`ApplicationReadyEvent` 时执行 `StartupRecovery`：

- `PENDING`：仍在库中，按分组唤醒协程继续按序执行，**不丢失**。
- `RETRY_WAITING`：保留退避计划，到点后由重试扫描拉起。
- `RUNNING`：说明上次进程在执行中退出（崩溃 / `kill -9`）。重启时**先 CAS 退回 `PENDING` 再重投一次**，
  因此该任务可能执行超过一次。

> 因此处理器 `JobHandler.handle(...)` 的副作用**必须幂等**（例如以任务 id / 业务幂等键去重），
> 这是 at-least-once 投递下避免重复副作用的标准约定。排队中的任务恰好只执行一次；
> 仅“崩溃发生在执行窗口内”的任务可能重复执行。

关闭阶段（优雅停机）中被中断的在执行任务不会被误判为失败 / 死信，而是保持 RUNNING，
交由下次启动恢复，避免“刚崩溃就被错误标死”。

## 配置项（`application.properties` 前缀 `async-job`）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `max-concurrency` | 4 | 全局最大并发执行数 |
| `queue-capacity` | 16 | 在途任务总容量（PENDING/RUNNING/RETRY_WAITING） |
| `default-max-attempts` | 4 | 缺省最大执行次数 |
| `backoff-initial-millis` | 200 | 首次重试等待 |
| `backoff-multiplier` | 2.0 | 退避倍率 |
| `backoff-max-millis` | 5000 | 退避封顶 |
| `retry-tick-millis` | 100 | 重试扫描周期 |

## 本地验证

### 一键跑测试

```bash
mvn test
```

覆盖场景（共 16 个用例，全部断言最终状态与真实执行次数）：

| 测试类 | 覆盖点 |
| --- | --- |
| `IdempotentSubmitTest` | 串行 / 16 线程并发同一幂等键只产生一条任务、处理器只执行一次 |
| `RetryAndDeadLetterTest` | 重试到上限进死信（执行次数=maxAttempts、失败原因可区分）、瞬时失败重试后成功、未知类型即死信 |
| `RestartRecoveryTest` | 排队任务跨“重启”不丢且按序执行；中断的 RUNNING 重投（at-least-once） |
| `BackpressureAndConcurrencyTest` | 容量打满明确抛 `QueueFullException`、排空后恢复、并发峰值不超上限、并发提交接受数不超容量 |
| `GroupOrderingTest` | 同组严格串行且顺序等于提交顺序、不同分组并行（出现执行重叠） |
| `CancelAndConsistencyTest` | PENDING 可取消且永不执行、RUNNING 不可回退取消、12 线程并发取消/查询下状态迁移恰好一次且自洽 |

测试日志会打印任务标识（jobId）、每次状态迁移（`[STATE] ... from -> to`）、
每次重试原因（`[RETRY] ... 第 n/m 次失败，x ms 后重试 reason=...`）和死信原因（`[DEAD] ...`）。

### 手工 curl 验证

```bash
mvn spring-boot:run

# 1) 提交并查询
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","groupKey":"g1","jobType":"echo","payload":"hello"}'
curl localhost:8080/api/jobs/{id}

# 2) 同键重复提交 -> 200 且 duplicate=true，同一 id
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","groupKey":"g1","jobType":"echo","payload":"hello"}'

# 3) 重试 -> 死信
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"d1","groupKey":"gd","jobType":"echo","payload":"always-fail:boom","maxAttempts":3}'
curl localhost:8080/api/jobs/{id}          # 轮询直到 DEAD，查看 attempts / failureReason
curl localhost:8080/api/jobs/dead

# 4) 背压（发超过容量的 sleep 慢任务，观察 503 QUEUE_FULL）
for i in $(seq 1 22); do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/api/jobs \
    -H 'Content-Type: application/json' \
    -d "{\"idempotencyKey\":\"bp-$i\",\"groupKey\":\"gb-$i\",\"jobType\":\"echo\",\"payload\":\"sleep:5000\"}"
done | sort | uniq -c

# 5) 重启恢复：提交几个 sleep 任务后 kill -9 进程，再重新启动，
#    观察日志 [RECOVERY] ... RUNNING(中断) -> PENDING，任务随后 SUCCEEDED
```

## 代码结构

```
job/
  JobStatus / JobRecord / JobRequest      状态机、持久化实体、提交请求
  JobHandler / JobHandlerRegistry         业务处理器 SPI 与按 type 注册
  JobRepository / JdbcJobRepository       持久化：幂等插入、CAS 更新、FOR UPDATE 领取、恢复查询
dispatch/
  JobService / DefaultJobService          提交(幂等+背压)、查询、取消
  JobDispatcher / DefaultJobDispatcher    分组串行协程、全局并发许可、CAS 状态机、重试/死信收口
  RetryScheduler                          到点重试扫描
  StartupRecovery                         重启恢复
  CapacityGauge                           在途容量计数（背压闸门）
  QueueFullException / JobQueueProperties 背压异常与配置
web/
  JobController / JobViews                REST 接口与视图
handler/
  EchoJobHandler                          内置示例处理器
```

## 设计要点与边界

- **为什么容量计数同时计入 RUNNING 与 PENDING/RETRY_WAITING**：背压要约束的是“系统已承诺但未完成”的总量，
  否则队列可以无限堆积。终态 / 取消时归还。
- **为什么崩溃恢复是 at-least-once 而非 exactly-once**：无法判断被强杀时业务副作用是否已落库，
  重投 + 处理器幂等是分布式任务的标准取舍。
- **生产化建议**（当前实现聚焦本地可验证）：把 H2 换成 Postgres/MySQL 并将
  `FOR UPDATE` 领取替换为 `SKIP LOCKED` 以支持多实例；为 `async_job` 增加归档/清理任务。
