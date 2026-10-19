# Redis Sink 分段延迟观测指标 设计文档

| 项 | 值 |
|---|---|
| 仓库 / 分支 | `/Users/administrator/GolandProjects/flink-connector-redis`，分支 `feat/sink-latency-metrics`，基于 tag `v2.2.6`（`7c0418a`） |
| 运行时 | Flink 2.2.x（pom `flink.version=2.2.1`，provided），Java 11，Lettuce 6.2.1 纯异步 |
| 状态 | 设计稿 v1.4（第 5 轮评审 APPROVED）；**已进入实现**（分支 `feat/sink-latency-metrics`） |
| 目标作业 | `i-20251209-halykbank-limit-s`（A/B 双活，同写一个 sentinel Redis） |

## 1. 概述

生产作业出现“交易 30–90 秒后才在 Redis 可见或累计”的现象，但现有 Flink 指标（Kafka lag=0、算子 watermark 延迟 <25s、无背压、checkpoint 正常）都看不出问题。connector 是 Lettuce 纯异步写入，外加一个只在下一条 `write()`、batch 满或 checkpoint `flush()` 时才清空的内部缓冲。这两部分的耗时不会表现为背压，所以现有指标看不到。

本设计只在 `RedisSinkWriter`（SQL 走的 Sink V2 writer）里增加观测，不改任何写入逻辑，不新增任何 WITH 选项。生产只替换 jar，不改 SQL、不改参数。问题复现时，可以把一条记录的端到端延迟拆成三段：上游+计算（`invoke_delay`）、sink 内部（`buffer_wait`）、Redis 往返（`cmd_latency`），再配合两个积压 gauge（`buffer_size`、`pending_commands`）判断积压在哪一层。

需要先说明一个关键结论（§3）：读 Flink 2.2 源码后确认，**生产拓扑下 `SinkWriter.Context#timestamp()` 恒为 `null`**。原因是 LAG OVER 后面紧跟的 Calc（`WHERE previous_value IS NULL OR ...` 和 `CAST`）是代码生成算子，它输出的 StreamRecord 不带时间戳。如果严格按“timestamp 为 null 就跳过”实现，生产上 `invoke_delay` 和 `ack_freshness` 两个 P0 指标一个样本都不会有。本设计因此采用**方案 C（用户已确认，原话见 §18-1）**：`Context#timestamp()` 非 null 且 > 0 时用它；否则退回 `Context#currentWatermark() + 1`；watermark 为 `Long.MIN_VALUE`、非正或 `Long.MAX_VALUE` 时跳过。回退基准可以证明 ≤ 窗口时间，所以回退时 `invoke_delay` 和 `ack_freshness` 是**上界**：偏大量小于一次 watermark 跳变，低流量时这个跳变可能达到十几秒。解读时要结合精确的 `buffer_wait`/`cmd_latency` 一起看。另加一个低基数 gauge `redis_sink_event_time_source`（用户已确认），标明当前用的是哪种口径。

## 2. 范围

**本次实现的指标（不扩展）：**

| 代码内注册名 | 类型 | 定义 |
|---|---|---|
| `redis_sink_invoke_delay_ms` | 延迟三件套 | 进入 `write()` 的时刻 − 窗口时间基准 |
| `redis_sink_buffer_wait_ms` | 延迟三件套 | 首次把命令提交给 Lettuce 的时刻 − 进入 `write()` 的时刻（组成见下） |
| `redis_sink_cmd_latency_ms` | 延迟三件套 | 主写命令（如 HSET）ack 时刻 − 该次提交时刻 |
| `redis_sink_ack_freshness_ms` | 延迟三件套 | 主写命令 ack 时刻 − 窗口时间基准 |
| `redis_sink_buffer_size` | Gauge<Integer> | 缓冲区中还没提交给 Lettuce 的记录数 |
| `redis_sink_pending_commands` | Gauge<Integer> | 已提交、尚未完成的主写命令数 |

“延迟三件套”指每个延迟指标同时注册三种形态：Histogram `<x>_ms`、Gauge `<x>_max_ms`、Gauge `<x>_last_ms`（§4.2）。

**`buffer_wait` 的组成**（源码 `flushBuffer` 第 284-300 行，每条记录依次执行 `cooperativeWait(rateLimiter.reserve(1))` → `acquireInFlightPermit()` → `checkAsyncError()` → `sendRecord(record)`；`submitNanos` 在 `sendRecord` 内取）：

1. **等 flush 触发**：下一条 `write()` 检查到 batch 满或距上次 flush ≥ `sink.batch.flush-interval`（只在 `write()` 内检查），或 checkpoint 前的 `flush()`；
2. **`sink.write.qps` 限速等待**：生产未配置（默认 0，限速器为 `null`），不生效；
3. **等 in-flight 许可**：`sink.max-in-flight-requests`（默认 1000，每个 writer 一个 `Semaphore`）耗尽时，要等前面的命令 ack 释放许可，最长 60s；
4. 同一批次中排在前面的记录的提交耗时（微秒级）。

因此 `buffer_wait` 高**不一定**是“卡在缓冲”，必须结合 `pending_commands` 判断（§15.2 解读规则）。本次不拆分这几部分，拆分会扩大指标范围。

**辅助项（用户已确认，原话见 §18-1）：**

- `redis_sink_event_time_source`（Gauge<Integer>）：最近一条记录使用的窗口时间基准口径。0 = 无基准（已跳过），1 = record timestamp（精确），2 = watermark 回退（上界）。第一条记录到达之前为 0。

**不在范围内：** feasibility 报告里的其他指标（`time_since_last_ack`、`records_written_total`、timeout/error/reconnect 计数、连接类指标等）、基准缺失计数器（与 `event_time_source` 和 `_count` 速率重复，见 §18-2）、任何新的 WITH 选项或开关（§10、§18-3）、`CLIENT SETNAME`、trace 日志、Lua CAS、定时 flush、任何超时/重试/连接参数的修改、旧版 `RedisSinkFunction`（§5.4）、Kafka source 侧指标。

## 3. 窗口时间基准：`Context#timestamp()` 论证与决策

### 3.1 生产拓扑

`jobs-halykbank-prod/i-20251209-halykbank-limit-s/job.sql`，以 iS0005 为例，13 个 INSERT 都是同一形态：

```
evofinder_orders (WATERMARK FOR creationTime AS creationTime - 3s, creationTime TIMESTAMP(3))
  → CUMULATE(5s, 1h) GROUP BY ... , window_time                 -- *_source（第 292-316 行）
  → LAG(metric_value) OVER (PARTITION BY k, hk ORDER BY window_time)  -- *_compare（第 318-321 行）
  → SELECT k, hk, CAST(metric_value AS string) hv
    WHERE previous_value IS NULL OR metric_value <> previous_value -- 第 323-326 行 → Calc
  → INSERT INTO dest_redis_table_40 (k, hk, hv)                    -- 第 594 行，sink 没有时间列
```

### 3.2 Flink 2.2 源码证据（`release-2.2` 分支，本地副本在 `.agents/flink22src/`）

1. **Sink 端怎么取值。** `SinkWriterOperator.java` 第 181-185 行：`processElement` 里 `context.element = element; sinkWriter.write(...)`。第 297-303 行：`Context#timestamp()` 只有在 `element.hasTimestamp()` 且不等于 `NO_TIMESTAMP` 时才返回值，否则返回 `null`。第 198-200 行：`processWatermark` 先把 `currentWatermark` 设成新值，`Context#currentWatermark()` 返回的就是它，初始值是 `Long.MIN_VALUE`（第 102 行）。
2. **OVER 算子会设置时间戳。** `KeyedProcessOperator.java` 第 67-68 行：`onEventTime` 调用 `collector.setAbsoluteTimestamp(timer.getTimestamp())`。Flink 2.2 的 `table.exec.unbounded-over.version` 默认是 **2**（`ExecutionConfigOptions` 第 763-766 行；读自上游 release-2.2，本地副本中没有，行为由 §14.1-5 (b) 验证），对应 `RowTimeUnboundedPrecedingOverFunctionV2`：第 173 行按行的 rowtime 注册定时器 `registerEventTimeTimer(timestamp)`，所以 OVER 输出记录的时间戳 = `window_time`（= window_end − 1ms）。V1（`AbstractRowTimeUnboundedPrecedingOver` 第 155-157 行）则按 `curWatermark + 1` 注册，输出时间戳 ≤ `window_time`。
3. **紧跟的 Calc 会抹掉时间戳。** `OperatorCodeGenerator.scala` 第 39-40、52 行：代码生成的单输入算子持有一个复用成员 `outElement = new StreamRecord(null)`，它的 `hasTimestamp=false`。第 275-276 行 `generateCollect` 生成的是 `output.collect(outElement.replace(result))`。`StreamRecord.java` 第 100-103 行：`replace(X)` 只替换 value，不动时间戳。`CalcCodeGenerator.scala` 第 133-136 行，Calc 正是走这条路径。代码生成器里没有任何地方调用 `outElement.setTimestamp`（已 grep `OperatorCodeGenerator`/`CalcCodeGenerator`/`CodeGeneratorContext`）。**所以 OVER 之后只要有一个 Calc，下游拿到的就是没有时间戳的记录。**
4. **Sink 也不会补时间戳。** `StreamExecSink.java` 第 219-239 行（读自上游 release-2.2，本地副本中没有，行为由 §14.1-5 (b) 验证）：只有 sink 的输入里有 rowtime 属性列时，`rowtimeFieldIndex` 才不是 −1。`CommonExecSink.java` 第 456-457、511-529 行：SinkV2 路径只在 `rowtimeFieldIndex != -1` 时插入 `StreamRecordTimestampInserter`。生产 sink 的输入只有 `(k, hk, hv)`，所以不会插入。

**结论：生产上 `Context#timestamp()` 对每条记录都返回 `null`。** “timestamp ≈ window_time”这个预期，只在 OVER 直接连 sink、或 sink 输入保留了 rowtime 列时才成立。（评审已逐条复核成立。）

### 3.3 替代基准：`Context#currentWatermark()`

`AbstractStreamOperator.java` 第 682-686 行：`emitWatermarkDirectly` 先 `timeServiceManager.advanceWatermark(mark)`（触发定时器、输出记录），**之后**才 `output.emitWatermark(mark)`。由此可以推出以下关系（T 表示某行的 `window_time`）：

- 这行在 OVER 的 `processElement` 中没有被当成迟到数据丢弃，说明到达时 OVER 的水位线 < T。
- 它在 OVER 收到某个 W ≥ T 的水位线时，从定时器里被输出；W 本身要等这一轮定时器跑完后才往下游发。
- 因此这行到达 sink 时，sink 的 `currentWatermark` = 上一个已下发的水位线 `W_prev`，并且 **`W_prev < T`**。OVER→Calc→Sink 是 forward 链接时，sink 的水位线就是 OVER 下发的值。即使不链接、有多路输入，sink 的水位线取各通道最小值，携带这行的通道在这行之后才会送来 W，所以结论不变。

所以 **`wm + 1 ≤ T`**。用它作基准时，`invoke_delay` 和 `ack_freshness` 是**上界**，只会高估，高估量 = `T − (W_prev + 1)` < **一次 watermark 跳变**（W − W_prev）。

- 流量稳定时，跳变约等于 `pipeline.auto-watermark-interval`（默认 200ms）内事件时间的推进量，也就是亚秒级。
- **低流量时跳变可能很大，可达十几秒**：最慢分区稀疏时，水位线只在新事件到来时才前进；`table.exec.source.idle-timeout=5s` 切换时，水位线也可能一次跳过多个窗口。这时一次性输出的多个窗口，基准都是同一个 `W_prev+1`，越早的窗口高估越少，越晚的越多。

解读规则（README 必须写明）：`event_time_source==2` 时，`invoke_delay`/`ack_freshness` 只能当作“最多这么慢”。判断慢在哪里，要以 `buffer_wait` 和 `cmd_latency` 为准，这两段是精确值。如果它们都小，而 `ack_freshness` 大，就说明慢在“上游+计算”，或者只是回退基准的高估。这种高估，正好对应“水位线被拖住”的那段时间，本来也属于“上游+计算”段。**这个误差只会进入 `invoke_delay`/`ack_freshness`，`buffer_wait` 和 `cmd_latency` 用 `nanoTime` 自测，完全不受影响**，三段拆分的定位结论不受影响。

### 3.4 决策

可选方案：

- A. 严格按需求，只用 `Context#timestamp()`，null 就跳过。生产上两个 P0 指标永远为空，达不到用户目标。**否决。**
- B. 改 SQL 或加列（feasibility 方案 A）。用户明确不改 SQL、不改参数。**否决。**
- C. **`timestamp()`（非 null 且 > 0）优先，否则 `currentWatermark() + 1`（要求 0 < wm < Long.MAX_VALUE），两个都没有则跳过。** 零 SQL 改动、零行为改动，生产可用，误差有上界。**选用（用户已确认）。**

判定规则（伪代码，`resolveEventTime` 的唯一实现）：

```java
Long ts = ctx == null ? null : ctx.timestamp();
if (ts != null && ts > 0)                         { source = 1; return ts; }        // RECORD_TIMESTAMP
long wm = ctx == null ? Long.MIN_VALUE : ctx.currentWatermark();
if (wm > 0 && wm < Long.MAX_VALUE)                { source = 2; return wm + 1; }    // WATERMARK（上界）
source = 0; return NO_EVENT_TIME;                                                   // NONE，跳过
```

`source` 写入 `volatile int`，由 gauge `redis_sink_event_time_source` 导出（取最近一条记录的值）。“基准缺失”可从 `event_time_source==0`，以及 `invoke_delay_ms_count` 不涨而 `buffer_wait_ms_count` 在涨识别，不另设计数器。

基准来源按记录判定。同一个 sink 算子的上游拓扑是固定的，所以实际上一个 writer 只会用一种来源；唯一的例外是第一个水位线到达之前（NONE → WATERMARK）。writer 在 task 线程上**每种来源第一次出现时打一行 INFO**（每个 writer 最多 3 行），内容是 subtask index 和来源名，例如 `Redis sink latency metrics: event-time reference source=WATERMARK (subtask 2)`，不打印任何配置。subtask index 在 `tryRegister` 中从 `g.getAllVariables().get("<subtask_index>")` 读取（`ScopeFormat.SCOPE_TASK_SUBTASK_INDEX` 的变量名），取不到时用 `"?"`，不改 `RedisSink`/构造函数签名。

`Long.MAX_VALUE`（输入结束时的 MAX_WATERMARK）必须排除，否则 `now − (wm+1)` 会溢出。

### 3.5 时区与时钟前提

`creationTime` 是 `TIMESTAMP(3)`（不带时区）。rowtime 和水位线的毫秒值，是把它按 UTC 解释得到的 epoch。指标用 TM 的 `System.currentTimeMillis()` 去减，因此**前提是上游写进 `creationTime` 的是 UTC 墙钟**。

现有看板 `watermarkLag` 显示 <25s，而不是 ±5h 量级，说明这个前提成立，但仍需在测试环境确认（§14.3）。如果前提不成立：生产方时钟比 UTC 快，所有样本会被钳成 0（表现为 `_count` 在涨，`_max_ms`/`_last_ms` 恒为 0）；比 UTC 慢，则会多出一个固定偏移。生产方与 TM 之间的 NTP 偏差同样会叠加进这两个指标。

### 3.6 验证方法

1. **单测（MiniCluster，不需要 Redis）**：`EventTimeReferenceTest`（§14.1-5），用 Flink 2.2.1 跑两个拓扑，rowtime 为物理列 `TIMESTAMP(3)`（与生产一致），会话时区固定 UTC。(a) 和生产同形的 CUMULATE→LAG→Calc→probe sink：断言 `timestamp()==null`；对每个 `wm != Long.MIN_VALUE` 的行，同时断言上界 `wm+1 ≤ epochMillis(wt)` 和下界 `epochMillis(wt) − (wm+1) < 10_000`，且这样的行至少有一行，防止断言空过。(b) SQL 与 (a) 完全相同（同样是 CUMULATE → LAG OVER → `WHERE previous_value IS NULL OR v <> previous_value` 的 Calc），唯一区别是 SELECT 里写 `window_time AS wt`（不 CAST，保留 rowtime 属性），probe sink 的列为 `wt TIMESTAMP(3)`：断言每行 `timestamp()==epochMillis(wt)`，且行数 > 0。它证明的是“即使 OVER 后接 Calc，只要 sink 输入保留 rowtime 列，`StreamRecordTimestampInserter` 就会补上时间戳”，即 source=1 分支在真实拓扑下可达。（类名用 `*Test` 后缀，确保被 surefire 默认 include 规则执行，见 §14.2。）
2. **测试环境**：只换 jar。TM 日志里应出现 `event-time reference source=WATERMARK` 的 INFO 行；Flink UI 的作业图里应能看到 `OverAggregate → Calc → Sink`。
3. **数值对照**：在流量稳定时段，`redis_sink_invoke_delay_ms{quantile="0.5"}` 应约等于 3s 水位线延迟 + 采集延迟 + 亚秒级，并且和现有 `watermarkLag` 面板同量级。

## 4. 指标定义、导出名与形态

### 4.1 注册位置与导出名

所有指标都**直接注册在** `RedisSink#createWriter` 传进来的 `SinkWriterMetricGroup` 上，不调用 `addGroup`。`InternalSinkWriterMetricGroup` 是 operator group 的 `ProxyMetricGroup`（`InternalSinkWriterMetricGroup.java` 第 33、45 行），所以 logical scope 是 `taskmanager_job_task_operator`。Prometheus reporter 的导出名是 `"flink_" + logicalScope + "_" + metricName`（`AbstractPrometheusReporter.java` 第 63、142-146 行）。**代码里注册的名字不带 `flink_`**，reporter 会自动加上。

| 代码内注册名 | Prometheus 导出名 | 导出类型 |
|---|---|---|
| `redis_sink_<x>_ms` | `flink_taskmanager_job_task_operator_redis_sink_<x>_ms{quantile="0.5\|0.75\|0.95\|0.98\|0.99\|0.999"}` 和 `..._redis_sink_<x>_ms_count` | summary（`QUANTILES` 见第 316 行，`_count` 见第 368 行） |
| `redis_sink_<x>_max_ms` | `flink_taskmanager_job_task_operator_redis_sink_<x>_max_ms` | gauge |
| `redis_sink_<x>_last_ms` | `flink_taskmanager_job_task_operator_redis_sink_<x>_last_ms` | gauge |
| `redis_sink_buffer_size` | `flink_taskmanager_job_task_operator_redis_sink_buffer_size` | gauge |
| `redis_sink_pending_commands` | `flink_taskmanager_job_task_operator_redis_sink_pending_commands` | gauge |
| `redis_sink_event_time_source` | `flink_taskmanager_job_task_operator_redis_sink_event_time_source` | gauge（取值只有 0/1/2，没有额外 label） |

`<x>` ∈ {`invoke_delay`, `buffer_wait`, `cmd_latency`, `ack_freshness`}。每个 writer 共注册 15 个 metric 对象（12 个延迟形态 + 3 个 gauge）。

**label 形态**：和现有 `numRecordsSend` 一致，来自 operator group 的全部 scope 变量，即 `host, tm_id, job_id, job_name, task_id, task_name, task_attempt_id, task_attempt_num, subtask_index, operator_id, operator_name`；summary 额外带 `quantile`。Prometheus 抓取侧可能还会附加 `instance`/`pod`/`dc` 等 label，这些不来自 connector。**不新增任何 label**，不把 key、hk、storeID、表名放进 label 或 group，所以没有高基数风险。同一张表的 8 个 INSERT 对应 8 个 sink 算子，靠 `operator_name`/`operator_id` 区分。具体 label 集合以测试环境 `curl :9249/metrics` 的输出为准。

### 4.2 每个延迟指标为什么要三种形态

- **Histogram**：`org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(1024)`。分位数基于最近 1024 个样本。
- **`_max_ms`**：最近 60 秒滑动窗口内的最大值（§8），读取不会清零，窗口内没有样本时返回 0。
- **`_last_ms`**：最近一个样本的值，用 `volatile long` 存，第一个样本出现前为 0。

原因有两个：Prometheus reporter 只导出 quantile 和 `_count`，**不导出 histogram 的 max**；而“最近 N 个样本”的分位数在低流量时会长期停在旧样本上，问题过去后还一直显示高值，问题正在发生时又可能被大量旧的正常样本稀释。`_max_ms` 按时间窗口衰减，可以直接跨 subtask 用 `max by` 聚合，`_last_ms` 用来看即时值。`_count` 配合 `rate()` 用来判断样本是否还在产生。

**单位与取整**：全部是毫秒 `long`。纳秒差值用 `TimeUnit.NANOSECONDS.toMillis` 截断，所以亚毫秒的 Redis 往返会显示为 0。目标量级是 30–90s，毫秒精度足够。负值统一钳成 0。

## 5. 采样点（v2.2.6 行号已核对）

文件 `src/main/java/org/apache/flink/streaming/connectors/redis/table/RedisSinkWriter.java`（737 行）。

| # | 位置（当前行号） | 新增动作 | 线程 |
|---|---|---|---|
| S0 | 构造函数第 124 行起，`if (metricGroup != null)` 块（第 168-172 行）内部末尾 | `latencyMetrics = RedisSinkLatencyMetrics.tryRegister(metricGroup)`，失败返回 `null`；`metricGroup == null` 时保持 `null` | task |
| S1 | `write()` 第 212 行，紧接 `UPDATE_BEFORE` 判断（第 218-220 行）之后 | `invokeNanos = nanoTime()`；`eventTimeMs = resolveEventTime(context)`；`onInvoke(eventTimeMs)` 记录 `invoke_delay` | task |
| S2 | `write()` 第 237 行 `batchBuffer.add(...)` | 改成 `new PendingRecord(params, kind, invokeNanos, eventTimeMs)`；之后 `setBuffered(batchBuffer.size())` | task |
| S3 | `flushBuffer()` 第 284 行循环前，以及第 300 行 `sendRecord(record)` 之前 | 循环前 `int remaining = batchBuffer.size()`；调用 `sendRecord` 前 `setBuffered(--remaining)` | task |
| S4 | `flushBuffer()` 第 322 行 `batchBuffer.clear()` 之后 | `setBuffered(0)` | task |
| S5 | `sendRecord()` 第 391 行重试循环内、第 393-397 行发命令之前 | `submitNanos = nanoTime()`；`i == 0` 时 `onFirstSubmit(record.invokeNanos, submitNanos)` 记录 `buffer_wait` | task |
| S6 | `sendRecord()` 第 400 行 `if (redisFuture != null)` 块内，**放在原有 `whenComplete`（第 408-425 行）之后** | `onSubmitted(redisFuture, submitNanos, record.eventTimeMs)`：`pending++`，再挂一个独立的 `whenComplete`（回调里 `pending--`，成功时记录 `cmd_latency` 和 `ack_freshness`） | task 注册；回调在 netty 线程（future 已完成时在 task 线程同步执行） |

所有新增调用都包在 `if (latencyMetrics != null)` 里。原有语句的内容和顺序都不变。

注意 S3：`setBuffered(--remaining)` 在 `acquireInFlightPermit()` 之后执行，所以等许可期间 `buffer_size` 仍 > 0，这正是“记录还没提交”的事实；它和 `pending_commands` 一起才能区分等 flush 与等许可（§15.2）。

### 5.1 “主写命令”的界定

主写命令就是 `sink()`（第 476 行）或 `rowKindDelete()` 返回的那个 future。生产用的 `hset` 且没开 `set.if.absent`，返回的就是 HSET 本身（第 552-556 行），下游 `HGET` 在它 ack 时已经可读。TTL 的 `EXPIRE`/`TTL`（`setTtl` 第 670 行，在原回调第 412 行 fire-and-forget）**不计入**。

几种例外，写进文档，不改代码：

- `set.if.absent=true` 时，SET/HSET/HMSET 返回的是 EXISTS/HEXISTS，真正的写入在它的回调里发出，原代码不跟踪这次写入，所以测到的是存在性检查的往返。
- ZADD 配 `zset.zremrangeby` 时，只测 ZADD。
- `rowKindDelete` 返回 `null` 时（命令没有对应的删除操作），不提交命令，也没有 ack 类样本，但 `buffer_wait` 仍然记录。

### 5.2 RowKind

`UPDATE_BEFORE` 在 S1 之前就 return 了，不采样。`INSERT`/`UPDATE_AFTER`/`DELETE` 都采样。

### 5.3 `RedisLimitedSinkWriter`

它继承 `RedisSinkWriter`，`write()` 调用 `super.write()` 和 `flushBuffer()`，所以自动覆盖，不需要改。

### 5.4 旧版 `RedisSinkFunction`

**不覆盖。** 它是 SinkFunction（v1）实现，只给 DataStream 测试用；SQL 路径是 `RedisDynamicTableSink → SinkV2Provider → RedisSink → RedisSinkWriter`，不经过它。

## 6. 数据结构与新增类

### 6.1 `PendingRecord`（第 113-121 行，私有静态类）

```java
private static class PendingRecord {
    final String[] params;
    final RowKind kind;
    final long invokeNanos;   // write() 入口的 System.nanoTime()；未开指标时为 0
    final long eventTimeMs;   // 窗口时间基准（epoch ms）；没有时为 RedisSinkLatencyMetrics.NO_EVENT_TIME = Long.MIN_VALUE
    PendingRecord(String[] params, RowKind kind, long invokeNanos, long eventTimeMs) { ... }
}
```

新字段都是 `final`，在构造函数里赋值，回调线程读取时有 final 字段的安全发布保证。每条缓冲记录多 16 字节，缓冲上限是 `sink.batch.size`（默认 100）。指标未注册时 S2 传 `0, NO_EVENT_TIME`。

### 6.2 `RedisSinkWriter` 新增字段

```java
private transient RedisSinkLatencyMetrics latencyMetrics;   // null = metricGroup 为 null 或注册失败
```

### 6.3 新包 `org.apache.flink.streaming.connectors.redis.metrics`

- **`SlidingWindowMax`**：60 个 1 秒桶的滑动最大值（§8）。时钟用注入的 `LongSupplier`（纳秒）。
- **`LatencyMetric`**：组合 `DescriptiveStatisticsHistogram(1024)`、`SlidingWindowMax` 和 `volatile long last`。
  - `void update(long ms)`：`v = max(0, ms)`，依次更新 histogram、max、last。
  - `void register(MetricGroup g, String base)`：注册 `base + "_ms"`（histogram）、`base + "_max_ms"`（`Gauge<Long>`）、`base + "_last_ms"`（`Gauge<Long>`）。
- **`RedisSinkLatencyMetrics`**（门面，供 writer 调用）：
  - `static final long NO_EVENT_TIME = Long.MIN_VALUE`
  - `static RedisSinkLatencyMetrics tryRegister(MetricGroup g)`：使用 `System::nanoTime` 和 `System::currentTimeMillis`；读取 `subtaskLabel = g.getAllVariables().getOrDefault("<subtask_index>", "?")`（只用于 INFO/WARN 日志）。包级可见的重载可以注入两个时钟，供测试使用。
  - `long nanoTime()`：实现就是 `return nanoClock.getAsLong();`，不做 disabled 检查，也不 try/catch。约定：**生产时钟 `System::nanoTime` 不会抛异常**；S1、S5 在 writer 路径上调用它（S5 位于 `sendRecord` 的重试 try 块内），所以**经过 writer 路径的测试一律注入不抛异常的时钟**（§14.1-4）。会抛异常的假时钟只在 `RedisSinkLatencyMetricsTest` 中直接调用门面方法时使用（§14.1-3），用来验证门面内部的异常隔离，不会触发 HSET 重发。
  - `long resolveEventTime(SinkWriter.Context ctx)`：规则见 §3.4 伪代码；每条记录都把来源写进 `volatile int eventTimeSource`（常量 `SOURCE_NONE=0`、`SOURCE_RECORD_TIMESTAMP=1`、`SOURCE_WATERMARK=2`）；某种来源首次出现时打 INFO。
  - `void onInvoke(long eventTimeMs)`：`eventTimeMs != NO_EVENT_TIME` 时，`invokeDelay.update(wallNow − eventTimeMs)`。
  - `void onFirstSubmit(long invokeNanos, long submitNanos)`：`bufferWait.update(toMillis(submitNanos − invokeNanos))`。
  - `void setBuffered(int n)`：写 `volatile int buffered`，只有 task 线程写。
  - `void onSubmitted(RedisFuture<?> f, long submitNanos, long eventTimeMs)`：`pending.incrementAndGet()`，然后在 try/catch 里执行下面的 `f.whenComplete(...)`；**任何异常都不外泄到 `sendRecord`**（S6 位于 `sendRecord` 的重试 try 块内，异常外泄会被 `catch (Exception e1)` 接住并重发同一条 HSET）。完整代码见下。
  - 注册的 Gauge：`redis_sink_buffer_size → buffered`，`redis_sink_pending_commands → Math.max(0, pending.get())`，`redis_sink_event_time_source → eventTimeSource`。

**异常隔离状态与回调**（三个字段的类型和线程归属都是确定的）：

```java
private volatile boolean disabled;                                    // 任一线程首次异常时置 true；所有线程可见
private final AtomicReference<Throwable> disableCause = new AtomicReference<>();  // 首个异常，CAS 只设一次
private boolean disableWarned;                                        // 只有 task 线程读写，不需要 volatile

// S6：task 线程调用；整个方法保证不向 sendRecord 抛出任何 RuntimeException
void onSubmitted(RedisFuture<?> f, long submitNanos, long eventTimeMs) {
    skipBecauseDisabled();                             // 只用于补打 WARN，不据此跳过（pending 在 disabled 后仍需准确）
    pending.incrementAndGet();
    try {
        // 回调：netty 线程，或 future 已完成时在 task 线程同步执行
        f.whenComplete((r, t) -> {
            pending.decrementAndGet();                 // 无条件执行，保证与上面的 +1 一一配对
            if (t != null || disabled) {
                return;
            }
            try {
                cmdLatency.update(TimeUnit.NANOSECONDS.toMillis(nanoClock.getAsLong() - submitNanos));
                if (eventTimeMs != NO_EVENT_TIME) {
                    ackFreshness.update(wallClock.getAsLong() - eventTimeMs);
                }
            } catch (RuntimeException e) {
                disableCause.compareAndSet(null, e);
                disabled = true;                       // 回调线程不打日志
            }
        });   // 返回的新 stage 直接丢弃
    } catch (RuntimeException e) {
        // whenComplete 本身抛错（现实中不会发生）：回调没注册上，撤销 +1，保持配对
        pending.decrementAndGet();
        disableCause.compareAndSet(null, e);
        disabled = true;
        skipBecauseDisabled();                         // 在 task 线程立即打出唯一一行 WARN
    }
}

// task 线程上的采样入口（resolveEventTime / onInvoke / onFirstSubmit）共用的前置检查
private boolean skipBecauseDisabled() {
    if (!disabled) {
        return false;
    }
    if (!disableWarned) {
        disableWarned = true;
        LOG.warn("Redis sink latency metrics disabled (subtask {}): {}", subtaskLabel, String.valueOf(disableCause.get()));
    }
    return true;
}
```

task 线程入口的规则：

- `resolveEventTime`、`onInvoke`、`onFirstSubmit`：开头调用 `skipBecauseDisabled()`，为 true 时直接返回（`resolveEventTime` 返回 `NO_EVENT_TIME`，不更新 `eventTimeSource`）；方法体包在 `try/catch (RuntimeException e)` 里，catch 中 `disableCause.compareAndSet(null, e); disabled = true;`，然后立即 `skipBecauseDisabled()` 打出 WARN。
- `onSubmitted`：**disabled 时仍然**执行 `pending.incrementAndGet()` 并注册回调（回调本身在 disabled 时只做 `pending--`），所以 `pending_commands` 在 disabled 后仍然准确。它的入口也调用 `skipBecauseDisabled()`，但只用于补打 WARN，不据此跳过。`whenComplete` 的注册包在 try/catch 里，注册失败时撤销 `+1` 并置 disabled，异常不外泄（见上面的代码）。`skipBecauseDisabled()` 本身只有 volatile 读、一次 boolean 写和一次 `LOG.warn`，SLF4J 的 `warn` 不向调用方抛异常。
- `setBuffered`：只是一次 volatile int 写，不可能抛异常，不做 disabled 检查，`buffer_size` 在 disabled 后仍然准确。

disabled 之后各 gauge 的值：`buffer_size`、`pending_commands` 继续准确；`event_time_source` 和各 `_last_ms` 保持最后的值；各 `_max_ms` 在 60 秒后自然衰减为 0（它按时间窗口读取）；histogram 停在最后 1024 个样本，`_count` 不再增长。回调线程触发的 disabled，会在 task 线程下一次进入任一入口时（通常就是下一条 `write`）补打这行 WARN；整个 writer 生命周期最多 1 行。

### 6.4 依赖

`DescriptiveStatisticsHistogram` 在 `flink-runtime` 里。`flink-runtime` 是 `flink-streaming-java`（provided）的传递依赖，运行时由 flink-dist 提供，不打进 jar。实现时用 `mvn dependency:tree` 确认它在编译 classpath 上；如果不在，显式加上 `flink-runtime` `${flink.version}` provided。**不引入任何新的第三方依赖。**

## 7. 线程模型与线程安全

| 线程 | 做的事 | 涉及的共享状态 |
|---|---|---|
| Task/mailbox 线程 | `write`、`flushBuffer`、`sendRecord`（含同步重试）、S0-S6 的注册与采样；future 已经完成时，`whenComplete` 也在这里同步执行 | 全部 |
| Lettuce netty event loop（每个 writer 一条连接，对应一个 event loop 线程） | S6 回调：`pending--`，更新 `cmd_latency`/`ack_freshness`，异常时设置 `disabled`/`disableCause` | `pending`、两个 `LatencyMetric`、`disabled`、`disableCause` |
| Metric reporter 线程 | 读取 gauge、`getStatistics()` | 全部指标 |

- `DescriptiveStatisticsHistogram`：内部的 `CircularDoubleArray.addValue`/`toUnsortedArray` 都是 `synchronized`（Flink 2.2 源码 `DescriptiveStatisticsHistogram.java` 第 67-90 行），可以多线程 update，读取时拷贝快照。
- `SlidingWindowMax`：`update` 和 `get` 都用 `synchronized`。写入方基本只有一个线程（task 或 netty），读取方是 reporter 线程，几乎没有竞争，无竞争的 monitor 进出约 20ns。考虑过把（秒, 值）打包进 `AtomicLongArray` 的无锁方案，正确性论证更复杂，收益可以忽略，不采用。
- `last`：`volatile long`，写入方可能是两个不同线程（同步完成时在 task 线程，异步完成时在 netty 线程），“最后写入者胜”，符合“最近一个样本”的语义。
- `buffered`、`eventTimeSource`：`volatile int`，只有 task 线程写，不需要 CAS。
- `pending`：`AtomicInteger`。increment 在 task 线程、`whenComplete` 注册之前；decrement 在回调里，且无条件执行（包括 disabled 和 `t != null`）。future 已完成时回调同步执行，先 +1 再 −1，不会出现负数。gauge 仍然钳到 `≥ 0` 兜底。
- `disabled`：`volatile boolean`，任一线程都可能写 true（只会从 false 变 true，幂等），所有线程都读；`disableCause`：`AtomicReference`，`compareAndSet(null, e)` 保留首个异常；`disableWarned`：普通 boolean，只在 task 线程读写。
- `PendingRecord` 的 final 字段、lambda 捕获的 `submitNanos`（局部 final）、`eventTimeMs`：都可以安全发布。
- **回调线程上**：只做 O(1) 运算加上述线程安全的更新，**不打日志、不分配大对象、不阻塞**。

## 8. 60 秒滑动窗口 max

```java
final class SlidingWindowMax {
    private static final int BUCKETS = 60;               // 每桶 1 秒
    private final long[] bucketSecond = new long[BUCKETS];
    private final long[] bucketMax = new long[BUCKETS];
    private final LongSupplier nanoClock;                // 生产用 System::nanoTime

    SlidingWindowMax(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        Arrays.fill(bucketSecond, Long.MIN_VALUE);       // 显式标记“从未写入”，不依赖 0 初值
    }

    synchronized void update(long v) {
        long s = Math.floorDiv(nanoClock.getAsLong(), 1_000_000_000L);
        int i = (int) Math.floorMod(s, BUCKETS);
        if (bucketSecond[i] != s) { bucketSecond[i] = s; bucketMax[i] = v; }  // 桶已过期或从未写入：覆盖
        else if (v > bucketMax[i]) { bucketMax[i] = v; }
    }

    synchronized long get() {
        long s = Math.floorDiv(nanoClock.getAsLong(), 1_000_000_000L);
        long m = 0;
        for (int i = 0; i < BUCKETS; i++) {
            long b = bucketSecond[i];
            if (b != Long.MIN_VALUE && b > s - BUCKETS && b <= s && bucketMax[i] > m) { m = bucketMax[i]; }
        }
        return m;                                        // 窗口内没有样本时返回 0
    }
}
```

- **时间基准**：用单调的 `nanoTime` 换算成秒，不受 NTP 调整墙钟的影响。`nanoTime` 原点任意、可能为负，`floorDiv`/`floorMod` 保证负值下标正确；`Long.MIN_VALUE` 哨兵在 `b > s - BUCKETS` 上也天然不成立，`b != Long.MIN_VALUE` 只是让意图显式。
- **窗口长度**：当前这一秒（可能只过了一部分）加上之前 59 个整秒，实际覆盖 59–60 秒。
- **读取不清零**：`get()` 只读不写。过期的桶在下一次 `update` 落到同一个下标时被覆盖，`get()` 也会忽略它们。
- **成本**：update O(1)；get O(60)，只在 reporter 线程上执行。

## 9. 各指标在重试、失败、flush、close 下的行为

v2.2.6 只在 `sink()`/`rowKindDelete()` **同步抛异常**时重试（第 391-442 行，`Thread.sleep(500L*i)`，默认 `max.retries=1`）。一旦拿到 future 就 `break`，异步失败不重试，而是写进 `asyncError`，下一次 `write`/`flush` 抛出，作业失败。

| 场景 | invoke_delay | buffer_wait | cmd_latency | ack_freshness | buffer_size | pending |
|---|---|---|---|---|---|---|
| 正常 | `write` 时记 1 次 | 第一次 attempt 开始时记 1 次（含等 flush 触发和等许可，§2） | ack 成功时记 1 次 | ack 成功时记 1 次 | add 时 +，提交前 −，clear 后归 0 | 提交 +1，完成 −1 |
| 在途许可耗尽（Redis 慢） | 1 次 | 1 次，**包含等许可的时长**，会升高 | 1 次，升高 | 1 次 | 等许可期间 > 0 | 接近 `sink.max-in-flight-requests` |
| 同步异常后重试成功 | 1 次 | 1 次，取**首次**提交时刻 | 1 次，从**拿到 future 的那次 attempt** 开始算 | 1 次 | 同上 | 只在拿到 future 时 +1 |
| 同步异常，重试用尽 | 1 次 | 1 次 | 无 | 无 | 不变（原逻辑抛 IOException，作业失败） | 不变 |
| 异步失败（`t != null`） | 1 次 | 1 次 | **无** | 无 | — | −1 |
| future 一直不完成（v2.2.6 异步命令没有超时） | 1 次 | 1 次 | 无 | 无 | — | **一直 >0**；下一次 checkpoint `flush()` 的 `future.get(60s)` 按原逻辑超时抛错 |
| `flush()`（checkpoint 前） | — | 缓冲记录在这时提交，`buffer_wait` 会体现“等 checkpoint 才被刷出”的时长 | 正常 | 正常 | 归 0 | `flush()` 返回后在亚毫秒内归 0。指标回调可能晚于 `get()` 返回才执行（见下方说明），所以 `flush()` 返回的那一刻 `pending` 可能仍短暂 >0 |
| `close()` | — | 最后一次 flush 正常采样 | close 之前完成的会记；被 close 取消的 future 是 `t != null`，不记 | 同左 | — | 取消也会触发回调 −1 |
| `flushBuffer` 中途抛错后 `close()` 再次 flush | — | 已提交过的记录在 close 时会被重新提交，所以它们的 `buffer_wait` 会被采样两次 | — | — | 显示的是尚未提交的条数，与 `batchBuffer.size()` 不同 | —（只出现在作业失败/close 路径，不改代码） |
| 指标 disabled（§6.3） | 停止 | 停止 | 停止 | 停止 | 继续准确 | 继续准确 |

**cmd_latency 只记成功完成的那次（含同步重试场景下最终拿到 future 的那次 attempt），理由：**

1. v2.2.6 中同步失败的 attempt 根本没有拿到 future，命令很可能没有发出去，没有可测的“往返”。
2. 异步失败在 v2.2.6 是致命的，紧接着作业就会重启，最多只有几个样本。这些样本测的是错误路径的耗时（比如连接 reset 的探测时间），混进去会污染“Redis 往返”的分位数。失败本身已经由现有的 `numRecordsSendErrors` 和作业重启体现。
3. “命令卡住”的情况由 `pending_commands` 持续 >0、`cmd_latency_max_ms` 却很小或为 0 来识别，不依赖失败样本。

**ack_freshness 只在最终成功时记 1 次。buffer_wait 取首次提交时刻**，这样同步重试的 sleep 不会被算进 `buffer_wait`。

**`flush()` 返回与指标回调的时序**：`waitForInFlightFutures` 用阻塞的 `future.get(60s)` 等待，它向 `CompletableFuture` 的依赖栈压入一个 `Signaller`，压入时间晚于两个 `whenComplete`。`postComplete` 按 LIFO 执行，所以完成方线程会**先**唤醒 `get()` 的等待者，**再**执行指标回调（`pending--`、`cmd_latency` 更新）和原回调。因此 `flush()` 返回时，`pending` 和 `cmd_latency_ms_count` 可能还没更新，要再过亚毫秒才一致。这只影响指标与 `flush()` 返回之间的先后，不影响写入语义；原回调本来就有同样的时序，v2.2.6 已经如此。测试中的断言必须等完成方线程结束（§14.1-4）。

**关于“三段之和”**：对单条记录，`ack_freshness ≈ invoke_delay + buffer_wait + cmd_latency`。差异有三个来源：同步重试的 sleep（属于第一次和最后一次 attempt 之间）；墙钟与单调钟各自取样的微秒级误差；回调调度延迟（含 §12-2 的回调执行顺序）。**分位数不能相加**，只能逐段对比。

## 10. 开关（决策：不加）

v1.1 曾提议可选 WITH 选项 `sink.metrics.latency.enabled`（默认开）。评审指出它没有用户授权出处，用户也明确“不会增加任何参数”。本版**不新增任何选项**，`RedisOptions` 和 `RedisDynamicTableFactory` 不改动。

- 不需要它也能满足所有需求：指标只读、不影响写入（§12）；需要撤掉时，回退到原 v2.2.6 jar 即可，和本次上线方式（只换 jar）对称。
- 测试中的“无指标”对照组用 `metricGroup == null` 构造 writer（原代码已有这个分支，S0 也在其内），不需要开关。
- 若以后确有“同一 jar、按作业关闭”的需求，作为待用户确认项（§18-3），不在本次实现。

## 11. 错误处理

| 操作 | 失败条件 | 可恢复性 | 调用方得到什么 | 日志 |
|---|---|---|---|---|
| 指标注册 `tryRegister` | 同名指标已存在（Flink 只记 WARN、不抛异常）；未预期的 `RuntimeException` | 可恢复 | 捕获后返回 `null`，writer 正常运行，只是没有新指标 | 由 Flink 打重名 WARN；未预期异常在 task 线程打一行 WARN，只带异常信息，**不打印 `flinkConfigBase`**（它的 `toString()` 第 62-70 行含明文 password） |
| task 线程的采样（S1、S5 及 `resolveEventTime`） | 理论上不会抛（只有算术运算和 volatile 写）；防御上在门面方法内 `try/catch (RuntimeException)` | 可恢复 | 不向 writer 抛出；`disableCause` CAS 记录首个异常，`disabled=true`，之后延迟采样全部 no-op（§6.3 规则） | task 线程立即打 1 行 WARN（`disableWarned` 保证每个 writer 最多 1 行） |
| 回调线程的采样（S6） | 同上 | 可恢复 | 异常在回调内的 `try/catch` 中吞掉，不会落到原 future 或原回调；`pending--` 在 try 之前已执行；设置 `disableCause`/`disabled` | **回调线程不打日志**；task 线程下一次进入任一门面入口时由 `skipBecauseDisabled()` 补打那 1 行 WARN |
| `setBuffered`（S2-S4） | 不会抛（volatile 写） | — | disabled 后仍执行，保证 `buffer_size` 准确 | 无 |
| `onSubmitted`（S6，位于 `sendRecord` 的重试 try 块内） | `pending++` 不会抛；`whenComplete` 注册理论上不会抛，防御上**内部 try/catch，任何异常都不外泄到 `sendRecord`**，否则会被 `catch (Exception e1)` 接住并重发同一条 HSET | 可恢复 | 注册失败时撤销 `pending+1`、置 disabled，`sendRecord` 照常 `break`；disabled 后仍执行 `+1` 和注册，保证 `pending_commands` 准确 | task 线程立即打 1 行 WARN（与其他入口共用 `disableWarned`，每个 writer 最多 1 行） |
| `Context` 为 null、时间戳为 null 或 ≤0、水位线无效（`Long.MIN_VALUE`、≤0、`Long.MAX_VALUE`） | 正常情况 | — | 时间戳无效但水位线有效时回退到水位线（source=2）；两者都无效时记为 NONE（source=0），跳过两个指标 | 每种来源第一次出现时打 1 行 INFO |

**不变量的归属：**

- “指标永远不影响写入”由 `RedisSinkWriter` 的接入方式保证：只追加语句、独立的 future stage、全部有 null 守卫。`RedisSinkLatencyMetrics` 内部的异常隔离是第二层保障。
- “`pending` 与已提交未完成的主写命令一致、且 ≥ 0”由门面保证：`onSubmitted` 无论是否 disabled 都先 +1 再注册回调，回调无条件 −1；gauge 钳到 0 只是兜底。
- “`buffer_size` = 未提交数”由 writer 保证：只有 task 线程写，`write` 不会在 `flushBuffer` 期间重入（mailbox `tryYield` 只执行 mail，不执行 `processElement`），`flushBuffer` 自身由 `flushing` 防重入。

## 12. 行为不变的证明点

实现 PR 必须逐条满足，评审时对照 diff 检查：

1. **以下代码不修改任何已有语句（只在指定位置追加）**：
   - 构造函数：只在 `if (metricGroup != null)` 块末尾追加 S0 一行；container 构建与 `open()`、batch/semaphore/限速器初始化的语句和顺序都不动；
   - flush 触发条件（第 239-245 行）；
   - `flushBuffer` 的循环顺序、限速 `cooperativeWait`、`acquireInFlightPermit`、`checkAsyncError` 的位置、计数器、异常路径（第 276-327 行）；
   - `sendRecord` 的重试次数、`Thread.sleep`、permit 释放、`inFlightFutures` 维护、**原有 `whenComplete` 的 lambda 内容**（第 390-444 行）；
   - `setTtl`、`sink`、`rowKindDelete`、`waitForInFlightFutures`、`checkAsyncError`、`close` 的顺序；
   - `RedisClientBuilder`/`RedisContainer` 的任何超时、连接、重连参数；`RedisOptions`、`RedisDynamicTableFactory`、`RedisSink` 都不改。
   - 唯一的“结构性”改动是 `PendingRecord` 构造函数增加两个参数，以及 S2 处 `new PendingRecord(...)` 的实参变化。**不新增构造函数重载**（单测改用反射注入，§14.1-4）。
2. **新增的 `whenComplete` 是同一个 future 上的另一个独立依赖**：它不改变 future 的值、完成时刻和异常；`waitForInFlightFutures` 的 `future.get` 和原回调都不受影响；它返回的 stage 被丢弃。新增回调注册在原回调之后，但 `CompletableFuture`（Lettuce `AsyncCommand` 继承它）的依赖栈是 LIFO，`postComplete` 通常**先执行后注册的依赖**，所以实际上指标回调一般在原回调（释放许可、`setTtl` 发 EXPIRE、记录 asyncError）**之前**执行。它只增加 O(1)（约百纳秒）的延迟，不改变许可释放和 EXPIRE 的发出顺序（两者都在原回调内部，相对顺序不变）。两者之间没有数据依赖，执行顺序不影响原有语义。S6 处于 `sendRecord` 的重试 try 块内，`onSubmitted` 内部 try/catch 保证它不向外抛异常（§6.3），所以不会因指标代码触发 HSET 重发。对指标的影响：指标回调先执行时，`cmd_latency` 的 ack 时刻不包含原回调的执行时间；future 在注册时已经完成（同步执行）或并发完成导致顺序相反时，最多包含原回调的几微秒（`setTtl` 只是异步入队）。
3. **没有新线程、定时器、`ProcessingTimeService` 注册、阻塞调用或 mailbox 交互。**
4. **`PendingRecord` 只增加字段**。它不参与序列化或 checkpoint 状态（writer 没有状态），所以对 savepoint/checkpoint 恢复零影响，sink 算子的 uid 也不变。
5. **没有新 WITH 选项**；现有 SQL 的校验结果不变。
6. **日志**：只在 task 线程上新增 ≤3 行 INFO（基准来源）和最多 1 行 WARN（异常），都不包含配置对象、key 和 row 内容。
7. **可测试的等价性**：§14.1-4 的“调用序列等价”单测分别在有指标（`metricGroup` 为 Proxy）和无指标（`metricGroup == null`）时，对同一组输入断言 `RedisCommandsContainer` 收到的调用序列（方法名、参数、顺序）以及抛出的异常完全一致。真实的 container 构建路径（`RedisCommandsContainerBuilder.build` + `open`）本身未改动，由 §14.1-6 的回归测试覆盖。

如果实现时发现任何一点必须改动已有语句才能做到，**暂停实现，提交给用户决定**，不擅自更改行为。

## 13. 性能开销

**每条记录（task 线程）**：

- 时钟调用：2 次 `nanoTime`、1 次 `currentTimeMillis`，各约 20–25ns；
- `invoke_delay` 和 `buffer_wait` 各一次三件套更新：2 次无竞争的 synchronized histogram 写、2 次 synchronized max 写、2 次 volatile 写；
- 一次 `AtomicInteger` 自增、几次 volatile int 写、几次 volatile boolean 读；
- 一次额外的 `whenComplete` 依赖对象，约 40–60 字节，属于短命垃圾。

**每条记录（netty 线程）**：2 次时钟调用、2 次三件套更新、1 次自减。

合计约 **<1µs/记录、<100 字节垃圾**。对比现有每条记录的开销（RowData 转字符串、`numBytesSend` 里的 `getBytes`、Lettuce 命令编码与 IO，都是数微秒以上），以及生产 `scan.rate-limit.records-per-second=800` 的量级，可以忽略。

**内存**：每个 writer 有 4 个 histogram × 1024 × 8B ≈ 32KB，4 个 max 各 60×16B ≈ 4KB，合计约 36KB；limit-s 一共 52 个 writer，约 1.9MB，分散在各个 TM 上。

**Reporter 线程**：每次抓取，每个 histogram 拷贝 1024 个 double 并做一次分位数计算（约数十微秒），每个 max gauge 遍历 60 个桶。按 limit-s 的 208 个 histogram 算，每次抓取合计约 10ms 量级，分散在多个 TM 上。

## 14. 测试计划

### 14.1 单元与集成测试（JUnit5，放在 `src/test/java/.../redis/metrics/` 和 `.../redis/table/`）

1. **`SlidingWindowMaxTest`**（假时钟）：
   - 空窗口返回 0；**假时钟从 0 纳秒开始（第 0 秒），空窗口 `get()` 返回 0**；假时钟为负值时 update/get 正确；
   - 同一秒内多次 update 取最大值；
   - 第 59 秒的样本仍可见，第 60 秒后过期；
   - 下标回绕后，旧桶被覆盖而不是叠加；
   - 读取不清零；
   - 4 个线程并发 update 加 1 个线程并发 get，结束后最大值正确，过程中不抛异常。
2. **`LatencyMetricTest`**：
   - 负数钳成 0；
   - `last` 等于最后一次写入的值；
   - histogram 的 `getCount` 正确；
   - 在捕获型 `MetricGroup`（`java.lang.reflect.Proxy` 实现，记录 `histogram`/`gauge` 的注册调用）上，注册的三个名字正确。
3. **`RedisSinkLatencyMetricsTest`**：
   - `resolveEventTime`，每个用例同时断言返回值和 `event_time_source` gauge：
     - ts 有效 → 返回 ts，source=1；
     - **ts 为 null、wm 有效 → 返回 wm+1，source=2**（用户要求的必测项）；
     - ts ≤ 0、wm 有效 → 回退到水位线，source=2；
     - **ts 为 null、wm 为 `Long.MIN_VALUE` → 返回 NONE，source=0**；后续 `onInvoke` 和 ack 回调都没有 `invoke_delay`/`ack_freshness` 样本（用户要求的必测项）；
     - wm 为 0 或负数 → NONE；wm 为 `Long.MAX_VALUE` → NONE；ctx 为 null → NONE；
     - 来源切换：先 NONE、再 WATERMARK，gauge 反映最近一条（0→2）；
   - 回退场景的数值：注入墙钟 `now`，`wm = now − 8000`，ts 为 null，断言 `invoke_delay == 7999`；
   - `onSubmitted` 分别传入“已完成 / 稍后完成 / 异常完成”的 future（用 Lettuce `AsyncCommand` 手动 `complete`/`completeExceptionally`）：检查 pending 的变化，`cmd_latency` 和 `ack_freshness` 只在成功时有样本，NONE 时没有 `ack_freshness`；
   - **disabled 语义**（只直接调用门面，不经过 writer；见 §6.3 `nanoTime()` 约定）：注入的纳秒钟由一个 `AtomicBoolean throwNow` 控制，`throwNow==false` 时返回假时间，为 true 时抛 `RuntimeException`。测试先在 `throwNow==false` 下调用 `onSubmitted(future, ...)`（`submitNanos` 由测试直接传入，不经时钟），**在调用 `future.complete(...)` 之前**把 `throwNow` 置为 true，于是第一次抛异常一定发生在回调中的 `cmdLatency.update(...)` 计算里。之后将 `throwNow` 复位为 false，再调用后续入口。断言：异常没有传到 future 的其他依赖；`pending` 回到 0；之后的 `onInvoke`/`onFirstSubmit` 不再产生样本；disabled 后 `onSubmitted` 仍然让 `pending` +1，完成后 −1；`setBuffered` 仍然生效；用 `LoggerAuditingExtension`（见下）断言 WARN 恰好 1 条，且该事件的 `getThreadName()` 是测试主线程（即 task 线程的下一次入口调用打出的，回调线程没有打）；
   - **`onSubmitted` 注册失败**：传入一个 `whenComplete` 抛 `RuntimeException` 的 `RedisFuture`（Proxy 实现）。断言 `onSubmitted` 不抛异常、`pending` 回到 0、之后进入 disabled、WARN 恰好 1 条；
   - **日志捕获方式**：用 `flink-test-utils-junit` 的 `org.apache.flink.testutils.logging.LoggerAuditingExtension`：
     ```java
     @RegisterExtension
     final LoggerAuditingExtension logs =
             new LoggerAuditingExtension(RedisSinkLatencyMetrics.class, org.slf4j.event.Level.WARN);
     // 断言：logs.getEvents() 中 level==WARN 的事件恰好 1 条
     ```
     已核实：`flink-test-utils` 2.2.1（本项目 test scope 已有）以 compile scope 依赖 `flink-test-utils-junit` 2.2.1，后者以 compile scope 依赖 `log4j-core`、`log4j-api`、`log4j-slf4j-impl` 和 `junit-jupiter`，所以都在 test classpath 上；2.2.1 的这个类构造函数签名是 `(Class<?>, org.slf4j.event.Level)`，按 logger 名挂一个内存 appender，只捕获 ≥ 指定级别的事件（INFO 的来源日志不会被捕获）。实现时用 `mvn dependency:tree -Dscope=test` 再确认一次；如果因依赖调解缺失，在 test scope 显式加 `org.apache.flink:flink-test-utils-junit:${flink.version}`，不引入其他日志库。`RedisSinkLatencyMetrics` 的 logger 必须是 `LoggerFactory.getLogger(RedisSinkLatencyMetrics.class)`，与扩展监听的名字一致；
   - `tryRegister` 在一个注册时抛异常的 MetricGroup 上返回 `null`，不向外抛异常；`getAllVariables()` 不含 `<subtask_index>` 时日志用 `?`。
4. **`RedisSinkWriterLatencyMetricsTest`**（照 v2.2.9 `RedisSinkWriterConnectionClosedTest` 的模式：用真实构造函数创建 writer，连接本地测试 Redis，先 `close()` 掉真实 container，再用反射把非 final 字段 `redisCommandsContainer` 替换为 Proxy 实现的假 `RedisCommandsContainer`：`hset`/`expire` 返回可控的 `AsyncCommand`。`SinkWriterMetricGroup` 用 Proxy 实现，`getAllVariables` 返回含 `<subtask_index>` 的 map；`SinkWriter.Context` 用假实现。需要本地 Redis 仅为了通过构造函数，命令本身全部打到假 container。writer 经由 S0 的 `tryRegister(metricGroup)` 使用真实的 `System::nanoTime`/`System::currentTimeMillis`，本类**不注入任何会抛异常的时钟**，见 §6.3 `nanoTime()` 约定）：
   - 注册的名字恰好是 12 个延迟形态加 3 个 gauge（`buffer_size`、`pending_commands`、`event_time_source`）；
   - **跨线程断言的通用规则**（§9 “`flush()` 返回与指标回调的时序”）：`complete()` 返回时，该 future 上的同步依赖（含指标回调）已在调用 `complete()` 的线程上执行完；但被 `get()` 唤醒的线程可能先于指标回调返回。所以：future 在**测试线程**上 `complete()` 时，`complete()` 返回后即可断言；在**另一个线程**（下称 completer）上完成时，必须先 `completer.join()` 再断言 `pending`、`cmd_latency`/`ack_freshness` 的 count。不允许在 `flush()`/`write()` 返回后直接断言这些值；
   - `sink.batch.size=3`：写 2 条时 `buffer_size==2`、`hset` 没被调用；写第 3 条后 `buffer_size==0`、`pending==3`；在测试线程上依次 `complete` 这 3 个 future 后，`pending==0`，`cmd_latency` 的 count 为 3；
   - **许可耗尽**：`sink.max-in-flight-requests=2`、`sink.batch.size=3`、future 不完成。测试线程启动 completer 线程后调用第 3 条 `write`（它阻塞在 `acquireInFlightPermit()`）。completer 线程**先轮询**（每 5ms 一次，上限 5s，超时则记录失败标志、测试判失败）直到 `pending_commands==2 && buffer_size==1`。这个条件成立说明前两条已提交、第 3 条已进入 `write()` 并阻塞在 `acquireInFlightPermit()`，即第 3 条记录的 `invokeNanos` 早于此刻。**之后**再 `sleep(200ms)`，然后读 `pending_commands` 和 `buffer_size` 两个 gauge，存入两个 `AtomicInteger` 快照，**最后**完成第一个 future。由于 sleep 开始时刻晚于 `invokeNanos`，第 3 条的 `buffer_wait` 必然 ≥ 200ms，下面的 `≥ 200` 断言是确定成立的，不会因截断偶发失败。
     - 本用例使用的纳秒钟和墙钟都是不抛异常的真实时钟（`System::nanoTime`/`System::currentTimeMillis`）。测试线程在 `write` 返回并 `completer.join()` 之后断言：快照 `pending_commands==2`、`buffer_size==1`（验证 §15.2 的“等许可”判据）；`redis_sink_buffer_wait_last_ms`（第 3 条记录是最后一个 `buffer_wait` 样本）≥ 200，`redis_sink_buffer_wait_max_ms` ≥ 200；`cmd_latency` 的 count 为 1；
   - `context.timestamp = now−5000` 时，`invoke_delay` 和 `ack_freshness` 都 ≥ 5000，`event_time_source==1`；
   - `context.timestamp = null`、`currentWatermark = now−5000` 时，两个指标都 ≥ 4999，`event_time_source==2`；
   - `context.timestamp = null`、`currentWatermark = Long.MIN_VALUE` 时，写入照常（hset 调用不变），`invoke_delay`/`ack_freshness` 的 count 都是 0，`buffer_wait`/`cmd_latency` 照常有样本，`event_time_source==0`；
   - 第一次 `hset` 同步抛异常、第二次成功（`max.retries=1`）：`hset` 被调用 2 次（原有行为），`buffer_wait`/`cmd_latency`/`ack_freshness` 的 count 都是 1；
   - 异步失败：下一次 `write` 仍然抛 `IOException("Async Redis write failed")`（原有行为），`pending` 回到 0，没有 `cmd_latency` 样本；
   - `flush(false)` 时，用 completer 线程在 100ms 后完成 future：`flush` 正常返回；`completer.join()` 之后断言 `pending==0`、`cmd_latency` 的 count 为 1（不在 `flush` 返回后立即断言，原因见上面的通用规则）；
   - **调用序列等价**：同一组输入（包括一次同步异常、一次异步失败），`metricGroup` 为 Proxy 和为 `null` 两种情况下，container 记录的调用序列和抛出的异常完全一致；`null` 时不注册任何指标。
5. **`EventTimeReferenceTest`**（MiniCluster，不需要 Redis；验证 §3）：
   - 测试专用的 `DynamicTableSinkFactory`，标识为 `ts-probe`，注册在 `src/test/resources/META-INF/services/org.apache.flink.table.factories.Factory`，提供 SinkV2 writer，把 `(context.timestamp(), context.currentWatermark(), 行中的 wt)` 写进一个静态队列。
   - **时区与类型（写死，防止断言空过）**：
     - `tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC)`；
     - 源表的 `creationTime` 是**物理列 `TIMESTAMP(3)`**（与生产一致，不用 `fromDataStream` 的 `TIMESTAMP_LTZ(3)` 元数据 rowtime）。DataStream 元素类型是 `Row`，字段 `(k STRING, hk STRING, amount BIGINT, creationTime TIMESTAMP(3))`，`creationTime` 字段值由 `LocalDateTime.ofEpochSecond(ms / 1000, (int) (ms % 1000) * 1_000_000, ZoneOffset.UTC)` 生成（`ms ≥ 0`）；
     - DataStream 上 `assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forMonotonousTimestamps().withTimestampAssigner((r, p) -> ms(r)))`，时间戳与 `creationTime` 的 UTC 解释是同一个 epoch 值；
     - `tEnv.fromDataStream(ds, Schema.newBuilder().column("k", "STRING").column("hk", "STRING").column("amount", "BIGINT").column("creationTime", "TIMESTAMP(3)").watermark("creationTime", "SOURCE_WATERMARK()").build())`；
     - 所有从 `TimestampData`/`LocalDateTime` 到毫秒的换算统一用 `epochMillis(x) = x.toInstant(ZoneOffset.UTC).toEpochMilli()`（`TimestampData` 先 `toLocalDateTime()`）。
   - **数据节奏**：`env.fromData(rows)`（`Row` 类型信息用 `Types.ROW_NAMED(...)`，`creationTime` 为 `Types.LOCAL_DATE_TIME`）后接一个 `map(r -> { Thread.sleep(5); return r; })` 限速，再 `assignTimestampsAndWatermarks`，不引入新依赖。生成 600 条，事件时间 `ms = i * 100`（0 到 59_900），`k` 固定 1 个、`hk` 取 2 个值，`amount = 1`。`pipeline.auto-watermark-interval=20ms`，并行度 1。这样每次水位线跳变约 100–400ms，最后一条落在 59_900，最后一个 10s 窗口在 60_000 结束，输入结束时 MAX_WATERMARK 一次性触发的窗口离最后一个水位线也不超过约 1s。
   - 拓扑 (a)：和生产同形，CUMULATE(1s, 10s) GROUP BY k, hk 求 `SUM(amount)` → LAG OVER (PARTITION BY k, hk ORDER BY window_time) → `SELECT k, hk, CAST(v AS STRING), CAST(window_time AS TIMESTAMP(3)) wt ... WHERE previous_value IS NULL OR v <> previous_value`（`CAST` 去掉了 rowtime 属性，不会插入 `StreamRecordTimestampInserter`）。断言：
     1. 每行 `timestamp()==null`；
     2. 至少有一行 `wm != Long.MIN_VALUE`（否则断言 3、4 等于空过，测试判失败）；
     3. 对每个 `wm != Long.MIN_VALUE` 的行，**上界** `wm + 1 ≤ epochMillis(wt)`；
     4. 对同样的行，**下界** `epochMillis(wt) − (wm + 1) < 10_000`。差值等于 `T − (W_prev + 1)`，小于一次水位线跳变（§3.3）。流式过程中跳变约 100–400ms；输入结束时 MAX_WATERMARK 触发的剩余窗口，`T ≤ 59_999` 而 `W_prev ≈ 59_899`，差值约 100ms。所以实际差值应在 1s 内。10s（CUMULATE 的最大窗口长度）是留足余量的硬界，按评审建议取值。若时区或类型配错（例如 LTZ 在非 UTC 会话时区下转换），差值会是整小时量级，断言 3 或 4 必然失败。
   - 拓扑 (b)：SQL 与 (a) **完全相同**（CUMULATE(1s, 10s) → LAG OVER → `WHERE previous_value IS NULL OR v <> previous_value`），唯一区别是 SELECT 写 `window_time AS wt`（**不 CAST**，保留 rowtime 属性），probe sink 的列为 `wt TIMESTAMP(3)`；同样在 UTC 会话时区下执行。断言每行 `timestamp()==epochMillis(wt)`，且行数 > 0。两个拓扑写成同一个类里的两个 `@Test` 方法，便于在 surefire 报告中核对都已执行。
6. **回归**：现有 `SQLInsertTest`、`SQLExpireTest`、`SinkMetricsTest`、`LimitedSinkTest` 等全部通过，覆盖真实 container 构建与写入路径。这些测试需要本地 Redis `127.0.0.1:6379`，密码 `test123`。

**单测与集成测试的划分**：§14.1-1、2、3、5 不需要 Redis；§14.1-4 只在构造时需要本地 Redis；§14.1-6 是真实 Redis 集成测试。

### 14.2 构建

```
mvn spotless:apply && mvn -q verify
```

spotless check 绑定在 compile 阶段，不先 apply 就会编译失败。

**测试类命名约束**：`pom.xml` 第 302-306 行只声明了 `maven-surefire-plugin` 3.0.0-M5，没有 `<includes>`，也没有 failsafe 插件。surefire 默认只执行 `**/Test*.java`、`**/*Test.java`、`**/*Tests.java`、`**/*TestCase.java`。因此本次所有新增测试类一律以 `Test` 结尾（包括 `EventTimeReferenceTest`），不改 pom 的插件配置。实现完成后，在 `target/surefire-reports/` 中确认 `EventTimeReferenceTest` 的两个拓扑用例（(a)、(b)）都已执行且没有被 skip（`TEST-...EventTimeReferenceTest.xml` 中 `tests="2" skipped="0" failures="0" errors="0"`）。**本机目前没有 JDK 和 Maven**，实现步骤需要先安装 JDK 11 和 Maven 3.8+（例如 `brew install openjdk@11 maven`，属于中等风险，执行前说明），或者用 `docker run maven:3.9-eclipse-temurin-11` 构建。打包：`mvn -DskipTests package`。pom 版本改成 `2.2.6.1`，和上游 `v2.2.7+`（重试和超时行为不同）区分开。

### 14.3 测试环境（无锡）验证

只换 jar，SQL 和参数都不动。

1. `curl <TM>:9249/metrics | grep redis_sink_`，核对指标名和 label 集合（§4.1）。
2. TM 日志里有 `event-time reference source=WATERMARK`。
3. 用 Redis `MONITOR` 采样，命令形态仍然是每条 HSET 后跟一条 EXPIRE；`numRecordsSend` 的速率和换 jar 前一致。
4. 发 iS0005 测试交易，对照 `HGET` 轮询到值的时间和 `ack_freshness_last_ms`。
5. 观察低流量 sink 算子的 `buffer_wait_max_ms` 是否出现接近 checkpoint 间隔（60s）的值，同时 `pending_commands` 远低于 1000（即“等 flush 触发”分支）。

## 15. 文档、PromQL 与序列数估算

### 15.1 文档改动

- `README.md` 新增 “3.6 Sink 分段延迟指标” 一节，内容包括：导出名表（§4.1）、每个指标的含义和窗口时间基准说明（§3.4、§3.5）、`buffer_wait` 的组成（§2）、三种形态的用途（§4.2）、下面的 PromQL 和解读规则、序列数估算。**必须包含两段醒目的口径说明**（下面是 README 原文草稿）：

  > `invoke_delay_ms` 与 `ack_freshness_ms` 的“窗口时间”优先取记录自带的时间戳（`event_time_source=1`，精确）。拿不到时（例如 OVER 之后接了 WHERE/投影的 SQL，这是最常见的情况）退回 `当前 watermark + 1`（`event_time_source=2`），此时这两个值是**上界**：偏大量小于一次 watermark 跳变。低流量时 watermark 一次可能跳十几秒，所以这两个值可能被放大十几秒。判断慢在哪一段时，请以 `buffer_wait_ms` 和 `cmd_latency_ms` 为准，它们是精确值。`event_time_source=0` 表示没有可用基准，这两个指标不产生样本（`_count` 不增长）。

  > `buffer_wait_ms` 是“进入 sink 到命令真正发给 Redis”的全部等待，包括：等 flush 触发（下一条记录 / batch 满 / checkpoint）、`sink.write.qps` 限速、等在途许可（`sink.max-in-flight-requests`）。所以 `buffer_wait_ms` 高时要看 `pending_commands`：接近 `sink.max-in-flight-requests`，说明 Redis/网络回不过来，根因在 Redis 一侧；远低于上限，才是记录在 sink 缓冲里等 flush。
- `README-en.md` 加一节对应的英文简版。
- 本设计文档随分支提交。`.agents/` 不提交。

### 15.2 推荐 PromQL

`dc` 必须是 Prometheus 侧已有、能区分 A/B 的 label（来自 external_labels 或 relabel，connector 不产生它）。halykbank-flink 仓库里没有找到 `dc` 的定义，**按实际 label 名替换**（比如 A/B 两套 Prometheus 各自查询时，就去掉 `by (dc)`）。

```promql
# 端到端：每个 DC 中最差 subtask 的 p50，以及 60s 内的最大值
max by (dc) (flink_taskmanager_job_task_operator_redis_sink_ack_freshness_ms{job_name="$job", quantile="0.5"})
max by (dc) (flink_taskmanager_job_task_operator_redis_sink_ack_freshness_max_ms{job_name="$job"})

# 三段拆分（同一面板放三条线；按算子拆开时改成 by (dc, operator_name)）
max by (dc) (flink_taskmanager_job_task_operator_redis_sink_invoke_delay_max_ms{job_name="$job"})
max by (dc) (flink_taskmanager_job_task_operator_redis_sink_buffer_wait_max_ms{job_name="$job"})
max by (dc) (flink_taskmanager_job_task_operator_redis_sink_cmd_latency_max_ms{job_name="$job"})
# p50 版本：把 _max_ms 换成 _ms{quantile="0.5"}

# 积压在哪一层
max by (dc, operator_name) (flink_taskmanager_job_task_operator_redis_sink_buffer_size{job_name="$job"})
max by (dc, operator_name) (flink_taskmanager_job_task_operator_redis_sink_pending_commands{job_name="$job"})
sum by (dc) (flink_taskmanager_job_task_operator_redis_sink_pending_commands{job_name="$job"})

# 在途许可是否耗尽（单 subtask；1000 为 sink.max-in-flight-requests 默认值，生产未配置）
max by (dc, operator_name) (flink_taskmanager_job_task_operator_redis_sink_pending_commands{job_name="$job"}) >= 0.9 * 1000

# 口径识别：2 = watermark 回退（上界），1 = 精确，0 = 无基准
max by (dc, operator_name) (flink_taskmanager_job_task_operator_redis_sink_event_time_source{job_name="$job"})

# 样本是否还在产生；基准缺失时 invoke_delay 的速率为 0 而 buffer_wait 的速率 > 0
sum by (dc) (rate(flink_taskmanager_job_task_operator_redis_sink_ack_freshness_ms_count{job_name="$job"}[1m]))
sum by (dc, operator_name) (rate(flink_taskmanager_job_task_operator_redis_sink_invoke_delay_ms_count{job_name="$job"}[5m]))
sum by (dc, operator_name) (rate(flink_taskmanager_job_task_operator_redis_sink_buffer_wait_ms_count{job_name="$job"}[5m]))
```

**解读方法**（按顺序判断）：

1. 先看 `event_time_source`：等于 2 时，`invoke_delay`/`ack_freshness` 是上界，可能被低流量下的 watermark 跳变放大到十几秒；等于 0 时这两个指标没有样本，只能用后两段判断。
2. `invoke_delay` 高，后两段低：慢在 Kafka→Flink→计算（含等水位线），或者是回退基准的高估。两者都属于“上游+计算”一侧，不在 sink 里。
3. `buffer_wait` 高，**且同一 subtask 的 `pending_commands` 接近 `sink.max-in-flight-requests`**（≥ 0.9×上限，生产即 ≥ 900）：在途许可耗尽，记录在等前面的命令 ack。**根因在网络/Redis**，归到 `cmd_latency` 一侧（此时 `cmd_latency` 通常也高；若 `cmd_latency` 无样本而 `pending` 不降，就是命令卡住）。
4. `buffer_wait` 高，**且 `pending_commands` 远低于上限**、`buffer_size>0`：记录在 sink 缓冲里等 flush 触发（低流量算子要等下一条记录，或者等 checkpoint 才刷出）。根因在 sink 缓冲策略。
5. `cmd_latency` 高，或 `pending_commands` 持续 >0 且不下降：慢在网络或 Redis。

**注意**：summary 的 quantile 跨 subtask 做 `max by`，含义是“最差 subtask 的 p50”，不是全局 p50。需要真正跨 subtask 聚合时，看 `_max_ms`。`pending_commands` 判据要按单个 subtask 看（每个 writer 一个许可池），不能用 `sum`。

### 15.3 序列数估算

| 项 | 每个 writer 的序列数 |
|---|---|
| 4 个 summary × (6 个 quantile + `_count`) | 28 |
| 4 × (`_max_ms` + `_last_ms`) | 8 |
| `buffer_size` + `pending_commands` | 2 |
| `event_time_source` | 1 |
| **合计** | **39** |

limit-s 有 13 个 sink 算子 × 并行度 4 = 52 个 writer，即 **每个 DC 约 2,028 个新序列，A+B 合计约 4,056 个**。label 都是现有的 operator scope 变量，作业重启后 `task_attempt_id`/`tm_id` 会变，旧序列在 Prometheus 中自然过期，和现有 operator 指标的行为一样。

## 16. 风险

1. **窗口时间基准在生产上是上界**（§3.3）：比真实值偏大，偏大量小于一次 watermark 跳变，低流量时可能达到十几秒。缓解：`event_time_source` gauge 标明口径，README 写明以 `buffer_wait`/`cmd_latency` 为准。基准依赖 `creationTime` 是 UTC（§3.5），也依赖生产方与 TM 的时钟偏差。缓解：在测试环境对照 `watermarkLag`，并在 README 中写明。
2. **`buffer_wait` 混合了等 flush 与等许可**（§2）：单看它会把 Redis 慢误判为缓冲慢。缓解：§15.2 的三分支解读规则和 README 口径说明，强制结合 `pending_commands` 判断。
3. **分位数会停留在旧样本上**：1024 个样本窗口在低流量时更新很慢。缓解：用 `_max_ms`/`_last_ms` 加 `_count` 的 rate 一起判断。
4. **summary 不可以跨 subtask 聚合**：已在 PromQL 说明中写明。
5. **亚毫秒截断**：Redis 往返通常显示为 0–1ms。目标量级是数十秒，可以接受。
6. **命令卡住时没有 `cmd_latency` 样本**（v2.2.6 异步命令不超时）：靠 `pending_commands` 识别。
7. **`DescriptiveStatisticsHistogram` 在 flink-runtime 中**，不属于公共 API：升级 Flink 时编译期就会暴露变化；这个类在 2.2 中的行为已核对（§7）。
8. **新增约 2k 序列/DC**：可接受；没有开关，需要撤掉时回退原 jar。
9. **`set.if.absent`、ZADD+zrem 场景测到的不是真正的写入命令**（§5.1）：生产不涉及，已写入文档。
10. **密码泄露**：任何新增日志都不打印 `FlinkConfigBase` 和 row 内容；代码评审时检查。
11. **本机无法构建**：需要先安装工具链或用容器（§14.2）。§14.1-4 需要本地 Redis 才能构造 writer。

## 17. 实施清单

| 文件 | 改动 |
|---|---|
| `src/main/java/.../redis/metrics/SlidingWindowMax.java` | 新增 |
| `src/main/java/.../redis/metrics/LatencyMetric.java` | 新增 |
| `src/main/java/.../redis/metrics/RedisSinkLatencyMetrics.java` | 新增 |
| `src/main/java/.../redis/table/RedisSinkWriter.java` | 按 S0–S6 追加语句；`PendingRecord` 加两个 final 字段；新增 `latencyMetrics` 字段。不加构造函数重载 |
| `src/test/java/.../redis/metrics/*Test.java`、`.../table/RedisSinkWriterLatencyMetricsTest.java`、`.../table/EventTimeReferenceTest.java` 及 probe factory | 新增（所有测试类以 `Test` 结尾，保证被 surefire 默认规则执行，§14.2） |
| `src/test/resources/META-INF/services/org.apache.flink.table.factories.Factory` | 新增，注册 `ts-probe` |
| `README.md`、`README-en.md` | 指标文档 |
| `pom.xml` | 版本改为 `2.2.6.1`；如有需要，显式声明 provided 的 `flink-runtime` |

`RedisOptions.java`、`RedisDynamicTableFactory.java`、`RedisSink.java` **不改动**。

## 18. 偏离项与用户决策

1. **窗口时间基准增加 `currentWatermark()+1` 回退（§3.4 方案 C）：用户已确认采用。** 出处：设计第一轮本步骤暂停（severity=warning）后，workflow 父会话 `sess_48103828-1d22-42cf-97f6-609aaa2598a0` 转达的用户答复，通知 id `notify-797223f5-9102-4463-902b-19e694f88a32`，时间 2026-10-18T02:03:24Z，记录在设计步骤会话 `sess_12fde7c2-91f3-4518-b620-4158fbd61842` 的 messages.jsonl 第 205–206 条。原文：

   > 用户选择 (1) 采用方案 C：timestamp 非 null 用它，否则用 Context#currentWatermark()+1（watermark 为 Long.MIN_VALUE 或非正时跳过）。附加要求：1) 设计文档与 README 指标说明中明确 invoke_delay 与 ack_freshness 在回退到 watermark 基准时为“上界”，偏大量 < 一次 watermark 跳变，低流量时 watermark 跳变可能较大（可达十几秒），解读时需结合 buffer_wait/cmd_latency（精确）判断；2) 增加一个低基数的来源指示（例如 gauge redis_sink_event_time_source：0=无/跳过，1=record timestamp，2=watermark 回退，取最近一条），便于看板识别口径；3) 单元测试覆盖 timestamp 为 null 时走 watermark 回退、watermark 为 Long.MIN_VALUE 时跳过。其余需求不变，按此更新设计后进入评审。

   这条答复是父会话转述的用户选择，不在本 workflow 的 original_user_request（message 1–12）里。`redis_sink_event_time_source` gauge 也出自这条答复的附加要求 2。已定需求 3 原本只用 `Context#timestamp()`、null 时跳过，但源码证明它在生产上恒为 null。用户确认的规则是：timestamp 非 null（且 > 0）时用它；否则用 `currentWatermark()+1`；watermark 为 `Long.MIN_VALUE` 或非正时跳过（本设计另外排除 `Long.MAX_VALUE`，防止溢出）。用户的附加要求及落实位置：
   - 设计文档和 README 写明回退时两个指标是“上界”，偏大量小于一次 watermark 跳变，低流量时可达十几秒，解读时要结合精确的 `buffer_wait`/`cmd_latency`：见 §1、§3.3、§15.1（README 原文草稿）、§15.2、§16-1。
   - 低基数的来源指示 gauge `redis_sink_event_time_source`（0 = 无/跳过，1 = record timestamp，2 = watermark 回退，取最近一条）：见 §2、§3.4、§4.1、§6.3、§15.2、§15.3。
   - 单测覆盖“timestamp 为 null 时走 watermark 回退”和“watermark 为 `Long.MIN_VALUE` 时跳过”：见 §14.1-3 和 §14.1-4。
2. **基准缺失计数器 `redis_sink_event_time_missing_total`：删除，不实现。** 它没有用户授权出处，信息与 `event_time_source==0` 加 `invoke_delay_ms_count`/`buffer_wait_ms_count` 的速率对比重复（§15.2）。
3. **WITH 选项 `sink.metrics.latency.enabled`：不实现，列为待用户确认的 backlog。** 用户明确“不会增加任何参数”，而指标的撤回手段是回退 jar。只有用户以后提出“同一 jar 按作业关闭”的需求时再加，届时为可选、默认开、只控制注册。

## 评审意见响应

### 第 4 轮（评审 v1.3 → 本稿 v1.4）

针对 `/Users/administrator/GolandProjects/halykbank-flink/.agents/tasks/connector-metrics/design-review.md`（第 4 轮，CHANGES_REQUESTED，HIGH 0 / MEDIUM 1 / NIT 3）。4 条全部处理，没有 backlog 或忽略项：

| # | 级别 | 处理 | 落实位置 |
|---|---|---|---|
| F1 | MEDIUM `EventTimeReferenceIT` 不会被 `mvn verify` 执行 | **已解决**，按评审方案，不改 pom：已核对 `pom.xml` 第 302-306 行只有 surefire 3.0.0-M5、无 `<includes>`、无 failsafe。类名改为 `EventTimeReferenceTest`，全文同步。§14.2 新增“测试类命名约束”：所有新增测试类以 `Test` 结尾，实现后在 `target/surefire-reports/` 中确认 `EventTimeReferenceTest` 两个拓扑用例都已执行、`skipped="0"` | §3.6-1、§14.1-5、§14.2、§17 |
| F2 | NIT 许可耗尽用例 `≥ 200` 可能截断为 199 | **已解决**：completer 先轮询（5ms 间隔、5s 上限）到 `pending_commands==2 && buffer_size==1`，再 `sleep(200ms)`、拍快照、完成第一个 future；写明 sleep 起点晚于第 3 条的 `invokeNanos`，所以断言确定成立 | §14.1-4 |
| F3 | NIT `nanoTime()` 异常约定与抛异常时钟的触发点 | **已解决**：§6.3 写明 `nanoTime()` 就是 `return nanoClock.getAsLong();`，生产时钟不抛异常；经过 writer 路径的测试（§14.1-4）一律用真实时钟；会抛异常的时钟只在 `RedisSinkLatencyMetricsTest` 中直接调门面时使用。disabled 用例改为 `AtomicBoolean throwNow` 控制，在 `future.complete()` 之前置 true，保证首次异常发生在回调里 | §6.3、§14.1-3、§14.1-4 |
| F4 | NIT 拓扑 (b) SQL 形态未写定 | **已解决**：(b) 与 (a) 的 SQL 相同（CUMULATE → LAG → `WHERE` Calc），只把 SELECT 改为 `window_time AS wt`（不 CAST），probe sink 列为 `wt TIMESTAMP(3)`；断言 `timestamp()==epochMillis(wt)` 且行数 > 0。§3.6-1 写明它证明的是“OVER 后接 Calc 时，只要保留 rowtime 列，时间戳就会被补上” | §3.6-1、§14.1-5 |

### 第 3 轮（评审 v1.2 → v1.3）

针对 `/Users/administrator/GolandProjects/halykbank-flink/.agents/tasks/connector-metrics/design-review.md`（第 3 轮，CHANGES_REQUESTED，HIGH 0 / MEDIUM 2 / NIT 4）。6 条全部处理，没有 backlog 或忽略项：

| # | 级别 | 处理 | 落实位置 |
|---|---|---|---|
| F1 | MEDIUM 跨线程完成 future 后立即断言有竞态 | **已解决**。§9 新增 “`flush()` 返回与指标回调的时序” 说明：`get()` 的 `Signaller` 后压栈、LIFO 先被唤醒，指标回调可能晚于 `flush()` 返回；`flush()` 行改为 “返回后亚毫秒内归 0”。§14.1-4 新增跨线程断言通用规则：测试线程上 `complete()` 的可直接断言，completer 线程完成的必须 `completer.join()` 后再断言；`flush(false)` 和许可耗尽用例都改为 `join()` 后断言 `pending` 与 `cmd_latency` count | §9、§14.1-4 |
| F2 | MEDIUM `EventTimeReferenceTest` 时区与类型未定，可能空过 | **已解决**，按评审方案：`setLocalTimeZone(UTC)`；`creationTime` 为物理列 `TIMESTAMP(3)`，用 `LocalDateTime.ofEpochSecond(..., ZoneOffset.UTC)` 生成；`forMonotonousTimestamps` + `SOURCE_WATERMARK()`；`epochMillis` 统一按 UTC 换算；拓扑 (a) 增加下界 `epochMillis(wt) − (wm+1) < 10_000`，并要求至少一行 `wm != Long.MIN_VALUE`。另外固定了数据节奏（`fromData` + 限速 map、600 条、100ms 事件间隔、20ms 水位线间隔、并行度 1），让上下界都有确定的预期值，不引入新依赖 | §3.6-1、§14.1-5 |
| F3 | NIT WARN 计数的日志捕获方式未定 | **已解决**。指定 `org.apache.flink.testutils.logging.LoggerAuditingExtension(RedisSinkLatencyMetrics.class, org.slf4j.event.Level.WARN)`。已对照 Maven Central 的 2.2.1 POM 和 sources jar 核实：`flink-test-utils` → `flink-test-utils-junit`（compile）→ `log4j-core`/`log4j-slf4j-impl`/`junit-jupiter`（compile），类和构造函数签名都存在。实现时仍用 `mvn dependency:tree` 复核，缺失时在 test scope 显式加 `flink-test-utils-junit` | §14.1-3 |
| F4 | NIT `onSubmitted` 没有异常隔离 | **已解决**，按评审代码：`pending++` 后 try { `whenComplete` } catch (RuntimeException) { `pending--`；置 disabled；立即打 WARN }。§11 拆出 `onSubmitted` 单独一行，写明 “内部 try/catch，任何异常不外泄到 `sendRecord`”；§12-2 补充不会因指标代码触发 HSET 重发；§14.1-3 新增注册失败用例 | §6.3、§11、§12-2、§14.1-3 |
| F5 | NIT 许可耗尽用例的 gauge 读取线程未定 | **已解决**，按评审方案：completer 线程 sleep 200ms 后先把两个 gauge 快照存入 `AtomicInteger`，再完成第一个 future；测试线程在 `write` 返回并 `join()` 后断言快照，以及 `buffer_wait_last_ms`/`_max_ms` ≥ 200 | §14.1-4 |
| F6 | NIT “用户已确认” 没有原话出处 | **已解决**。§18-1 引用用户答复原文，附父会话 id、通知 id、时间戳和会话日志位置；§1、§2 改为指向 §18-1 | §1、§2、§18-1 |

### 第 2 轮（评审 v1.1 → v1.2）

针对 `/Users/administrator/GolandProjects/halykbank-flink/.agents/tasks/connector-metrics/design-review.md`（v1.1，CHANGES_REQUESTED，MEDIUM 3 / NIT 4）：

| # | 级别 | 处理 | 落实位置 |
|---|---|---|---|
| 1 | MEDIUM `buffer_wait` 含限速与许可等待，解读规则误判 | **已解决**。§2 写明 `buffer_wait` 的四个组成（等 flush 触发、限速（生产不生效）、等 in-flight 许可、批内排队）；§15.2 解读规则改为按 `pending_commands` 是否 ≥ 0.9×`sink.max-in-flight-requests` 分支，许可耗尽归因网络/Redis；新增许可耗尽的 PromQL；README 草稿新增第二段口径说明；§9 新增“在途许可耗尽”行；§14.1-4 新增许可耗尽单测；§16-2 新增风险。不改代码、不拆指标 | §2、§5 注、§9、§14.1-4、§14.3-5、§15.1、§15.2、§16-2 |
| 2 | MEDIUM `disabled` 语义未定义、§6.3 与 §11 不一致 | **已解决**，采用评审给出的方案：`volatile boolean disabled` + `AtomicReference<Throwable> disableCause` + 仅 task 线程的 `disableWarned`；回调无条件 `pending--`，延迟 update 包在 try/catch 里，回调不打日志；task 线程入口 `skipBecauseDisabled()` 补打唯一一行 WARN；`onSubmitted` 在 disabled 时仍 `pending++` 并注册回调；`setBuffered` 不受 disabled 影响；定义了 disabled 后每个 gauge 的取值。§11 表与 §6.3 一致 | §6.3、§7、§9 末行、§11、§14.1-3 |
| 3 | MEDIUM 计数器与开关无授权出处 | **已解决**，采用评审推荐方案：删除 `redis_sink_event_time_missing_total`，序列数改为每 writer 39；`sink.metrics.latency.enabled` 不实现，移到 §18-3 作为待用户确认项。`RedisOptions`/`RedisDynamicTableFactory` 不改；等价性测试的“无指标”对照改用 `metricGroup == null` | §2、§3.4、§4.1、§10、§11、§12-5、§14.1、§15.2、§15.3、§16-8、§17、§18-2/3 |
| 4 | NIT CompletableFuture LIFO 执行顺序 | **已解决**。§12-2 写明指标回调通常先于原回调执行，只增加 O(1) 延迟，不改变许可释放与 EXPIRE 的相对顺序；并说明对 `cmd_latency` ack 时刻的影响 | §12-2、§9 末段 |
| 5 | NIT `bucketSecond` 初值与注释不一致 | **已解决**。构造函数 `Arrays.fill(bucketSecond, Long.MIN_VALUE)`，`get()` 显式跳过哨兵；新增“假时钟从 0 开始空窗口返回 0”和负值时钟用例 | §8、§14.1-1 |
| 6 | NIT INFO 日志的 subtask index 无来源 | **已解决**。`tryRegister` 从 `g.getAllVariables().get("<subtask_index>")` 读取，取不到用 `?`，不改 `RedisSink`/构造函数签名 | §3.4、§6.3、§14.1-3 |
| 7 | NIT 包级构造函数重载改动已有构造函数结构 | **已解决，方式比评审建议更保守**：取消构造函数重载，构造函数只在 `if (metricGroup != null)` 块末尾追加 S0 一行；单测照 v2.2.9 `RedisSinkWriterConnectionClosedTest` 的模式，通过真实构造函数（连本地 Redis）创建 writer 后用反射替换非 final 字段 `redisCommandsContainer`。代价是 §14.1-4 需要本地 Redis，与现有测试的前提一致；真实构建路径由 §14.1-6 回归覆盖 | §12-1、§12-7、§14.1-4、§16-11、§17 |
