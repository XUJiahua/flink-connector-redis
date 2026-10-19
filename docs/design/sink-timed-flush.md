# Redis Sink 定时刷新

## 行为

每个 Writer 在缓冲从空变为非空时，按 Flink processing time 注册一次定时器。
截止时间为第一条记录入队时间加 `sink.batch.flush-interval`，后续记录不会延后截止时间。
达到 `sink.batch.size` 时提前发送；checkpoint / end-of-input 刷新仍发送并等待在途命令确认。
`flush-interval=0` 禁用定时刷新，仅保留满批和 checkpoint / end-of-input 刷新。

定时器只负责提交异步命令，Redis 确认时间另受网络、服务端负载影响。
任务线程繁忙、GC、QPS 限流和 in-flight 背压也可能延迟提交，interval 不是业务可见时间的硬上限。

## 并发和生命周期

生产路径由 `RedisSink.createWriter` 传入 Flink 的 `ProcessingTimeService`。
Flink 2.2.1 已通过 mailbox 执行 Writer 的 processing-time 回调，回调直接执行刷新，
不再创建后台线程，也不重复转发一次 mailbox。

满批、checkpoint、关闭都会取消并使当前定时器代号失效。即使取消前回调已排队，
也不能刷新下一批。关闭后到达的回调不访问 Redis 连接。
刷新仍使用既有的 QPS 限流、in-flight 上限、重试和异步命令跟踪。

异步失败通过 mailbox 主动通知任务，不依赖下一条业务输入。
checkpoint 等待已提交的 Redis future，保留现有每个 future 60 秒超时。
该改动不引入事务或 exactly-once；失败恢复仍可能重放非幂等命令。

## 验收和发布

先运行 Writer 回归测试：

```sh
mvn -Dtest=RedisSinkWriterLatencyMetricsTest test
```

新增 `redis_sink_oldest_buffer_age_ms`，实时显示尚未发出的当前批次的年龄，
不必等到出队才看到缓冲等待；缓冲为空时为 0。

旧的七参数 Writer 构造器为兼容直接调用方而保留并标记 deprecated，
没有 processing-time service 时仍仅在 write 中检查时间。
SQL / Table API 的普通和 limited 路径均使用带定时器的新构造器。

覆盖单条后静默、连续稀疏输入、满批后旧回调、checkpoint 和关闭、禁用定时器，以及异步失败。
测试使用可控时间和手动完成的 Redis future，不依赖外部 Redis。

发布先保持 `sink.batch.size=1` 验证新 jar 兼容性，再在小范围恢复：

```sql
'sink.batch.size' = '100',
'sink.batch.flush-interval' = '1000'
```

本地集成测试 `RedisSinkTimedFlushIntegrationTest` 使用真实 Flink 任务和测试 RESP 服务，
验证定时器、mailbox、Lettuce 网络路径确实连通；服务只确认收到命令，不模拟 Redis 持久化。

在真实 Flink + Redis 环境关闭 checkpoint，发一条业务记录后静默，核对 Redis 收到命令。
同时验收峰值吞吐、pending commands、命令耗时和业务实际读取延迟。
双机房写同一 key 的版本顺序、窗口计算等待、读副本延迟仍需单独验证。


## 本地真实 Redis 验收记录（2026-10-19）

### 环境和执行方式

被测生产代码为提交 `5e1bd59`。本次新增 `RedisSinkRealRedisTest` 和一键验收脚本，
生产实现未作修改。环境为 OpenJDK 11.0.32.1、Flink 2.2.1、Lettuce 6.2.1.RELEASE、
Docker 29.9.0、Redis 7.4.7（`redis:7-alpine`）。本次镜像 ID 为
`sha256:02f2cc4882f8bf87c79a220ac958f58c700bdec0dfb9b9ea61b62fb0e8f1bfcf`。

从仓库根目录执行：

```sh
./scripts/verify-redis-timed-flush.sh
```

本机 Maven 需要使用已有的本地网络代理，本次实际命令为：

```sh
./scripts/verify-redis-timed-flush.sh -s /tmp/redis-maven-settings.xml
```

`-s` 是本机网络设置，并非测试本身的必需配置；脚本允许透传 Maven 参数。
脚本启动两个独立 Redis 容器，关闭持久化，只向 `127.0.0.1` 的随机端口发布服务。
一个用于正常写入、错误返回和双作业实验；另一个专用于停止和重启 Redis 的断连实验。
脚本退出时删除自己创建的两个容器，不使用既有 Redis 实例。

脚本同时运行原有 57 项相关测试与新增真实 Redis 验收。完整输出写入
`target/redis-timed-flush-acceptance.log`，JUnit 报告位于 `target/surefire-reports/`。
这些是本地生成文件，不提交 Git。未设置 `redis.it.port` 时，新增真实 Redis 测试默认跳过，
普通构建无需 Docker。断连用例另需 `redis.it.outage.port` 和 `redis.it.outage.container`，
由脚本自动传入；只停止脚本为该用例创建的容器。

### 验证方法

除两个直接验证 Writer `flush(false)` 的场景外，其余用例运行真实 Flink 本地任务：
源发出指定记录后保持阻塞，不结束输入；Sink 与源分属不同任务，以免源阻塞 Sink mailbox。
禁用 checkpoint 和自动重启，通过独立 Lettuce 连接轮询真实 Redis 的 `HGET`。
普通写入场景还检查作业仍为 `RUNNING`，因此成功不能归因于输入结束或取消任务时的最终刷新。

以下普通写入耗时从源首次发出记录计到独立连接读到目标值，包含传输、Sink 刷新、Redis 执行
和约 10ms 轮询误差，不含任务启动时间，也不等同于业务 PostCapture → PreRefund 的端到端延迟。
错误场景从源首次发出记录计到观察到作业失败；checkpoint 两个场景单独测量 Writer `flush(false)`。
所有写入均为 `HSET`，使用本次测试专属 key。

1. **即时发送**：`batch.size=1 / interval=60000ms`，仅发送一条；应无需等待 60 秒。
2. **单条后静默**：`100 / 1000ms`，仅发送一条；应在约一秒后可读，而非等待下一条记录或 checkpoint。
3. **满批和单 Writer 顺序**：`100 / 60000ms`，连续发送 100 个同 key、同 field 的快照，
   最终值应为第 100 条 `v99`，无需等待 60 秒。
4. **真实 Redis 异步报错**：先将目标 key 写为 String，再执行 HSET，Redis 返回 `WRONGTYPE`。
   源保持静默，检查作业通过 mailbox 感知异步失败，并检查原 String 未被覆盖。
5. **确认等待**：直接调用生产 Writer 的 checkpoint 刷新接口。
   一条记录先留在缓冲，使用 `CLIENT PAUSE 600` 暂停 Redis，调用 `flush(false)`，
   检查它等待真实确认后才返回，随后独立读取新值。
6. **真实断连**：先确认一条写入成功，然后停止专用 Redis 容器，提交第二条并调用 `flush(false)`。
   检查刷新报错，随后在相同地址重启该容器再关闭 Writer，并通过独立连接确认
   `after-outage` 最终写入成功，避免把超时误认为写入取消。
7. **双作业覆盖实验**：两个独立 Flink 作业写同 key、同 field。
   旧作业为 `100 / 5000ms`，新作业为 `1 / 60000ms`。
   特意拉长旧作业 interval，让先读到 `new0`、随后被 `old0` 覆盖的顺序可复现。
   这是跨作业顺序风险的确认，不能解释为该风险已修复。

### 实测结果

最终执行时间：2026-10-19 15:17:58～15:19:35（Asia/Shanghai）。
本次正常 Redis 端口为 `32783`，断连 Redis 端口为 `32784`；下次运行会重新随机分配。
Maven 结果为 `BUILD SUCCESS`，脚本退出码为 `0`，相关测试 **64 项通过，失败 0、异常 0、跳过 0**。
编译阶段的 Spotless 检查、脚本 `bash -n` 和 `git diff --check` 也通过。
脚本创建的两个容器已删除；没有修改本机原有的 Redis 服务。

| 场景 | 配置与操作 | 本次观察 | 判定 |
| --- | --- | --- | --- |
| 即时发送 | `1 / 60000ms`，1 条后静默 | 104ms 后读到 `v0`，作业仍运行 | 通过 |
| 单条定时刷新 | `100 / 1000ms`，1 条后静默 | 1096ms 后读到 `v0`，作业仍运行 | 通过 |
| 满批及同 Writer 顺序 | `100 / 60000ms`，100 条同 key 更新 | 126ms 后读到 `v99` | 通过 |
| 异步错误主动通知 | String key 上执行 HSET，源静默 | 1132ms 内作业因 `Async Redis write failed` / `WRONGTYPE` 失败；原值保留 | 通过 |
| checkpoint 接口等待确认 | 缓冲 1 条，Redis 暂停 600ms | `flush(false)` 等待 637ms 返回，随后可读 | 通过 |
| 真实断连 | 停止专用 Redis，提交写入并刷新 | 60000ms 后抛 `IOException`，根因为 `TimeoutException` | 通过 |
| 断连后恢复 | 相同地址重启 Redis，关闭 Writer 后独立读值 | 读到 `after-outage` | 通过 |
| 双独立作业写同 key | 旧作业 `100 / 5000ms`，新作业 `1 / 60000ms` | 新作业发出后 102ms 可读 `new0`；旧作业发出后 5104ms 值变为 `old0` | 复现跨作业覆盖风险 |

日志中的关键原始输出如下：

```text
REAL_REDIS idle-async-error elapsed_ms=1132 result=job failed with WRONGTYPE
REAL_REDIS batch-one elapsed_ms=104 result=v0
REAL_REDIS disconnected-checkpoint-failure elapsed_ms=60000 result=IOException/TimeoutException
REAL_REDIS disconnected-recovered result=after-outage
REAL_REDIS two-jobs-new-visible elapsed_ms=102 result=new0
REAL_REDIS two-jobs-old-overwrite elapsed_ms=5104 result=old0
REAL_REDIS checkpoint-flush-ack elapsed_ms=637 result=v
REAL_REDIS full-batch elapsed_ms=126 result=v99
REAL_REDIS single-idle elapsed_ms=1096 result=v0
Tests run: 64, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

试跑时修正了两处验收设施问题：错误作业会自动关闭 MiniCluster，清理时不应再次取消；
Docker 对随机映射端口的容器执行 stop/start 可能改变端口，因此脚本为断连容器选出空闲端口后，
使用显式端口绑定重新创建，确保重连地址保持一致。上表来自修正后完整重跑的输出。

断连实测还有两个含义：一秒刷新不等于一秒故障判定；checkpoint 超时也不等于写入被取消。
本次 `flush(false)` 超时后，恢复连接仍执行了待确认的写入。调用方不能用该超时推断 Redis
一定未发生更新；非幂等命令的重放和重复执行仍受既有至少一次语义约束。

### 结论边界和后续验收

- 单条静默和满批测试验证 HSET 的提交与实际读取；没有覆盖 Kafka、CUMULATE/LAG 窗口 SQL、
  PostCapture/PreRefund 业务调用、读副本和客户端缓存。
- 100 条同 key 更新是顺序验证，不是生产峰值压力测试；不能据此确定吞吐、CPU、网络或 Redis 容量。
- `CLIENT PAUSE` 和断连用例验证生产 Writer 的 `flush(false)`，并非完整分布式 checkpoint
  barrier 到达、对齐和状态恢复流程；任务重启、重放及 savepoint 升级仍需环境验收。
- WRONGTYPE 用例验证“异步命令明确返回失败时，无后续输入也会通知 Flink”；
  断连用例验证“checkpoint 刷新无法获得确认时会报错”。它们不证明禁用 checkpoint、
  Redis 长期不可达时，静默作业一定会在固定时间内主动失败。
- 双作业实验确认跨作业 HSET 没有版本保护：较旧累计快照仍可覆盖较新快照。
  定时刷新修复只缩短本 Writer 的缓冲等待；跨机房写入归属或版本校验需要另外处理。
