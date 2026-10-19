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
