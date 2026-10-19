[EN](README-en.md)
# 1 项目介绍

基于[bahir-flink](https://github.com/apache/bahir-flink.git)二次开发，相对bahir调整的内容有：
```
1.使用Lettuce替换Jedis,同步读写改为异步读写，大幅度提升了性能 
2.增加了Table/SQL API，增加select/维表join查询支持
3.增加关联查询缓存(支持增量与全量)
4.增加支持整行保存功能，用于多字段的维表关联查询
5.增加限流功能，用于Flink SQL在线调试功能
6.支持Flink 2.x（使用Sink V2 API）
7.统一过期策略等
8.支持flink cdc删除及其它RowKind.DELETE
9.支持select查询
10.支持mini-batch批量写入与异步背压控制
```

因bahir使用的flink接口版本较老，所以改动较大，开发过程中参考了腾讯云与阿里云两家产商的流计算产品，取两家之长，并增加了更丰富的功能。

注：redis不支持两段提交无法实现刚好一次语义。



# 2 使用方法: 
## 2.1 工程直接引用
项目依赖Lettuce(6.2.1)及netty-transport-native-epoll(4.1.82.Final),如flink环境有这两个包,则使用flink-connector-redis-2.2.2.jar，
否则使用flink-connector-redis-2.2.2-jar-with-dependencies.jar。
<br/>
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <!-- 没有单独引入项目依赖Lettuce netty-transport-native-epoll依赖时 -->
    <!--            <classifier>jar-with-dependencies</classifier>-->
    <version>2.2.2</version>
</dependency>
```
## 2.2 自行打包
打包命令： mvn package,将生成的包放入flink lib中即可，无需其它设置。

## 2.3 使用示例 
```
-- 创建redis表示例
create table redis_table (name varchar, age int) 
  with ('connector'='redis', 'host'='10.11.69.176', 'port'='6379','password'='test123', 
  'redis-mode'='single','command'='set');
-- 写入  
  insert into redis_table select * from (values('test', 1));

-- 查询  
  insert into redis_table select name,age + 1 from redis_table /*+ options('scan.key'='test') */
  
create table gen_table (age int , level int, proctime as procTime()) with ('connector'='datagen','fields.age.kind' = 'sequence',
 'fields.age.start' = '2','fields.age.end' = '2','fields.level.kind' = 'sequence','fields.level.start' = '10','fields.level.end' = '10'); 

-- 关联查询 
insert into redis_table select 'test', j.age + 10 from gen_table s left join redis_table  for system_time as of proctime as j
on j.name = 'test'

```


# 3 参数说明：
## 3.1 主要参数：

| 字段                    | 默认值    | 类型      | 说明                                                                                               |
|-----------------------|--------|---------|--------------------------------------------------------------------------------------------------|
| connector             | (none) | String  | `redis`                                                                                          |
| host                  | (none) | String  | Redis IP                                                                                         |
| port                  | 6379   | Integer | Redis 端口                                                                                         |
| password              | null   | String  | 如果没有设置，则为 null                                                                                   |
| database              | 0      | Integer | 默认使用 db0                                                                                         |
| timeout               | 2000   | Integer | 连接超时时间，单位 ms，默认 1s                                                                               |
| cluster-nodes         | (none) | String  | 集群ip与端口，当redis-mode为cluster时不为空，如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000          |
| command               | (none) | String  | 对应上文中的redis命令                                                                                    |
| redis-mode            | (none) | Integer | mode类型： single cluster sentinel                                                                  |
| lookup.cache.max-rows | -1     | Integer | 查询缓存大小,减少对redis重复key的查询                                                                          |
| lookup.cache.ttl      | -1     | Integer | 查询缓存过期时间，单位为秒， 开启查询缓存条件是max-rows与ttl都不能为-1                                                       |
| lookup.cache.load-all | false  | Boolean | 开启全量缓存,当命令为hget时,将从redis map查询出所有元素并保存到cache中,用于解决缓存穿透问题                                         |
| max.retries           | 1      | Integer | 写入/查询失败重试次数                                                                                      |
| value.data.structure  | column | String  | column: value值来自某一字段 (如, set: key值取自DDL定义的第一个字段, value值取自第二个字段)<br/> row: 将整行内容保存至value并以'\01'分割 |
| set.if.absent         | false  | Boolean | 在key不存在时才写入,只对set hset有效                                                                         |
| io.pool.size          | (none) | Integer | Lettuce内netty的io线程池大小,默认情况下该值为当前JVM可用线程数，并且大于2                                                   |
| event.pool.size       | (none) | Integer | Lettuce内netty的event线程池大小 ,默认情况下该值为当前JVM可用线程数，并且大于2                                               |
| scan.key              | (none) | String  | 查询时redis key                                                                                     |
| scan.addition.key     | (none) | String  | 查询时限定redis key,如map结构时的hashfield                                                                 |
| scan.range.start      | (none) | Integer | 查询list结构时指定lrange start                                                                          |
| scan.range.stop       | (none) | Integer | 查询list结构时指定lrange start                                                                          |
| scan.count            | (none) | Integer | 查询set结构时指定srandmember count                                                                      |
| zset.zremrangeby      | (none) | String  | 执行zadd之后，是否执行zremrangeby，取值：SCORE、LEX、RANK                                                       |
| audit.log             | false  | Boolean | 打开sink日志                                                                                         |

### 3.1.1 command值与redis命令对应关系：

| command值              | 写入                    | 查询             | 维表关联    | 删除(Flink CDC等产生的RowKind.delete)  |
|-----------------------|-----------------------|----------------|---------|----------------------------------|
| set                   | set                   | get            | get     | del                              |
| hset                  | hset                  | hget           | hget    | hdel                             |
| get                   | set                   | get            | get     | del                              |
| hset                  | hset                  | hget           | hget    | hdel                             | 
| rpush                 | rpush                 | lrange         |         |                                  |
| lpush                 | lpush                 | lrange         |         |                                  |
| incrBy incrByFloat    | incrBy incrByFloat    | get            | get     | 写入相对值，如:incrby 2 -> incryby -2   | 
| hincrBy hincryByFloat | hincrBy hincryByFloat | hget           | hget    | 写入相对值，如:hincrby 2 -> hincryby -2 |
| zincrby               | zincrby               | zscore         | zscore  | 写入相对值，如:zincrby 2 -> zincryby -2 |
| sadd                  | sadd                  | srandmember 10 |         | srem                             |   
| zadd                  | zadd                  | zscore         | zscore  | zrem                             |   
| pfadd(hyperloglog)    | pfadd(hyperloglog)    |                |         |                                  |   
| publish               | publish               |                |         |                                  |
| zrem                  | zrem                  | zscore         | zscore  |                                  |
| srem                  | srem                  | srandmember 10 |         |                                  |
| del                   | del                   | get            | get     |                                  |
| hdel                  | hdel                  | hget           | hget    |                                  |
| decrBy                | decrBy                | get            | get     |                                  | 

注：**为空表示不支持**

### 3.1.2 value.data.structure = column(默认)
无需通过primary key来映射redis中的Key，直接由ddl中的字段顺序来决定Key,如：

```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, score为value.
```
### 3.1.3 value.data.structure = row
整行内容保存至value并以'\01'分割
```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, username\01passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, name\01subject\01score为value.
```

## 3.2 sink时ttl相关参数

| Field              | Default | Type    | Description                                                       |
|--------------------|---------|---------|-------------------------------------------------------------------|
| ttl                | (none)  | Integer | key过期时间(秒),每次sink时会设置ttl                                          |
| ttl.on.time        | (none)  | String  | key的过期时间点,格式为LocalTime.toString(), eg: 10:00 12:12:01,当ttl未配置时才生效 |
| ttl.key.not.absent | false   | boolean | 与ttl一起使用,当key不存在时才设置ttl                                           |


## 3.3 sink批量写入与背压参数:

| Field                      | Default | Type    | Description                                              |
|----------------------------|---------|---------|----------------------------------------------------------|
| sink.batch.size            | 100     | Integer | 缓冲多少条记录后批量写入Redis，设为1则禁用批量（逐条写入）                        |
| sink.batch.flush-interval  | 1000    | Long    | 从首条记录开始计时的定时刷新间隔(毫秒)，静默时也刷新；0 禁用定时刷新                          |
| sink.max-in-flight-requests| 1000    | Integer | 最大异步在途请求数，用于背压控制，Redis响应慢时自动限速                            |
| sink.write.qps             | 0       | Long    | 整个sink（所有并行子任务合计）的最大写入QPS（每秒发往Redis的命令数）。运行时按并行度均分到各子任务（每子任务QPS=总QPS/并行度）。<=0 表示不限速 |
| sink.write.qps.burst-seconds| 1.0    | Double  | 令牌桶允许累积的突发容量(秒)。值越大越能容忍批量突发，值越小限速越平滑。仅当 sink.write.qps>0 时生效 |

### 3.3.1 写 QPS 限速与 checkpoint 配合（重要）

`sink.write.qps` 用令牌桶限制每秒发往 Redis 的命令数，用于在高峰期保护 Redis（例如把写 QPS 从 20w 压到 5w）。使用时请注意以下几点：

1. **限速是全局预算、按并行度均分。** 配 `sink.write.qps=50000`、sink 并行度为 10 时，每个子任务限到 5000 QPS。修改 sink 并行度会改变单子任务速率，但总量不变。

2. **限速会产生反压，这是预期行为。** 被限速的 sink 是作业的人为瓶颈，Flink UI 中该 sink 的 `busy` 会接近 100%，上游（含 Source）会被反压而呈现"读一批—停—再读一批"的脉冲式消费。这说明限速生效了，不是故障。

3. **等待令牌时不会冻结 task 线程。** 限速器与背压（`sink.max-in-flight-requests`）的等待都改成了协作式：等待期间会让出 Flink 的 mailbox 线程去处理其他邮件，并以毫秒级切片轮询，因此不会像 `Thread.sleep` 那样把主线程整段占死。

4. **务必开启非对齐 checkpoint。** 限速带来的反压会让 checkpoint barrier 被网络缓冲区里堆积的数据挡住，导致对齐式 checkpoint 长时间无法完成。开启非对齐 checkpoint 可让 barrier 越过堆积数据：

   ```
   execution.checkpointing.unaligned.enabled: true
   execution.checkpointing.aligned-checkpoint-timeout: 10s
   ```

   配合后，在限速产生持续反压的情况下 checkpoint 仍可稳定完成。


## 3.4 在线调试SQL时，用于限制sink资源使用的参数:

| Field                 | Default | Type    | Description                             |
|-----------------------|---------|---------|-----------------------------------------|
| sink.limit            | false   | Boolean | 是否打开限制                                  |
| sink.limit.max-num    | 10000   | Integer | taskmanager内每个slot可以写的最大数据量             |
| sink.limit.interval   | 100     | String  | taskmanager内每个slot写入数据间隔   milliseconds |
| sink.limit.max-online | 30 * 60 * 1000L   | Long    | taskmanager内每个slot最大在线时间, milliseconds  |


## 3.5 集群类型为sentinel时额外连接参数:

| 字段                 | 默认值 | 类型   | 说明                                                      |
|--------------------| ------ | ------ |---------------------------------------------------------|
| master.name        | (none) | String | 主名                                                      |
| sentinels.info     | (none) | String | 如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000 |
| sentinels.password | (none) | String | sentinel进程密码                                            |

## 3.6 Sink 分段延迟指标

Sink V2 writer（SQL 路径）会把一条记录的端到端延迟拆成三段，并给出两个积压 gauge，用于定位“写到 Redis 慢”到底慢在哪一层。只读观测，不改变任何写入逻辑，不需要任何 WITH 选项。指标直接注册在 sink 的 operator metric group 上，代码中的注册名不带 `flink_` 前缀；Prometheus reporter 导出时会加上 `flink_taskmanager_job_task_operator_` 前缀。

| 代码内注册名 | Prometheus 导出名 | 类型 | 含义 |
|---|---|---|---|
| `redis_sink_invoke_delay_ms` / `_max_ms` / `_last_ms` | `flink_taskmanager_job_task_operator_redis_sink_invoke_delay_ms{quantile=...}`、`..._ms_count`、`..._max_ms`、`..._last_ms` | summary + gauge | 进入 `write()` 的时刻 − 窗口时间基准（Kafka → Flink → 计算，含等 watermark） |
| `redis_sink_buffer_wait_ms` / `_max_ms` / `_last_ms` | `flink_taskmanager_job_task_operator_redis_sink_buffer_wait_ms...` | summary + gauge | 首次把命令提交给 Lettuce 的时刻 − 进入 `write()` 的时刻（sink 内部等待） |
| `redis_sink_cmd_latency_ms` / `_max_ms` / `_last_ms` | `flink_taskmanager_job_task_operator_redis_sink_cmd_latency_ms...` | summary + gauge | 主写命令（如 HSET）ack 时刻 − 提交时刻（网络 + Redis），TTL 的 EXPIRE 不计入 |
| `redis_sink_ack_freshness_ms` / `_max_ms` / `_last_ms` | `flink_taskmanager_job_task_operator_redis_sink_ack_freshness_ms...` | summary + gauge | 主写命令 ack 时刻 − 窗口时间基准（端到端，约等于上面三段之和） |
| `redis_sink_buffer_size` | `flink_taskmanager_job_task_operator_redis_sink_buffer_size` | gauge | 缓冲区中还没提交给 Lettuce 的记录数 |
| `redis_sink_pending_commands` | `flink_taskmanager_job_task_operator_redis_sink_pending_commands` | gauge | 已提交、尚未完成的主写命令数 |
| `redis_sink_event_time_source` | `flink_taskmanager_job_task_operator_redis_sink_event_time_source` | gauge | 最近一条记录的窗口时间基准口径：0 = 无基准（跳过），1 = record timestamp（精确），2 = watermark 回退（上界） |

每个延迟指标有三种形态：
- `<x>_ms`：histogram（最近 1024 个样本），导出为 summary 的 quantile 和 `_count`。低流量时分位数会长期停在旧样本上。
- `<x>_max_ms`：最近 60 秒滑动窗口内的最大值，读取不清零，窗口内没有样本时为 0，可以跨 subtask 用 `max by` 聚合。
- `<x>_last_ms`：最近一个样本的值。

单位都是毫秒，负值钳成 0，亚毫秒的 Redis 往返显示为 0。label 与现有 `numRecordsSend` 相同（operator scope 变量），不新增任何 label。

> `invoke_delay_ms` 与 `ack_freshness_ms` 的“窗口时间”优先取记录自带的时间戳（`event_time_source=1`，精确）。拿不到时（例如 OVER 之后接了 WHERE/投影的 SQL，这是最常见的情况）退回 `当前 watermark + 1`（`event_time_source=2`），此时这两个值是**上界**：偏大量小于一次 watermark 跳变。低流量时 watermark 一次可能跳十几秒，所以这两个值可能被放大十几秒。判断慢在哪一段时，请以 `buffer_wait_ms` 和 `cmd_latency_ms` 为准，它们是精确值。`event_time_source=0` 表示没有可用基准，这两个指标不产生样本（`_count` 不增长）。

> `buffer_wait_ms` 是“进入 sink 到命令真正发给 Redis”的全部等待，包括：等 flush 触发（下一条记录 / batch 满 / checkpoint）、`sink.write.qps` 限速、等在途许可（`sink.max-in-flight-requests`）。所以 `buffer_wait_ms` 高时要看 `pending_commands`：接近 `sink.max-in-flight-requests`，说明 Redis/网络回不过来，根因在 Redis 一侧；远低于上限，才是记录在 sink 缓冲里等 flush。

窗口时间与 TM 的 `System.currentTimeMillis()` 相减，前提是 rowtime（如 `TIMESTAMP(3)` 的 `creationTime`）按 UTC 墙钟写入；生产方与 TM 的时钟偏差也会叠加进 `invoke_delay`/`ack_freshness`。

推荐 PromQL（`dc` 换成 Prometheus 侧实际区分 A/B 的 label，没有就去掉 `by (dc)`）：

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

# 在途许可是否耗尽（单 subtask；1000 为 sink.max-in-flight-requests 默认值）
max by (dc, operator_name) (flink_taskmanager_job_task_operator_redis_sink_pending_commands{job_name="$job"}) >= 0.9 * 1000

# 口径识别：2 = watermark 回退（上界），1 = 精确，0 = 无基准
max by (dc, operator_name) (flink_taskmanager_job_task_operator_redis_sink_event_time_source{job_name="$job"})

# 样本是否还在产生；基准缺失时 invoke_delay 的速率为 0 而 buffer_wait 的速率 > 0
sum by (dc) (rate(flink_taskmanager_job_task_operator_redis_sink_ack_freshness_ms_count{job_name="$job"}[1m]))
sum by (dc, operator_name) (rate(flink_taskmanager_job_task_operator_redis_sink_invoke_delay_ms_count{job_name="$job"}[5m]))
sum by (dc, operator_name) (rate(flink_taskmanager_job_task_operator_redis_sink_buffer_wait_ms_count{job_name="$job"}[5m]))
```

解读方法（按顺序判断）：
1. 先看 `event_time_source`：等于 2 时，`invoke_delay`/`ack_freshness` 是上界，可能被低流量下的 watermark 跳变放大到十几秒；等于 0 时这两个指标没有样本，只能用后两段判断。
2. `invoke_delay` 高，后两段低：慢在 Kafka → Flink → 计算（含等水位线），或者是回退基准的高估，都不在 sink 里。
3. `buffer_wait` 高，且同一 subtask 的 `pending_commands` 接近 `sink.max-in-flight-requests`（≥ 0.9×上限）：在途许可耗尽，根因在网络/Redis（此时 `cmd_latency` 通常也高；若 `cmd_latency` 无样本而 `pending` 不降，就是命令卡住）。
4. `buffer_wait` 高，且 `pending_commands` 远低于上限、`buffer_size>0`：记录在 sink 缓冲里等 flush 触发（低流量算子要等下一条记录或 checkpoint）。
5. `cmd_latency` 高，或 `pending_commands` 持续 >0 且不下降：慢在网络或 Redis。

注意：summary 的 quantile 跨 subtask 做 `max by` 得到的是“最差 subtask 的 p50”，不是全局 p50；跨 subtask 聚合请看 `_max_ms`。`pending_commands` 判据要按单个 subtask 看（每个 writer 一个许可池）。

序列数：每个 writer 39 个（4 个 summary × 7 + 4 × 2 个 gauge + 3 个 gauge）。例如 13 个 sink 算子 × 并行度 4 = 52 个 writer，约 2,028 个序列。

另外：`set.if.absent=true` 时测到的是 EXISTS/HEXISTS 的往返；ZADD 配 `zset.zremrangeby` 时只测 ZADD；旧版 `RedisSinkFunction`（DataStream）不带这些指标。

# 4 数据类型转换

| flink type   | redis row converter                                          |
| ------------ | ------------------------------------------------------------ |
| CHAR         | String                                                       |
| VARCHAR      | String                                                       |
| String       | String                                                       |
| BOOLEAN      | String String.valueOf(boolean val) <br/>boolean Boolean.valueOf(String str) |
| BINARY       | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str)             |
| VARBINARY    | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str)                 |
| DECIMAL      | String  BigDecimal.toString <br/>DecimalData DecimalData.fromBigDecimal(new BigDecimal(String str),int precision, int scale)                               |
| TINYINT      | String String.valueOf(byte val)  <br/>byte Byte.valueOf(String str)                         |
| SMALLINT     | String String.valueOf(short val) <br/>short Short.valueOf(String str)                          |
| INTEGER      | String String.valueOf(int val)  <br/>int Integer.valueOf(String str)                          |
| DATE         | String the day from epoch as int <br/>date show as 2022-01-01                             |
| TIME         | String the millisecond from 0'clock as int <br/>time show as 04:04:01.023                         |
| BIGINT       | String String.valueOf(long val) <br/>long Long.valueOf(String str)                            |
| FLOAT        | String String.valueOf(float val) <br/>float Float.valueOf(String str)                            |
| DOUBLE       | String String.valueOf(double val) <br/>double Double.valueOf(String str)                           |
| TIMESTAMP | String the millisecond from epoch as long <br/>timestamp TimeStampData.fromEpochMillis(Long.valueOf(String str))                    |


# 5 使用示例:
- ## 5.1 维表查询：
```
create table sink_redis(name varchar, level varchar, age varchar) with ( 'connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='******','command'='hset');

-- 先在redis中插入数据,相当于redis命令： hset 3 3 100 --
insert into sink_redis select * from (values ('3', '3', '100'));
                
create table dim_table (name varchar, level varchar, age varchar) with ('connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single', 'password'='*****','command'='hget', 'maxIdle'='2', 'minIdle'='1', 'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'max-retries'='3');
    
-- 随机生成10以内的数据作为数据源 --
-- 其中有一条数据会是： username = 3  level = 3, 会跟上面插入的数据关联 -- 
create table source_table (username varchar, level varchar, proctime as procTime()) with ('connector'='datagen',  'rows-per-second'='1',  'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='10', 'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='10');

create table sink_table(username varchar, level varchar,age varchar) with ('connector'='print');

insert into
	sink_table
select
	s.username,
	s.level,
	d.age
from
	source_table s
left join dim_table for system_time as of s.proctime as d on
	d.name = s.username
	and d.level = s.level;
-- username为3那一行会关联到redis内的值，输出为： 3,3,100	
```

- ## 5.2 多字段的维表关联查询
很多情况维表有多个字段,本实例展示如何利用'value.data.structure'='row'写多字段并关联查询。
```
-- 创建表
create table sink_redis(uid VARCHAR,score double,score2 double )
with ( 'connector' = 'redis',
			'host' = '10.11.69.176',
			'port' = '6379',
			'redis-mode' = 'single',
			'password' = '****',
			'command' = 'SET',
			'value.data.structure' = 'row');  -- 'value.data.structure'='row':整行内容保存至value并以'\01'分割
-- 写入测试数据，score、score2为需要被关联查询出的两个维度
insert into sink_redis select * from (values ('1', 10.3, 10.1));

-- 在redis中，value的值为: "1\x0110.3\x0110.1" --
-- 写入结束 --

-- create join table --
create table join_table with ('command'='get', 'value.data.structure'='row') like sink_redis

-- create result table --
create table result_table(uid VARCHAR, username VARCHAR, score double, score2 double) with ('connector'='print')

-- create source table --
create table source_table(uid VARCHAR, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')

-- 关联查询维表，获得维表的多个字段值 --
insert
	into
	result_table
select
	s.uid,
	s.username,
	j.score, -- 来自维表
	j.score2 -- 来自维表
from
	source_table as s
join join_table for system_time as of s.proc_time as j on
	j.uid = s.uid
	
result:
2> +I[2, 1e0fe885a2990edd7f13dd0b81f923713182d5c559b21eff6bda3960cba8df27c69a3c0f26466efaface8976a2e16d9f68b3, null, null]
1> +I[1, 30182e00eca2bff6e00a2d5331e8857a087792918c4379155b635a3cf42a53a1b8f3be7feb00b0c63c556641423be5537476, 10.3, 10.1]
```

- ## 5.3 DataStream查询方式<br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.datastream.DataStreamTest.java<br>
  hset示例，相当于redis命令：*hset tom math 150*

```
        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_SINGLE);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());
        configuration.setInteger(TTL, 10);

        RedisSinkMapper redisMapper = new RowRedisSinkMapper(RedisCommand.HSET, configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BinaryRowData binaryRowData = new BinaryRowData(3);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeString(0, StringData.fromString("tom"));
        binaryRowWriter.writeString(1, StringData.fromString("math"));
        binaryRowWriter.writeString(2, StringData.fromString("152"));

        DataStream<BinaryRowData> dataStream = env.fromElements(binaryRowData, binaryRowData);

        List<String> columnNames = Arrays.asList("name", "subject", "scope");
        List<DataType> columnDataTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        ResolvedSchema resolvedSchema = ResolvedSchema.physical(columnNames, columnDataTypes);

        FlinkConfigBase conf =
                new FlinkSingleConfig.Builder()
                        .setHost(REDIS_HOST)
                        .setPort(REDIS_PORT)
                        .setPassword(REDIS_PASSWORD)
                        .build();

        RedisSinkFunction redisSinkFunction =
                new RedisSinkFunction<>(conf, redisMapper, resolvedSchema, configuration);

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");
```



- ## 5.4 redis-cluster写入示例 <br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.table.SQLInsertTest.java<br>
  set示例，相当于redis命令： *set test test11*

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

String ddl = "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', " +
              "'cluster-nodes'='10.11.80.147:7000,10.11.80.147:7001','redis- mode'='cluster','password'='******','command'='set')" ;

tEnv.executeSql(ddl);
String sql = " insert into sink_redis select * from (values ('test', 'test11'))";
TableResult tableResult = tEnv.executeSql(sql);
tableResult.getJobClient().get()
.getJobExecutionResult()
.get();
```

# 6 解决问题联系我

![img.png](img.png)


# 7 开发环境

ide: IntelliJ IDEA 

code format: google-java-format + Save Actions

flink 2.2.x

jdk11+ Lettuce 6.2.1

# 8 贡献
Pull Request需要提交至dev分支</br>
提交前请使用mvn spotless:apply进行代码格式化,然后使用maven package打包确认所有测试用例能通过。

# 9 历史版本
## Flink 1.x 支持
Flink 1.14/1.15 请使用 1.4.3 版本:
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.4.3</version>
</dependency>
```

## Flink 1.12 支持
请切换到分支flink-1.12(注：1.12使用jedis)
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.1.1-1.12</version>
</dependency>
```