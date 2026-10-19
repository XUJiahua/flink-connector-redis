/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.config.LettuceConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the real Flink processing-time / mailbox path with no checkpoints or end of input. */
class RedisSinkTimedFlushIntegrationTest {

    private static final CountDownLatch SOURCE_GATE = new CountDownLatch(1);

    @Test
    @SuppressWarnings("unchecked")
    void idleSinkFlushesThroughFlinkTimer() throws Exception {
        try (FakeRespServer server = new FakeRespServer()) {
            Configuration config = new Configuration();
            config.set(RedisOptions.SINK_BATCH_SIZE, 100);
            config.set(RedisOptions.SINK_BATCH_FLUSH_INTERVAL, 100L);
            FlinkSingleConfig connection = new FlinkSingleConfig.Builder()
                    .setHost("127.0.0.1").setPort(server.port()).setTimeout(2000)
                    .setDatabase(0).setLettuceConfig(new LettuceConfig(null, null)).build();
            RedisSinkMapper<RowData> mapper =
                    (RedisSinkMapper<RowData>) (RedisSinkMapper<?>) new RowRedisSinkMapper(RedisCommand.HSET, config);
            RedisSink sink = new RedisSink(connection, mapper, ResolvedSchema.of(
                    Column.physical("k", DataTypes.STRING()),
                    Column.physical("f", DataTypes.STRING()),
                    Column.physical("v", DataTypes.STRING())), config, false);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // The source emits one row, then stays open. A separate sink task is essential:
            // blocking a chained source would also block the sink's mailbox and its timers.
            env.fromData("first", "hold")
                    .map(value -> {
                        if (value.equals("hold")) {
                            SOURCE_GATE.await(30, TimeUnit.SECONDS);
                        }
                        return (RowData) GenericRowData.of(StringData.fromString("k"),
                                StringData.fromString("f"), StringData.fromString("v"));
                    })
                    .returns(TypeInformation.of(RowData.class))
                    .sinkTo(sink).disableChaining();
            JobClient job = env.executeAsync("idle-redis-sink-timer");
            try {
                assertTrue(server.awaitHset(20), "single buffered row never reached Redis while idle");
            } finally {
                job.cancel().get(20, TimeUnit.SECONDS);
                SOURCE_GATE.countDown();
            }
        }
    }
}
