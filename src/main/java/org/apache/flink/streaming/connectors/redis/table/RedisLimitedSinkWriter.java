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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Limited sink writer for Flink online debugging. */
public class RedisLimitedSinkWriter extends RedisSinkWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLimitedSinkWriter.class);

    private final long maxOnline;
    private final long startTime;
    private final long sinkInterval;
    private final int maxNum;
    private volatile int curNum;

    public RedisLimitedSinkWriter(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<RowData> redisSinkMapper,
            List<DataType> columnDataTypes,
            ReadableConfig config,
            SinkWriterMetricGroup metricGroup) {
        super(flinkConfigBase, redisSinkMapper, columnDataTypes, config, metricGroup);

        this.maxOnline = config.get(RedisOptions.SINK_LIMIT_MAX_ONLINE);
        Preconditions.checkState(
                maxOnline > 0 && maxOnline <= RedisOptions.SINK_LIMIT_MAX_ONLINE.defaultValue(),
                "the max online milliseconds must be more than 0 and less than %s seconds.",
                RedisOptions.SINK_LIMIT_MAX_ONLINE.defaultValue());

        this.sinkInterval = config.get(RedisOptions.SINK_LIMIT_INTERVAL);
        Preconditions.checkState(
                sinkInterval >= RedisOptions.SINK_LIMIT_INTERVAL.defaultValue(),
                "the sink limit interval must be more than %s millisecond",
                RedisOptions.SINK_LIMIT_INTERVAL.defaultValue());

        this.maxNum = config.get(RedisOptions.SINK_LIMIT_MAX_NUM);
        Preconditions.checkState(
                maxNum > 0 && maxNum <= RedisOptions.SINK_LIMIT_MAX_NUM.defaultValue(),
                "the max num must be more than 0 and less than %s.",
                RedisOptions.SINK_LIMIT_MAX_NUM.defaultValue());

        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void write(RowData rowData, Context context) throws IOException, InterruptedException {
        long remainTime = maxOnline - (System.currentTimeMillis() - startTime);
        if (remainTime < 0) {
            throw new RuntimeException(
                    "thread id:"
                            + Thread.currentThread().getId()
                            + ", the debugging time has exceeded the max online time.");
        }

        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        // all keys must expire 10 seconds after online debugging end.
        super.ttl = (int) remainTime / 1000 + 10;
        super.write(rowData, context);

        TimeUnit.MILLISECONDS.sleep(sinkInterval);
        curNum++;
        if (curNum > maxNum) {
            throw new RuntimeException(
                    "thread id:"
                            + Thread.currentThread().getId()
                            + ", the number of debug results has exceeded the max num."
                            + curNum);
        }
    }
}
