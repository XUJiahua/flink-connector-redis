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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.List;

/** Redis Sink V2 implementation. */
public class RedisSink implements Sink<RowData> {

    private static final long serialVersionUID = 1L;

    private final FlinkConfigBase flinkConfigBase;
    private final RedisSinkMapper<RowData> redisSinkMapper;
    private final List<DataType> columnDataTypes;
    private final ReadableConfig readableConfig;
    private final boolean limited;

    public RedisSink(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<RowData> redisSinkMapper,
            ResolvedSchema resolvedSchema,
            ReadableConfig readableConfig,
            boolean limited) {
        this.flinkConfigBase = flinkConfigBase;
        this.redisSinkMapper = redisSinkMapper;
        this.columnDataTypes = resolvedSchema.getColumnDataTypes();
        this.readableConfig = readableConfig;
        this.limited = limited;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        int numParallelSubtasks = context.getTaskInfo().getNumberOfParallelSubtasks();
        if (limited) {
            return new RedisLimitedSinkWriter(
                    flinkConfigBase, redisSinkMapper, columnDataTypes, readableConfig,
                    context.metricGroup(), numParallelSubtasks);
        }
        return new RedisSinkWriter(
                flinkConfigBase, redisSinkMapper, columnDataTypes, readableConfig,
                context.metricGroup(), numParallelSubtasks);
    }
}
