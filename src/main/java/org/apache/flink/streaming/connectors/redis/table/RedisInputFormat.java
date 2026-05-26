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

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisSelectCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Redis InputFormat for scan source. */
public class RedisInputFormat extends RichInputFormat<RowData, InputSplit> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisInputFormat.class);

    private final ReadableConfig readableConfig;
    private final FlinkConfigBase flinkConfigBase;
    private final int maxRetryTimes;
    private final RedisCommand redisCommand;
    private final RedisValueDataStructure redisValueDataStructure;
    private final List<DataType> dataTypes;

    private transient RedisCommandsContainer redisCommandsContainer;
    private transient List<RowData> results;
    private transient int currentIndex;
    private transient boolean fetched;

    public RedisInputFormat(
            RedisMapper redisMapper,
            ReadableConfig readableConfig,
            FlinkConfigBase flinkConfigBase,
            ResolvedSchema resolvedSchema) {
        this.readableConfig = readableConfig;
        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.dataTypes = resolvedSchema.getColumnDataTypes();
    }

    @Override
    public void configure(Configuration parameters) {
        // no-op
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void openInputFormat() throws IOException {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container.");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new IOException(e);
        }
    }

    @Override
    public void open(InputSplit split) throws IOException {
        this.results = new ArrayList<>();
        this.currentIndex = 0;
        this.fetched = false;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (!fetched) {
            fetchData();
            fetched = true;
        }
        return currentIndex >= results.size();
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        return results.get(currentIndex++);
    }

    @Override
    public void close() throws IOException {
        // per-split close
    }

    @Override
    public void closeInputFormat() throws IOException {
        if (redisCommandsContainer != null) {
            try {
                redisCommandsContainer.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private void fetchData() throws IOException {
        String[] queryParameter = new String[2];
        queryParameter[0] = this.readableConfig.get(RedisOptions.SCAN_KEY);

        if (redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        }

        Preconditions.checkNotNull(
                queryParameter[0],
                "the %s for source can not be null",
                RedisOptions.SCAN_KEY.key());

        Preconditions.checkArgument(
                redisCommand.getSelectCommand() != RedisSelectCommand.NONE,
                String.format("the command %s do not support query.", redisCommand.name()));

        if (redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            Preconditions.checkNotNull(
                    queryParameter[1],
                    "must set field value of Map to %s",
                    RedisOptions.SCAN_ADDITION_KEY.key());
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            Preconditions.checkNotNull(
                    queryParameter[1],
                    "must set member value of SortedSet to %s",
                    RedisOptions.SCAN_ADDITION_KEY.key());
            Preconditions.checkArgument(
                    dataTypes.get(1).getLogicalType() instanceof DoubleType,
                    "the second column's type of source table must be double.");
        }

        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(queryParameter);
                break;
            } catch (Exception e) {
                LOG.error("query redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new IOException("query redis error", e);
                }
                try {
                    Thread.sleep(500 * i);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException(ie);
                }
            }
        }
    }

    private void query(String[] queryParameter) throws Exception {
        switch (redisCommand.getSelectCommand()) {
            case GET: {
                String result = this.redisCommandsContainer.get(queryParameter[0]).get();
                GenericRowData rowData =
                        RedisResultWrapper.createRowDataForString(
                                queryParameter, result, redisValueDataStructure, dataTypes);
                results.add(rowData);
                break;
            }
            case HGET: {
                String result =
                        this.redisCommandsContainer
                                .hget(queryParameter[0], queryParameter[1])
                                .get();
                GenericRowData rowData =
                        RedisResultWrapper.createRowDataForHash(
                                queryParameter, result, redisValueDataStructure, dataTypes);
                results.add(rowData);
                break;
            }
            case ZSCORE: {
                Double result =
                        this.redisCommandsContainer
                                .zscore(queryParameter[0], queryParameter[1])
                                .get();
                GenericRowData rowData =
                        RedisResultWrapper.createRowDataForSortedSet(
                                queryParameter, result, dataTypes);
                results.add(rowData);
                break;
            }
            case LRANGE: {
                List list =
                        this.redisCommandsContainer
                                .lRange(
                                        queryParameter[0],
                                        this.readableConfig.get(RedisOptions.SCAN_RANGE_START),
                                        this.readableConfig.get(RedisOptions.SCAN_RANGE_STOP))
                                .get();
                list.forEach(
                        result -> {
                            GenericRowData rowData =
                                    RedisResultWrapper.createRowDataForString(
                                            queryParameter,
                                            String.valueOf(result),
                                            redisValueDataStructure,
                                            dataTypes);
                            results.add(rowData);
                        });
                break;
            }
            case SRANDMEMBER: {
                List list =
                        this.redisCommandsContainer
                                .srandmember(
                                        String.valueOf(queryParameter[0]),
                                        readableConfig.get(RedisOptions.SCAN_COUNT))
                                .get();
                list.forEach(
                        result -> {
                            GenericRowData rowData =
                                    RedisResultWrapper.createRowDataForString(
                                            queryParameter,
                                            String.valueOf(result),
                                            redisValueDataStructure,
                                            dataTypes);
                            results.add(rowData);
                        });
                break;
            }
            default:
                break;
        }
    }
}
