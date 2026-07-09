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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.config.LettuceConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reproduces async Redis write failures caused by a closed Lettuce connection. */
public class RedisSinkWriterConnectionClosedTest extends TestRedisConfigBase {

    @Test
    public void testFlushSurfacesConnectionClosedFromPendingAsyncWrites() throws Exception {
        Configuration config = sinkConfig(0);
        RedisSinkWriter writer = createWriter(config);

        try {
            List<ManualRedisFuture<String>> setFutures = new ArrayList<>();
            RedisCommandsContainer originalContainer =
                    replaceCommandsContainer(writer, pendingSetContainer(setFutures));
            originalContainer.close();

            RowData record =
                    record("f1:iS0010:47233011:5NDnc596hX3:card:20260701", "limit-value");
            writer.write(record, null);
            writer.write(record, null);
            writer.write(record, null);
            assertEquals(3, setFutures.size());

            RedisException connectionClosed = new RedisException("Connection closed");
            setFutures.forEach(future -> future.completeExceptionally(connectionClosed));

            IOException failure = assertThrows(IOException.class, () -> writer.flush(false));
            assertEquals("Async Redis write failed", failure.getMessage());
            assertTrue(failure.getCause() instanceof RedisException);
            assertEquals("Connection closed", failure.getCause().getMessage());
        } finally {
            writer.close();
        }
    }

    @Test
    public void testConnectionClosedIsRetriedAfterRebuildingConnection() throws Exception {
        String key = "connection_closed_retry_key";
        String value = "retry-value";
        singleRedisCommands.del(key);

        Configuration config = sinkConfig(1);
        RedisSinkWriter writer = createWriter(config);

        try {
            List<ManualRedisFuture<String>> setFutures = new ArrayList<>();
            RedisCommandsContainer originalContainer =
                    replaceCommandsContainer(writer, pendingSetContainer(setFutures));
            originalContainer.close();

            writer.write(record(key, value), null);
            assertEquals(1, setFutures.size());

            setFutures.get(0).completeExceptionally(new RedisException("Connection closed"));

            writer.flush(false);
            assertEquals(value, singleRedisCommands.get(key));
        } finally {
            writer.close();
            singleRedisCommands.del(key);
        }
    }

    @SuppressWarnings("unchecked")
    private static RedisSinkWriter createWriter(Configuration config) {
        FlinkSingleConfig flinkConfig =
                new FlinkSingleConfig.Builder()
                        .setHost(REDIS_HOST)
                        .setPort(REDIS_PORT)
                        .setPassword(REDIS_PASSWORD)
                        .setTimeout(2000)
                        .setDatabase(0)
                        .setLettuceConfig(new LettuceConfig(null, null, 1000))
                        .build();
        RedisSinkMapper<RowData> mapper =
                (RedisSinkMapper<RowData>) (RedisSinkMapper<?>) new RowRedisSinkMapper(RedisCommand.SET, config);
        List<DataType> columnDataTypes = Arrays.asList(DataTypes.STRING(), DataTypes.STRING());
        return new RedisSinkWriter(flinkConfig, mapper, columnDataTypes, config, null, 1, null);
    }

    private static Configuration sinkConfig(int maxRetries) {
        Configuration config = new Configuration();
        config.set(RedisOptions.SINK_BATCH_SIZE, 1);
        config.set(RedisOptions.SINK_MAX_IN_FLIGHT_REQUESTS, 10);
        config.set(RedisOptions.MAX_RETRIES, maxRetries);
        return config;
    }

    private static RowData record(String key, String value) {
        return GenericRowData.of(StringData.fromString(key), StringData.fromString(value));
    }

    private static RedisCommandsContainer replaceCommandsContainer(
            RedisSinkWriter writer, RedisCommandsContainer replacement) throws Exception {
        Field field = RedisSinkWriter.class.getDeclaredField("redisCommandsContainer");
        field.setAccessible(true);
        RedisCommandsContainer original = (RedisCommandsContainer) field.get(writer);
        field.set(writer, replacement);
        return original;
    }

    private static RedisCommandsContainer pendingSetContainer(
            List<ManualRedisFuture<String>> setFutures) {
        return (RedisCommandsContainer) Proxy.newProxyInstance(
                RedisCommandsContainer.class.getClassLoader(),
                new Class<?>[]{RedisCommandsContainer.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "open":
                        case "close":
                            return null;
                        case "set":
                            ManualRedisFuture<String> future = new ManualRedisFuture<>();
                            setFutures.add(future);
                            return future;
                        case "toString":
                            return "PendingSetRedisCommandsContainer";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new UnsupportedOperationException(
                                    "Unexpected Redis command: " + method.getName());
                    }
                });
    }

    private static final class ManualRedisFuture<T> extends CompletableFuture<T>
            implements RedisFuture<T> {

        private volatile Throwable error;

        @Override
        public boolean completeExceptionally(Throwable ex) {
            this.error = ex;
            return super.completeExceptionally(ex);
        }

        @Override
        public String getError() {
            return error == null ? null : error.getMessage();
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            try {
                get(timeout, unit);
                return true;
            } catch (ExecutionException e) {
                return true;
            } catch (TimeoutException e) {
                return false;
            }
        }
    }
}
