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

import org.apache.flink.api.common.JobStatus;
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
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in acceptance tests. Use a dedicated, disposable Redis: CLIENT PAUSE affects the server. */
@EnabledIfSystemProperty(named = "redis.it.port", matches = "[0-9]+")
class RedisSinkRealRedisTest {

    private static final Map<String, SourceControl> SOURCES = new ConcurrentHashMap<>();
    private final List<String> keys = new ArrayList<>();
    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> redis;

    @BeforeEach
    void connect() {
        client = RedisClient.create("redis://127.0.0.1:" + System.getProperty("redis.it.port"));
        client.setDefaultTimeout(Duration.ofSeconds(5));
        connection = client.connect();
        redis = connection.sync();
        assertEquals("PONG", redis.ping());
    }

    @AfterEach
    void disconnect() {
        try {
            for (String key : keys) {
                redis.del(key);
            }
        } finally {
            connection.close();
            client.shutdown();
        }
    }

    @Test
    void batchOneSendsWithoutWaitingForLongInterval() throws Exception {
        String key = key();
        try (RunningJob job = start(key, "v", 1, 1, 60000)) {
            long elapsed = awaitValue(key, "v0", job);
            assertTrue(elapsed < 5000, "batch.size=1 waited for the interval");
            assertEquals(JobStatus.RUNNING, job.client.getJobStatus().get(5, TimeUnit.SECONDS));
            report("batch-one", elapsed, "v0");
        }
    }

    @Test
    void singleRecordFlushesAfterOneSecondWhileSourceStaysIdle() throws Exception {
        String key = key();
        try (RunningJob job = start(key, "v", 1, 100, 1000)) {
            long elapsed = awaitValue(key, "v0", job);
            assertTrue(elapsed >= 900 && elapsed < 5000, "unexpected timer observation: " + elapsed);
            assertEquals(JobStatus.RUNNING, job.client.getJobStatus().get(5, TimeUnit.SECONDS));
            report("single-idle", elapsed, "v0");
        }
    }

    @Test
    void fullBatchSendsWithoutWaitingForLongIntervalAndKeepsOrder() throws Exception {
        String key = key();
        try (RunningJob job = start(key, "v", 100, 100, 60000)) {
            long elapsed = awaitValue(key, "v99", job);
            assertTrue(elapsed < 5000, "full batch waited for the interval");
            assertEquals(JobStatus.RUNNING, job.client.getJobStatus().get(5, TimeUnit.SECONDS));
            report("full-batch", elapsed, "v99");
        }
    }

    @Test
    void asyncRedisErrorFailsIdleFlinkJob() throws Exception {
        String key = key();
        redis.set(key, "string-instead-of-hash");
        try (RunningJob job = start(key, "v", 1, 100, 1000)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> job.client.getJobExecutionResult().get(15, TimeUnit.SECONDS));
            job.terminated = true;
            StringBuilder causes = new StringBuilder();
            for (Throwable t = failure; t != null; t = t.getCause()) {
                causes.append(t.getMessage()).append('\n');
            }
            assertTrue(causes.toString().contains("Async Redis write failed"), causes.toString());
            assertTrue(causes.toString().contains("WRONGTYPE"), causes.toString());
            assertEquals("string-instead-of-hash", redis.get(key));
            report("idle-async-error", elapsed(job.source), "job failed with WRONGTYPE");
        }
    }

    @Test
    void checkpointFlushWaitsForRealRedisAcknowledgement() throws Exception {
        String key = key();
        Configuration config = config(100, 60000);
        RedisSinkWriter writer = new RedisSinkWriter(connectionConfig(), mapper(config),
                schema().getColumnDataTypes(), config, null, 1, null, null);
        try {
            writer.write(row(key, "v"), null);
            assertFalse(redis.hexists(key, "f"), "record should still be buffered");
            redis.clientPause(600);
            long started = System.nanoTime();
            // This is the Writer checkpoint callback, not a distributed checkpoint-barrier test.
            writer.flush(false);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsed >= 400, "flush returned before the paused Redis could acknowledge");
            assertEquals("v", redis.hget(key, "f"));
            report("checkpoint-flush-ack", elapsed, "v");
        } finally {
            writer.close();
        }
    }

    @Test
    void independentJobsCanStillOverwriteNewerSnapshotWithOlderOne() throws Exception {
        String key = key();
        // Widen the old batch's interval to make the cross-job overwrite reproducible locally.
        try (RunningJob oldJob = start(key, "old", 1, 100, 5000)) {
            try (RunningJob newJob = start(key, "new", 1, 1, 60000)) {
                long newElapsed = awaitValue(key, "new0", newJob);
                report("two-jobs-new-visible", newElapsed, "new0");
                long oldElapsed = awaitValue(key, "old0", oldJob);
                assertEquals(JobStatus.RUNNING, oldJob.client.getJobStatus().get(5, TimeUnit.SECONDS));
                assertEquals(JobStatus.RUNNING, newJob.client.getJobStatus().get(5, TimeUnit.SECONDS));
                report("two-jobs-old-overwrite", oldElapsed, "old0");
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "redis.it.outage.container", matches = ".+")
    void disconnectedRedisFailsCheckpointFlush() throws Exception {
        String container = System.getProperty("redis.it.outage.container");
        int port = Integer.parseInt(System.getProperty("redis.it.outage.port"));
        FlinkSingleConfig endpoint = new FlinkSingleConfig.Builder().setHost("127.0.0.1")
                .setPort(port).setDatabase(0).setTimeout(2000)
                .setLettuceConfig(new LettuceConfig(2, 2)).build();
        Configuration config = config(1, 1000);
        RedisSinkWriter writer = new RedisSinkWriter(endpoint, mapper(config),
                schema().getColumnDataTypes(), config, null, 1, null, null);
        String key = "redis-timer-outage:" + UUID.randomUUID();
        try {
            writer.write(row(key, "before-outage"), null);
            writer.flush(false);
            docker("stop", container);
            writer.write(row(key, "after-outage"), null);
            long started = System.nanoTime();
            IOException failure = assertThrows(IOException.class, () -> writer.flush(false));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            Throwable cause = failure;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            report("disconnected-checkpoint-failure", elapsed,
                    failure.getClass().getSimpleName() + "/" + cause.getClass().getSimpleName());
        } finally {
            // Restore this isolated endpoint before close, allowing queued async commands to settle.
            try {
                docker("start", container);
            } finally {
                writer.close();
            }
        }
        RedisClient recovered = RedisClient.create("redis://127.0.0.1:" + port);
        recovered.setDefaultTimeout(Duration.ofSeconds(5));
        try (StatefulRedisConnection<String, String> check = recovered.connect()) {
            assertEquals("after-outage", check.sync().hget(key, "f"));
            System.out.println("REAL_REDIS disconnected-recovered result=after-outage");
        } finally {
            recovered.shutdown();
        }
    }

    private static void docker(String action, String container) throws Exception {
        Process process = new ProcessBuilder("docker", action, container).redirectErrorStream(true).start();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), "docker " + action + " timed out");
        assertEquals(0, process.exitValue(), "docker " + action + " failed");
    }

    private String key() {
        String key = "redis-timer-it:" + UUID.randomUUID();
        keys.add(key);
        return key;
    }

    private static Configuration config(int batchSize, long interval) {
        Configuration config = new Configuration();
        config.set(RedisOptions.SINK_BATCH_SIZE, batchSize);
        config.set(RedisOptions.SINK_BATCH_FLUSH_INTERVAL, interval);
        config.set(RedisOptions.MAX_RETRIES, 0);
        return config;
    }

    private static FlinkSingleConfig connectionConfig() {
        return new FlinkSingleConfig.Builder().setHost("127.0.0.1")
                .setPort(Integer.parseInt(System.getProperty("redis.it.port")))
                .setDatabase(0).setTimeout(2000).setLettuceConfig(new LettuceConfig(2, 2)).build();
    }

    @SuppressWarnings("unchecked")
    private static RedisSinkMapper<RowData> mapper(Configuration config) {
        return (RedisSinkMapper<RowData>) (RedisSinkMapper<?>) new RowRedisSinkMapper(RedisCommand.HSET, config);
    }

    private static ResolvedSchema schema() {
        return ResolvedSchema.of(Column.physical("k", DataTypes.STRING()),
                Column.physical("f", DataTypes.STRING()), Column.physical("v", DataTypes.STRING()));
    }

    private static RowData row(String key, String value) {
        return GenericRowData.of(StringData.fromString(key), StringData.fromString("f"),
                StringData.fromString(value));
    }

    private static RunningJob start(String key, String prefix, int count, int batchSize, long interval)
            throws Exception {
        String id = UUID.randomUUID().toString();
        SourceControl source = new SourceControl();
        SOURCES.put(id, source);
        Configuration runtime = new Configuration();
        runtime.setString("restart-strategy.type", "none");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(runtime);
        env.setParallelism(1);
        assertFalse(env.getCheckpointConfig().isCheckpointingEnabled());
        List<String> inputs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            inputs.add(prefix + i);
        }
        inputs.add("HOLD");
        Configuration config = config(batchSize, interval);
        env.fromData(inputs).flatMap((String value, Collector<RowData> out) -> {
            SourceControl control = SOURCES.get(id);
            if ("HOLD".equals(value)) {
                control.holding.countDown();
                if (!control.release.await(60, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test controller did not release the idle source");
                }
            } else {
                control.emittedAt.compareAndSet(0, System.nanoTime());
                out.collect(row(key, value));
            }
        }).returns(TypeInformation.of(RowData.class))
                .sinkTo(new RedisSink(connectionConfig(), mapper(config), schema(), config, false))
                .disableChaining();
        RunningJob job = new RunningJob(env.executeAsync("real-redis-timer-" + prefix), source, id);
        if (!source.holding.await(20, TimeUnit.SECONDS)) {
            job.close();
            throw new AssertionError("source never entered its idle state");
        }
        return job;
    }

    private long awaitValue(String key, String value, RunningJob job) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (value.equals(redis.hget(key, "f"))) {
                return elapsed(job.source);
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Redis never exposed " + value + "; actual=" + redis.hget(key, "f"));
    }

    private static long elapsed(SourceControl source) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - source.emittedAt.get());
    }

    private static void report(String scenario, long elapsed, String result) {
        System.out.println("REAL_REDIS " + scenario + " elapsed_ms=" + elapsed + " result=" + result);
    }

    private static class SourceControl {

        final CountDownLatch holding = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicLong emittedAt = new AtomicLong();
    }

    private static class RunningJob implements AutoCloseable {

        final JobClient client;
        final SourceControl source;
        final String id;
        boolean terminated;

        RunningJob(JobClient client, SourceControl source, String id) {
            this.client = client;
            this.source = source;
            this.id = id;
        }

        @Override
        public void close() throws Exception {
            try {
                if (!terminated) {
                    client.cancel().get(20, TimeUnit.SECONDS);
                }
            } finally {
                source.release.countDown();
                SOURCES.remove(id);
            }
        }
    }
}
