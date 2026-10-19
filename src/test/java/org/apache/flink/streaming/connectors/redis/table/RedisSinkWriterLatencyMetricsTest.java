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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.config.LettuceConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.metrics.CapturingMetricGroup;
import org.apache.flink.streaming.connectors.redis.metrics.ManualRedisFuture;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writer-level tests for the Redis sink latency metrics. No Redis is needed: the writer's
 * constructor connects to a {@link FakeRespServer}, then the commands container is replaced via
 * reflection by a Proxy fake whose futures are completed manually.
 */
public class RedisSinkWriterLatencyMetricsTest {

    private static final String PENDING = "redis_sink_pending_commands";
    private static final String BUFFER_SIZE = "redis_sink_buffer_size";
    private static final String SOURCE = "redis_sink_event_time_source";

    private static FakeRespServer server;

    @BeforeAll
    static void startServer() throws IOException {
        server = new FakeRespServer();
    }

    @AfterAll
    static void stopServer() throws IOException {
        server.close();
    }

    // ------------------------------------------------------------------------
    // Test fixtures
    // ------------------------------------------------------------------------

    /** Fake container recording every call; HSET futures are completed by the test. */
    private static final class FakeContainer {

        final List<String> calls = new CopyOnWriteArrayList<>();
        final List<ManualRedisFuture<Boolean>> hsetFutures = new CopyOnWriteArrayList<>();
        final AtomicBoolean autoComplete = new AtomicBoolean(false);
        final Set<Integer> syncThrowOnHsetCall = Collections.synchronizedSet(new HashSet<>());
        final AtomicInteger hsetCalls = new AtomicInteger();

        RedisCommandsContainer proxy() {
            return (RedisCommandsContainer) Proxy.newProxyInstance(
                    RedisCommandsContainer.class.getClassLoader(),
                    new Class<?>[]{RedisCommandsContainer.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "open":
                            case "close":
                                return null;
                            case "toString":
                                return "FakeContainer";
                            case "hashCode":
                                return System.identityHashCode(proxy);
                            case "equals":
                                return proxy == args[0];
                            default:
                                break;
                        }
                        calls.add(method.getName() + Arrays.asList(args));
                        switch (method.getName()) {
                            case "hset":
                                int n = hsetCalls.incrementAndGet();
                                if (syncThrowOnHsetCall.contains(n)) {
                                    throw new RedisException("sync failure #" + n);
                                }
                                ManualRedisFuture<Boolean> f = new ManualRedisFuture<>();
                                hsetFutures.add(f);
                                if (autoComplete.get()) {
                                    f.complete(true);
                                }
                                return f;
                            case "expire":
                                ManualRedisFuture<Boolean> e = new ManualRedisFuture<>();
                                e.complete(true);
                                return e;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unexpected Redis command: " + method.getName());
                        }
                    });
        }

        void completeAll() {
            for (ManualRedisFuture<Boolean> f : hsetFutures) {
                f.complete(true);
            }
        }
    }

    private static final class Ctx implements SinkWriter.Context {

        volatile Long timestamp;
        volatile long watermark = Long.MIN_VALUE;

        @Override
        public long currentWatermark() {
            return watermark;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }

    private final CapturingMetricGroup capture = new CapturingMetricGroup();
    private final FakeContainer fake = new FakeContainer();

    private SinkWriterMetricGroup metricGroup() {
        SimpleCounter send = new SimpleCounter();
        SimpleCounter errors = new SimpleCounter();
        SimpleCounter bytes = new SimpleCounter();
        return capture.create(
                SinkWriterMetricGroup.class,
                Collections.singletonMap("<subtask_index>", "0"),
                (method, args) -> {
                    switch (method.getName()) {
                        case "getNumRecordsSendCounter":
                            return send;
                        case "getNumRecordsSendErrorsCounter":
                            return errors;
                        case "getNumBytesSendCounter":
                            return bytes;
                        default:
                            throw new UnsupportedOperationException(method.toString());
                    }
                });
    }

    private static Configuration config(int batchSize, int maxInFlight, int maxRetries) {
        Configuration config = new Configuration();
        config.set(RedisOptions.SINK_BATCH_SIZE, batchSize);
        config.set(RedisOptions.SINK_MAX_IN_FLIGHT_REQUESTS, maxInFlight);
        config.set(RedisOptions.MAX_RETRIES, maxRetries);
        return config;
    }

    @SuppressWarnings("unchecked")
    private RedisSinkWriter createWriter(
            Configuration config, SinkWriterMetricGroup metricGroup, FakeContainer container)
            throws Exception {
        return createWriter(config, metricGroup, container, null);
    }

    private RedisSinkWriter createWriter(
            Configuration config, SinkWriterMetricGroup metricGroup, FakeContainer container,
            ProcessingTimeService timeService) throws Exception {
        return createWriter(config, metricGroup, container, timeService, null);
    }

    private RedisSinkWriter createWriter(
            Configuration config, SinkWriterMetricGroup metricGroup, FakeContainer container,
            ProcessingTimeService timeService, MailboxExecutor mailbox) throws Exception {
        FlinkSingleConfig flinkConfig =
                new FlinkSingleConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(server.port())
                        .setTimeout(2000)
                        .setDatabase(0)
                        .setLettuceConfig(new LettuceConfig(null, null))
                        .build();
        RedisSinkMapper<RowData> mapper =
                (RedisSinkMapper<RowData>) (RedisSinkMapper<?>) new RowRedisSinkMapper(RedisCommand.HSET, config);
        List<DataType> columnDataTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        RedisSinkWriter writer =
                new RedisSinkWriter(
                        flinkConfig, mapper, columnDataTypes, config, metricGroup, 1, mailbox, timeService);
        Field field = RedisSinkWriter.class.getDeclaredField("redisCommandsContainer");
        field.setAccessible(true);
        RedisCommandsContainer original = (RedisCommandsContainer) field.get(writer);
        field.set(writer, container.proxy());
        original.close();
        return writer;
    }

    private static final class ManualTime implements ProcessingTimeService {

        long now;
        final java.util.TreeMap<Long, java.util.List<ProcessingTimeCallback>> timers =
                new java.util.TreeMap<>();

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> registerTimer(
                long timestamp, ProcessingTimeCallback callback) {
            timers.computeIfAbsent(timestamp, ignored -> new ArrayList<>()).add(callback);
            // Deliberately deliver cancelled callbacks too, simulating already queued timer mail.
            return null;
        }

        void advance(long timestamp) throws Exception {
            now = timestamp;
            while (!timers.isEmpty() && timers.firstKey() <= now) {
                java.util.Map.Entry<Long, java.util.List<ProcessingTimeCallback>> entry =
                        timers.pollFirstEntry();
                for (ProcessingTimeCallback callback : entry.getValue()) {
                    callback.onProcessingTime(entry.getKey());
                }
            }
        }
    }

    @Test
    void singleRecordFlushesWithoutMoreInputOrCheckpoint() throws Exception {
        ManualTime time = new ManualTime();
        fake.autoComplete.set(true);
        RedisSinkWriter writer = createWriter(config(100, 10, 0), null, fake, time);
        try {
            writer.write(row("k", "f", "v"), new Ctx());
            time.advance(999);
            assertTrue(fake.calls.isEmpty());
            time.advance(1000);
            assertEquals(Collections.singletonList("hset[k, f, v]"), fake.calls);
        } finally {
            writer.close();
        }
    }

    @Test
    void laterRecordsDoNotPostponeFirstRecordDeadline() throws Exception {
        ManualTime time = new ManualTime();
        fake.autoComplete.set(true);
        RedisSinkWriter writer = createWriter(config(100, 10, 0), null, fake, time);
        try {
            writer.write(row("k1", "f", "v"), new Ctx());
            time.advance(900);
            writer.write(row("k2", "f", "v"), new Ctx());
            time.advance(1000);
            assertEquals(2, fake.hsetCalls.get());
        } finally {
            writer.close();
        }
    }

    @Test
    void staleTimerAfterSizeFlushCannotFlushNextBatch() throws Exception {
        ManualTime time = new ManualTime();
        fake.autoComplete.set(true);
        RedisSinkWriter writer = createWriter(config(2, 10, 0), null, fake, time);
        try {
            writer.write(row("k1", "f", "v"), new Ctx());
            time.advance(100);
            writer.write(row("k2", "f", "v"), new Ctx());
            assertEquals(2, fake.hsetCalls.get());
            time.advance(500);
            writer.write(row("k3", "f", "v"), new Ctx());
            time.advance(1000);
            assertEquals(2, fake.hsetCalls.get());
            time.advance(1500);
            assertEquals(3, fake.hsetCalls.get());
        } finally {
            writer.close();
        }
    }

    @Test
    void checkpointAndCloseInvalidateTimers() throws Exception {
        ManualTime time = new ManualTime();
        fake.autoComplete.set(true);
        RedisSinkWriter writer = createWriter(config(100, 10, 0), null, fake, time);
        try {
            writer.write(row("k1", "f", "v"), new Ctx());
            writer.flush(false);
            time.advance(500);
            writer.write(row("k2", "f", "v"), new Ctx());
            time.advance(1000);
            assertEquals(1, fake.hsetCalls.get());
            writer.close();
            time.advance(2000);
            assertEquals(2, fake.hsetCalls.get());
        } finally {
            writer.close();
        }
    }

    @Test
    void zeroIntervalDisablesTimers() throws Exception {
        ManualTime time = new ManualTime();
        fake.autoComplete.set(true);
        Configuration config = config(100, 10, 0);
        config.set(RedisOptions.SINK_BATCH_FLUSH_INTERVAL, 0L);
        RedisSinkWriter writer = createWriter(config, null, fake, time);
        try {
            writer.write(row("k", "f", "v"), new Ctx());
            time.advance(10000);
            assertTrue(fake.calls.isEmpty());
            assertTrue(time.timers.isEmpty());
        } finally {
            writer.close();
        }
    }

    @Test
    void timedAsyncFailureNotifiesMailboxWithoutAnotherWrite() throws Exception {
        ManualTime time = new ManualTime();
        List<ThrowingRunnable<Exception>> mail = new ArrayList<>();
        MailboxExecutor mailbox = (MailboxExecutor) Proxy.newProxyInstance(
                MailboxExecutor.class.getClassLoader(), new Class<?>[]{MailboxExecutor.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("execute")) {
                        for (Object arg : args) {
                            if (arg instanceof ThrowingRunnable) {
                                mail.add((ThrowingRunnable<Exception>) arg);
                                return null;
                            }
                        }
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
        RedisSinkWriter writer = createWriter(config(100, 10, 0), null, fake, time, mailbox);
        try {
            writer.write(row("k", "f", "v"), new Ctx());
            time.advance(1000);
            assertEquals(1, fake.hsetCalls.get());
            fake.hsetFutures.get(0).completeExceptionally(new RedisException("timer write failed"));
            assertEquals(1, mail.size());
            IOException failure = assertThrows(IOException.class, () -> mail.get(0).run());
            assertEquals("Async Redis write failed", failure.getMessage());
        } finally {
            writer.close();
        }
    }

    @Test
    void oldestBufferAgeIsVisibleBeforeFlushAndResetsAfterSubmit() throws Exception {
        ManualTime time = new ManualTime();
        fake.autoComplete.set(true);
        RedisSinkWriter writer = createWriter(config(100, 10, 0), metricGroup(), fake, time);
        try {
            assertEquals(0L, (long) capture.<Long>gaugeValue("redis_sink_oldest_buffer_age_ms"));
            writer.write(row("k", "f", "v"), new Ctx());
            Thread.sleep(20);
            assertTrue(capture.<Long>gaugeValue("redis_sink_oldest_buffer_age_ms") >= 10L);
            assertTrue(fake.calls.isEmpty());
            time.advance(1000);
            assertEquals(0L, (long) capture.<Long>gaugeValue("redis_sink_oldest_buffer_age_ms"));
        } finally {
            writer.close();
        }
    }

    private static RowData row(String key, String field, String value) {
        return GenericRowData.of(
                StringData.fromString(key),
                StringData.fromString(field),
                StringData.fromString(value));
    }

    private int pending() {
        return capture.<Integer>gaugeValue(PENDING);
    }

    private int bufferSize() {
        return capture.<Integer>gaugeValue(BUFFER_SIZE);
    }

    private long count(String base) {
        return capture.histogram(base + "_ms").getCount();
    }

    private long last(String base) {
        return capture.<Long>gaugeValue(base + "_last_ms");
    }

    private long max(String base) {
        return capture.<Long>gaugeValue(base + "_max_ms");
    }

    private static Ctx ctxWithTimestamp(long ts) {
        Ctx c = new Ctx();
        c.timestamp = ts;
        return c;
    }

    // ------------------------------------------------------------------------
    // Cases
    // ------------------------------------------------------------------------

    @Test
    void registersExactlyFifteenMetrics() throws Exception {
        RedisSinkWriter writer = createWriter(config(1, 10, 0), metricGroup(), fake);
        try {
            Set<String> expected = new HashSet<>();
            for (String base : Arrays.asList(
                    "redis_sink_invoke_delay",
                    "redis_sink_buffer_wait",
                    "redis_sink_cmd_latency",
                    "redis_sink_ack_freshness")) {
                expected.add(base + "_ms");
                expected.add(base + "_max_ms");
                expected.add(base + "_last_ms");
            }
            expected.add(BUFFER_SIZE);
            expected.add(PENDING);
            expected.add(SOURCE);
            expected.add("redis_sink_oldest_buffer_age_ms");
            assertEquals(expected, capture.metrics().keySet());
        } finally {
            writer.close();
        }
    }

    @Test
    void bufferSizeAndPendingAroundBatchFlushAndAck() throws Exception {
        RedisSinkWriter writer = createWriter(config(3, 10, 0), metricGroup(), fake);
        try {
            Ctx ctx = ctxWithTimestamp(System.currentTimeMillis());
            writer.write(row("k1", "f", "v1"), ctx);
            writer.write(row("k2", "f", "v2"), ctx);
            assertEquals(2, bufferSize());
            assertTrue(fake.calls.isEmpty());

            writer.write(row("k3", "f", "v3"), ctx);
            assertEquals(0, bufferSize());
            assertEquals(3, fake.hsetFutures.size());
            assertEquals(3, pending());

            fake.hsetFutures.get(0).complete(true);
            assertEquals(2, pending());
            fake.hsetFutures.get(1).complete(true);
            fake.hsetFutures.get(2).complete(true);
            assertEquals(0, pending());
            assertEquals(3, count("redis_sink_cmd_latency"));
            assertEquals(3, count("redis_sink_buffer_wait"));
            assertEquals(3, count("redis_sink_ack_freshness"));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void permitExhaustionShowsPendingAndBufferedAndBufferWait() throws Exception {
        RedisSinkWriter writer = createWriter(config(3, 2, 0), metricGroup(), fake);
        AtomicInteger pendingSnapshot = new AtomicInteger(-1);
        AtomicInteger bufferSnapshot = new AtomicInteger(-1);
        AtomicBoolean pollTimedOut = new AtomicBoolean(false);
        try {
            Ctx ctx = new Ctx();
            writer.write(row("k1", "f", "v1"), ctx);
            writer.write(row("k2", "f", "v2"), ctx);
            Thread completer =
                    new Thread(
                            () -> {
                                try {
                                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                                    while (!(pending() == 2 && bufferSize() == 1)) {
                                        if (System.nanoTime() > deadline) {
                                            pollTimedOut.set(true);
                                            break;
                                        }
                                        Thread.sleep(5);
                                    }
                                    Thread.sleep(200);
                                    pendingSnapshot.set(pending());
                                    bufferSnapshot.set(bufferSize());
                                    fake.hsetFutures.get(0).complete(true);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            completer.start();
            // Blocks in acquireInFlightPermit() until the completer frees a permit.
            writer.write(row("k3", "f", "v3"), ctx);
            completer.join();

            assertFalse(pollTimedOut.get());
            assertEquals(2, pendingSnapshot.get());
            assertEquals(1, bufferSnapshot.get());
            assertTrue(last("redis_sink_buffer_wait") >= 200, "buffer_wait_last");
            assertTrue(max("redis_sink_buffer_wait") >= 200, "buffer_wait_max");
            assertEquals(1, count("redis_sink_cmd_latency"));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void recordTimestampDrivesInvokeDelayAndAckFreshness() throws Exception {
        RedisSinkWriter writer = createWriter(config(1, 10, 0), metricGroup(), fake);
        try {
            writer.write(row("k", "f", "v"), ctxWithTimestamp(System.currentTimeMillis() - 5000));
            assertTrue(last("redis_sink_invoke_delay") >= 5000);
            fake.hsetFutures.get(0).complete(true);
            assertTrue(last("redis_sink_ack_freshness") >= 5000);
            assertEquals(1, (int) capture.<Integer>gaugeValue(SOURCE));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void nullTimestampFallsBackToWatermark() throws Exception {
        RedisSinkWriter writer = createWriter(config(1, 10, 0), metricGroup(), fake);
        try {
            Ctx ctx = new Ctx();
            ctx.watermark = System.currentTimeMillis() - 5000;
            writer.write(row("k", "f", "v"), ctx);
            assertTrue(last("redis_sink_invoke_delay") >= 4999);
            fake.hsetFutures.get(0).complete(true);
            assertTrue(last("redis_sink_ack_freshness") >= 4999);
            assertEquals(2, (int) capture.<Integer>gaugeValue(SOURCE));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void noReferenceSkipsInvokeDelayAndAckFreshness() throws Exception {
        RedisSinkWriter writer = createWriter(config(1, 10, 0), metricGroup(), fake);
        try {
            writer.write(row("k", "f", "v"), new Ctx());
            fake.hsetFutures.get(0).complete(true);
            assertEquals(Collections.singletonList("hset[k, f, v]"), fake.calls);
            assertEquals(0, count("redis_sink_invoke_delay"));
            assertEquals(0, count("redis_sink_ack_freshness"));
            assertEquals(1, count("redis_sink_buffer_wait"));
            assertEquals(1, count("redis_sink_cmd_latency"));
            assertEquals(0, (int) capture.<Integer>gaugeValue(SOURCE));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void syncFailureThenRetrySuccessSamplesOnce() throws Exception {
        fake.syncThrowOnHsetCall.add(1);
        RedisSinkWriter writer = createWriter(config(1, 10, 1), metricGroup(), fake);
        try {
            writer.write(row("k", "f", "v"), ctxWithTimestamp(System.currentTimeMillis()));
            assertEquals(2, fake.hsetCalls.get());
            fake.hsetFutures.get(0).complete(true);
            assertEquals(1, count("redis_sink_buffer_wait"));
            assertEquals(1, count("redis_sink_cmd_latency"));
            assertEquals(1, count("redis_sink_ack_freshness"));
            assertEquals(0, pending());
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void asyncFailureStillFailsNextWriteAndRecordsNoAck() throws Exception {
        RedisSinkWriter writer = createWriter(config(1, 10, 0), metricGroup(), fake);
        try {
            writer.write(row("k", "f", "v"), ctxWithTimestamp(System.currentTimeMillis()));
            fake.hsetFutures.get(0).completeExceptionally(new RedisException("boom"));
            IOException e =
                    assertThrows(
                            IOException.class,
                            () -> writer.write(row("k2", "f", "v"), new Ctx()));
            assertEquals("Async Redis write failed", e.getMessage());
            assertEquals(0, pending());
            assertEquals(0, count("redis_sink_cmd_latency"));
            assertEquals(0, count("redis_sink_ack_freshness"));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    @Test
    void flushWaitsAndMetricsSettleAfterCompleterJoins() throws Exception {
        RedisSinkWriter writer = createWriter(config(1, 10, 0), metricGroup(), fake);
        try {
            writer.write(row("k", "f", "v"), new Ctx());
            Thread completer =
                    new Thread(
                            () -> {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                fake.hsetFutures.get(0).complete(true);
                            });
            completer.start();
            writer.flush(false);
            completer.join();
            assertEquals(0, pending());
            assertEquals(1, count("redis_sink_cmd_latency"));
        } finally {
            fake.completeAll();
            writer.close();
        }
    }

    /** Runs the same script with and without metrics and compares what Redis receives. */
    @Test
    void callSequenceIsIdenticalWithAndWithoutMetrics() throws Exception {
        FakeContainer withMetrics = new FakeContainer();
        Throwable failureWith = runScript(withMetrics, metricGroup());
        int registered = capture.metrics().size();

        FakeContainer withoutMetrics = new FakeContainer();
        Throwable failureWithout = runScript(withoutMetrics, null);

        assertEquals(16, registered);
        // the metricGroup == null run registered nothing
        assertEquals(registered, capture.metrics().size());
        assertEquals(
                Arrays.asList(
                        "hset[k1, f1, v1]",
                        "expire[k1, 60]",
                        "hset[k2, f2, v2]",
                        "hset[k2, f2, v2]",
                        "expire[k2, 60]",
                        "hset[k3, f3, v3]",
                        "expire[k3, 60]"),
                withoutMetrics.calls);
        assertEquals(withoutMetrics.calls, withMetrics.calls);
        assertEquals(failureWithout.getClass(), failureWith.getClass());
        assertEquals(failureWithout.getMessage(), failureWith.getMessage());
        assertEquals("Async Redis write failed", failureWith.getMessage());
    }

    private Throwable runScript(FakeContainer container, SinkWriterMetricGroup metricGroup)
            throws Exception {
        Configuration config = config(1, 10, 1);
        config.set(RedisOptions.TTL, 60);
        container.syncThrowOnHsetCall.add(2);
        RedisSinkWriter writer = createWriter(config, metricGroup, container);
        Throwable failure = null;
        try {
            Ctx ctx = ctxWithTimestamp(System.currentTimeMillis());
            writer.write(row("k1", "f1", "v1"), ctx);
            container.hsetFutures.get(0).complete(true);
            // first attempt throws synchronously, retry succeeds
            writer.write(row("k2", "f2", "v2"), ctx);
            container.hsetFutures.get(1).complete(true);
            writer.write(row("k3", "f3", "v3"), ctx);
            container.hsetFutures.get(2).completeExceptionally(new RedisException("async boom"));
            try {
                writer.write(row("k4", "f4", "v4"), ctx);
            } catch (IOException e) {
                failure = e;
            }
        } finally {
            container.completeAll();
            writer.close();
        }
        assertTrue(failure != null, "expected the async failure to surface");
        List<String> hsets = new ArrayList<>();
        for (String c : container.calls) {
            if (c.startsWith("hset")) {
                hsets.add(c);
            }
        }
        for (String h : hsets) {
            assertEquals(3, h.substring(5, h.length() - 1).split(", ").length, h);
        }
        return failure;
    }
}
