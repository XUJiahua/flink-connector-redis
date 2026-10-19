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

package org.apache.flink.streaming.connectors.redis.metrics;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.lettuce.core.RedisFuture;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.redis.metrics.RedisSinkLatencyMetrics.NO_EVENT_TIME;
import static org.apache.flink.streaming.connectors.redis.metrics.RedisSinkLatencyMetrics.SOURCE_NONE;
import static org.apache.flink.streaming.connectors.redis.metrics.RedisSinkLatencyMetrics.SOURCE_RECORD_TIMESTAMP;
import static org.apache.flink.streaming.connectors.redis.metrics.RedisSinkLatencyMetrics.SOURCE_WATERMARK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link RedisSinkLatencyMetrics}, driven by fake clocks and manual futures. */
class RedisSinkLatencyMetricsTest {

    private static final long MS = 1_000_000L;
    private static final long SEC = 1_000_000_000L;
    private static final long NOW = 1_760_000_000_000L;

    @RegisterExtension
    final LoggerAuditingExtension logs =
            new LoggerAuditingExtension(
                    RedisSinkLatencyMetrics.class, org.slf4j.event.Level.WARN);

    private final AtomicLong nanos = new AtomicLong(1000 * SEC);
    private final AtomicLong wall = new AtomicLong(NOW);
    private final AtomicBoolean throwNow = new AtomicBoolean(false);
    private final CapturingMetricGroup capture = new CapturingMetricGroup();

    private RedisSinkLatencyMetrics register() {
        return register(Collections.singletonMap("<subtask_index>", "3"));
    }

    private RedisSinkLatencyMetrics register(Map<String, String> vars) {
        RedisSinkLatencyMetrics m =
                RedisSinkLatencyMetrics.tryRegister(
                        capture.create(vars),
                        () -> {
                            if (throwNow.get()) {
                                throw new RuntimeException("clock boom");
                            }
                            return nanos.get();
                        },
                        wall::get);
        assertNotNull(m);
        return m;
    }

    private List<LogEvent> warnings() {
        return logs.getEvents().stream()
                .filter(e -> e.getLevel() == org.apache.logging.log4j.Level.WARN)
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------------

    @Test
    void registersExactlyFifteenMetricsWithoutFlinkPrefix() {
        register();
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
        expected.add("redis_sink_buffer_size");
        expected.add("redis_sink_pending_commands");
        expected.add("redis_sink_event_time_source");
        assertEquals(expected, capture.metrics().keySet());
        assertEquals(15, capture.metrics().size());
        assertTrue(capture.metrics().keySet().stream().noneMatch(n -> n.startsWith("flink_")));
    }

    @Test
    void tryRegisterReturnsNullWhenHistogramClassIsMissing() {
        // Simulates a Flink upgrade where the flink-runtime histogram class can no longer be
        // linked: registration must degrade to null instead of failing the writer.
        MetricGroup failing =
                (MetricGroup) Proxy.newProxyInstance(
                        MetricGroup.class.getClassLoader(),
                        new Class<?>[]{MetricGroup.class},
                        (proxy, method, args) -> {
                            throw new NoClassDefFoundError(
                                    "org/apache/flink/runtime/metrics/DescriptiveStatisticsHistogram");
                        });
        assertNull(RedisSinkLatencyMetrics.tryRegister(failing));
    }

    @Test
    void tryRegisterReturnsNullWhenRegistrationThrows() {
        MetricGroup failing =
                (MetricGroup) Proxy.newProxyInstance(
                        MetricGroup.class.getClassLoader(),
                        new Class<?>[]{MetricGroup.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getAllVariables")) {
                                return Collections.emptyMap();
                            }
                            throw new IllegalStateException("registration boom");
                        });
        assertNull(RedisSinkLatencyMetrics.tryRegister(failing));
        assertEquals(1, warnings().size());
    }

    @Test
    void missingSubtaskIndexUsesQuestionMark() {
        RedisSinkLatencyMetrics m = register(Collections.emptyMap());
        assertEquals("?", m.subtaskLabel());
    }

    // ------------------------------------------------------------------------
    // Event-time reference
    // ------------------------------------------------------------------------

    @Test
    void recordTimestampIsPreferred() {
        RedisSinkLatencyMetrics m = register();
        assertEquals(NOW - 5000, m.resolveEventTime(ctx(NOW - 5000, NOW - 9000)));
        assertEquals(SOURCE_RECORD_TIMESTAMP, m.eventTimeSource());
        assertEquals(SOURCE_RECORD_TIMESTAMP, (int) capture.<Integer>gaugeValue(
                "redis_sink_event_time_source"));
    }

    @Test
    void nullTimestampFallsBackToWatermarkPlusOne() {
        RedisSinkLatencyMetrics m = register();
        assertEquals(NOW - 8000 + 1, m.resolveEventTime(ctx(null, NOW - 8000)));
        assertEquals(SOURCE_WATERMARK, m.eventTimeSource());
    }

    @Test
    void nonPositiveTimestampFallsBackToWatermark() {
        RedisSinkLatencyMetrics m = register();
        assertEquals(NOW - 100 + 1, m.resolveEventTime(ctx(0L, NOW - 100)));
        assertEquals(SOURCE_WATERMARK, m.eventTimeSource());
        assertEquals(NOW - 100 + 1, m.resolveEventTime(ctx(-7L, NOW - 100)));
        assertEquals(SOURCE_WATERMARK, m.eventTimeSource());
    }

    @Test
    void nullTimestampAndMinWatermarkSkipsInvokeDelayAndAckFreshness() {
        RedisSinkLatencyMetrics m = register();
        long ev = m.resolveEventTime(ctx(null, Long.MIN_VALUE));
        assertEquals(NO_EVENT_TIME, ev);
        assertEquals(SOURCE_NONE, m.eventTimeSource());

        m.onInvoke(ev);
        ManualRedisFuture<Boolean> f = new ManualRedisFuture<>();
        m.onSubmitted(f, nanos.get(), ev);
        nanos.addAndGet(3 * MS);
        f.complete(true);

        assertEquals(0, m.invokeDelay().histogram().getCount());
        assertEquals(0, m.ackFreshness().histogram().getCount());
        assertEquals(1, m.cmdLatency().histogram().getCount());
        assertEquals(3, m.cmdLatency().last());
    }

    @Test
    void invalidWatermarksAndNullContextResolveToNone() {
        RedisSinkLatencyMetrics m = register();
        for (SinkWriter.Context c : Arrays.asList(ctx(null, 0L), ctx(null, -10L), ctx(null, Long.MAX_VALUE), null)) {
            assertEquals(NO_EVENT_TIME, m.resolveEventTime(c));
            assertEquals(SOURCE_NONE, m.eventTimeSource());
        }
    }

    @Test
    void sourceGaugeFollowsMostRecentRecord() {
        RedisSinkLatencyMetrics m = register();
        m.resolveEventTime(ctx(null, Long.MIN_VALUE));
        assertEquals(0, (int) capture.<Integer>gaugeValue("redis_sink_event_time_source"));
        m.resolveEventTime(ctx(null, NOW - 1000));
        assertEquals(2, (int) capture.<Integer>gaugeValue("redis_sink_event_time_source"));
    }

    @Test
    void watermarkFallbackInvokeDelayValue() {
        RedisSinkLatencyMetrics m = register();
        m.onInvoke(m.resolveEventTime(ctx(null, NOW - 8000)));
        assertEquals(7999, m.invokeDelay().last());
    }

    @Test
    void negativeDeltasClampToZero() {
        RedisSinkLatencyMetrics m = register();
        m.onInvoke(NOW + 5000);
        assertEquals(0, m.invokeDelay().last());
        assertEquals(1, m.invokeDelay().histogram().getCount());
        m.onFirstSubmit(nanos.get(), nanos.get() - 5 * MS);
        assertEquals(0, m.bufferWait().last());
    }

    // ------------------------------------------------------------------------
    // Submit / ack
    // ------------------------------------------------------------------------

    @Test
    void alreadyCompletedFutureIsCountedSynchronously() {
        RedisSinkLatencyMetrics m = register();
        long submit = nanos.get();
        nanos.addAndGet(7 * MS);
        ManualRedisFuture<Boolean> f = new ManualRedisFuture<>();
        f.complete(true);
        m.onSubmitted(f, submit, NOW - 2000);
        assertEquals(0, m.pending());
        assertEquals(1, m.cmdLatency().histogram().getCount());
        assertEquals(7, m.cmdLatency().last());
        assertEquals(2000, m.ackFreshness().last());
    }

    @Test
    void laterCompletedFutureMovesPending() {
        RedisSinkLatencyMetrics m = register();
        ManualRedisFuture<Boolean> f = new ManualRedisFuture<>();
        m.onSubmitted(f, nanos.get(), NOW - 1000);
        assertEquals(1, m.pending());
        assertEquals(1, (int) capture.<Integer>gaugeValue("redis_sink_pending_commands"));
        nanos.addAndGet(40 * MS);
        wall.addAndGet(40);
        f.complete(true);
        assertEquals(0, m.pending());
        assertEquals(40, m.cmdLatency().last());
        assertEquals(1040, m.ackFreshness().last());
    }

    @Test
    void exceptionallyCompletedFutureRecordsNoAckSamples() {
        RedisSinkLatencyMetrics m = register();
        ManualRedisFuture<Boolean> f = new ManualRedisFuture<>();
        m.onSubmitted(f, nanos.get(), NOW - 1000);
        f.completeExceptionally(new RuntimeException("redis down"));
        assertEquals(0, m.pending());
        assertEquals(0, m.cmdLatency().histogram().getCount());
        assertEquals(0, m.ackFreshness().histogram().getCount());
    }

    @Test
    void bufferWaitAndBufferedGauge() {
        RedisSinkLatencyMetrics m = register();
        m.onFirstSubmit(nanos.get() - 250 * MS, nanos.get());
        assertEquals(250, m.bufferWait().last());
        m.setBuffered(4);
        assertEquals(4, (int) capture.<Integer>gaugeValue("redis_sink_buffer_size"));
    }

    @Test
    void maxGaugeExpiresAfterSixtySecondsWhileLastStays() {
        RedisSinkLatencyMetrics m = register();
        nanos.set(0);
        m.onInvoke(NOW - 500);
        assertEquals(500L, (long) capture.<Long>gaugeValue("redis_sink_invoke_delay_max_ms"));
        nanos.set(59 * SEC);
        assertEquals(500L, (long) capture.<Long>gaugeValue("redis_sink_invoke_delay_max_ms"));
        nanos.set(60 * SEC);
        assertEquals(0L, (long) capture.<Long>gaugeValue("redis_sink_invoke_delay_max_ms"));
        assertEquals(500L, (long) capture.<Long>gaugeValue("redis_sink_invoke_delay_last_ms"));
    }

    // ------------------------------------------------------------------------
    // Disabled semantics
    // ------------------------------------------------------------------------

    @Test
    void callbackFailureDisablesMetricsWithoutAffectingFuture() throws Exception {
        RedisSinkLatencyMetrics m = register();
        ManualRedisFuture<Boolean> f = new ManualRedisFuture<>();
        m.onSubmitted(f, nanos.get(), NOW - 1000);
        CompletableFuture<String> other = f.thenApply(v -> "seen-" + v);

        throwNow.set(true);
        f.complete(true);
        throwNow.set(false);

        assertEquals("seen-true", other.get(1, TimeUnit.SECONDS));
        assertTrue(f.get());
        assertEquals(0, m.pending());
        assertTrue(m.isDisabled());
        assertEquals(0, warnings().size(), "callback thread must not log");

        m.onInvoke(NOW - 10);
        m.onFirstSubmit(nanos.get() - MS, nanos.get());
        assertEquals(0, m.invokeDelay().histogram().getCount());
        assertEquals(0, m.bufferWait().histogram().getCount());
        assertEquals(NO_EVENT_TIME, m.resolveEventTime(ctx(NOW, NOW)));

        ManualRedisFuture<Boolean> g = new ManualRedisFuture<>();
        m.onSubmitted(g, nanos.get(), NOW - 1000);
        assertEquals(1, m.pending());
        g.complete(true);
        assertEquals(0, m.pending());
        assertEquals(0, m.cmdLatency().histogram().getCount());

        m.setBuffered(6);
        assertEquals(6, m.buffered());

        List<LogEvent> warns = warnings();
        assertEquals(1, warns.size());
        assertEquals(Thread.currentThread().getName(), warns.get(0).getThreadName());
    }

    @Test
    void whenCompleteRegistrationFailureIsIsolated() {
        RedisSinkLatencyMetrics m = register();
        RedisFuture<?> broken =
                (RedisFuture<?>) Proxy.newProxyInstance(
                        RedisFuture.class.getClassLoader(),
                        new Class<?>[]{RedisFuture.class},
                        (proxy, method, args) -> {
                            throw new RuntimeException("whenComplete boom");
                        });
        m.onSubmitted(broken, nanos.get(), NOW);
        assertEquals(0, m.pending());
        assertTrue(m.isDisabled());
        assertEquals(1, warnings().size());
        assertFalse(m.cmdLatency().histogram().getCount() > 0);
    }

    // ------------------------------------------------------------------------

    private static SinkWriter.Context ctx(Long ts, long wm) {
        return new SinkWriter.Context() {

            @Override
            public long currentWatermark() {
                return wm;
            }

            @Override
            public Long timestamp() {
                return ts;
            }
        };
    }
}
