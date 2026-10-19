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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Per-writer latency metrics for the Redis sink. Splits the end-to-end latency of a record into
 * {@code invoke_delay} (window time to {@code write()}), {@code buffer_wait} ({@code write()} to
 * first submit), {@code cmd_latency} (submit to ack of the main write command) and {@code
 * ack_freshness} (window time to ack), plus the {@code buffer_size} and {@code pending_commands}
 * backlog gauges.
 *
 * <p>Observation only: no method throws into the writer, and completion callbacks do O(1)
 * thread-safe work without logging.
 */
public final class RedisSinkLatencyMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkLatencyMetrics.class);

    /** Marker for "no window-time reference available". */
    public static final long NO_EVENT_TIME = Long.MIN_VALUE;

    public static final int SOURCE_NONE = 0;
    public static final int SOURCE_RECORD_TIMESTAMP = 1;
    public static final int SOURCE_WATERMARK = 2;

    private static final String[] SOURCE_NAMES = {"NONE", "RECORD_TIMESTAMP", "WATERMARK"};

    static final String INVOKE_DELAY = "redis_sink_invoke_delay";
    static final String BUFFER_WAIT = "redis_sink_buffer_wait";
    static final String CMD_LATENCY = "redis_sink_cmd_latency";
    static final String ACK_FRESHNESS = "redis_sink_ack_freshness";
    static final String BUFFER_SIZE = "redis_sink_buffer_size";
    static final String PENDING_COMMANDS = "redis_sink_pending_commands";
    static final String EVENT_TIME_SOURCE = "redis_sink_event_time_source";

    private static final String SUBTASK_INDEX_VARIABLE = "<subtask_index>";

    private final LongSupplier nanoClock;
    private final LongSupplier wallClock;
    private final String subtaskLabel;

    private final LatencyMetric invokeDelay;
    private final LatencyMetric bufferWait;
    private final LatencyMetric cmdLatency;
    private final LatencyMetric ackFreshness;

    /** Records buffered but not yet submitted. Written by the task thread only. */
    private volatile int buffered;

    /** Main write commands submitted but not yet completed. */
    private final AtomicInteger pending = new AtomicInteger();

    /** Window-time reference source of the most recent record. Written by the task thread only. */
    private volatile int eventTimeSource = SOURCE_NONE;

    /** Task thread only. */
    private final boolean[] sourceLogged = new boolean[SOURCE_NAMES.length];

    private volatile boolean disabled;
    private final AtomicReference<Throwable> disableCause = new AtomicReference<>();

    /** Task thread only. */
    private boolean disableWarned;

    private RedisSinkLatencyMetrics(
            LongSupplier nanoClock, LongSupplier wallClock, String subtaskLabel) {
        this.nanoClock = nanoClock;
        this.wallClock = wallClock;
        this.subtaskLabel = subtaskLabel;
        this.invokeDelay = new LatencyMetric(nanoClock);
        this.bufferWait = new LatencyMetric(nanoClock);
        this.cmdLatency = new LatencyMetric(nanoClock);
        this.ackFreshness = new LatencyMetric(nanoClock);
    }

    /**
     * Registers the metrics on the given group. Returns {@code null} (and logs one WARN) if
     * registration fails; the writer then runs without latency metrics.
     *
     * <p>{@link LinkageError} is caught as well: the histogram implementation ({@code
     * org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram}) is a flink-runtime class
     * resolved from flink-dist at runtime, so a Flink upgrade that moves or removes it must degrade
     * to "no latency metrics" instead of failing the writer. Other {@link Error}s (e.g. OOM) are
     * intentionally not caught.
     */
    public static RedisSinkLatencyMetrics tryRegister(MetricGroup g) {
        return tryRegister(g, System::nanoTime, System::currentTimeMillis);
    }

    static RedisSinkLatencyMetrics tryRegister(
            MetricGroup g, LongSupplier nanoClock, LongSupplier wallClock) {
        try {
            String subtaskLabel = g.getAllVariables().getOrDefault(SUBTASK_INDEX_VARIABLE, "?");
            RedisSinkLatencyMetrics m =
                    new RedisSinkLatencyMetrics(nanoClock, wallClock, subtaskLabel);
            m.invokeDelay.register(g, INVOKE_DELAY);
            m.bufferWait.register(g, BUFFER_WAIT);
            m.cmdLatency.register(g, CMD_LATENCY);
            m.ackFreshness.register(g, ACK_FRESHNESS);
            g.gauge(BUFFER_SIZE, (Gauge<Integer>) () -> m.buffered);
            g.gauge(PENDING_COMMANDS, (Gauge<Integer>) () -> Math.max(0, m.pending.get()));
            g.gauge(EVENT_TIME_SOURCE, (Gauge<Integer>) () -> m.eventTimeSource);
            return m;
        } catch (RuntimeException | LinkageError e) {
            LOG.warn("Failed to register Redis sink latency metrics: {}", String.valueOf(e));
            return null;
        }
    }

    /** Monotonic clock used for all duration measurements. */
    public long nanoTime() {
        return nanoClock.getAsLong();
    }

    /**
     * Resolves the window-time reference of the current record: the record timestamp if present
     * and positive, otherwise {@code currentWatermark() + 1} (an upper bound of the latency) if the
     * watermark is valid, otherwise {@link #NO_EVENT_TIME}.
     */
    public long resolveEventTime(SinkWriter.Context ctx) {
        if (skipBecauseDisabled()) {
            return NO_EVENT_TIME;
        }
        try {
            Long ts = ctx == null ? null : ctx.timestamp();
            if (ts != null && ts > 0) {
                setSource(SOURCE_RECORD_TIMESTAMP);
                return ts;
            }
            long wm = ctx == null ? Long.MIN_VALUE : ctx.currentWatermark();
            if (wm > 0 && wm < Long.MAX_VALUE) {
                setSource(SOURCE_WATERMARK);
                return wm + 1;
            }
            setSource(SOURCE_NONE);
            return NO_EVENT_TIME;
        } catch (RuntimeException | LinkageError e) {
            disable(e);
            skipBecauseDisabled();
            return NO_EVENT_TIME;
        }
    }

    /** Records {@code invoke_delay} on entry to {@code write()}. */
    public void onInvoke(long eventTimeMs) {
        if (skipBecauseDisabled()) {
            return;
        }
        try {
            if (eventTimeMs != NO_EVENT_TIME) {
                invokeDelay.update(wallClock.getAsLong() - eventTimeMs);
            }
        } catch (RuntimeException | LinkageError e) {
            disable(e);
            skipBecauseDisabled();
        }
    }

    /** Records {@code buffer_wait} when a record is submitted for the first time. */
    public void onFirstSubmit(long invokeNanos, long submitNanos) {
        if (skipBecauseDisabled()) {
            return;
        }
        try {
            bufferWait.update(TimeUnit.NANOSECONDS.toMillis(submitNanos - invokeNanos));
        } catch (RuntimeException | LinkageError e) {
            disable(e);
            skipBecauseDisabled();
        }
    }

    /** Sets the number of buffered, not yet submitted records. */
    public void setBuffered(int n) {
        buffered = n;
    }

    /**
     * Tracks a submitted main write command: increments {@code pending_commands} and registers an
     * independent completion stage that decrements it and, on success, records {@code
     * cmd_latency} and {@code ack_freshness}. Never throws.
     */
    public void onSubmitted(RedisFuture<?> f, long submitNanos, long eventTimeMs) {
        // Only to emit a pending WARN; pending must stay accurate even when disabled.
        skipBecauseDisabled();
        pending.incrementAndGet();
        try {
            // Runs on the netty thread, or synchronously here if the future is already done.
            f.whenComplete(
                    (r, t) -> {
                        // Unconditional, pairs with the increment above.
                        pending.decrementAndGet();
                        if (t != null || disabled) {
                            return;
                        }
                        try {
                            cmdLatency.update(
                                    TimeUnit.NANOSECONDS.toMillis(
                                            nanoClock.getAsLong() - submitNanos));
                            if (eventTimeMs != NO_EVENT_TIME) {
                                ackFreshness.update(wallClock.getAsLong() - eventTimeMs);
                            }
                        } catch (RuntimeException | LinkageError e) {
                            // No logging on the callback thread.
                            disable(e);
                        }
                    });
        } catch (RuntimeException | LinkageError e) {
            // Callback was not registered: undo the increment to keep the pairing.
            pending.decrementAndGet();
            disable(e);
            skipBecauseDisabled();
        }
    }

    private void setSource(int source) {
        eventTimeSource = source;
        if (!sourceLogged[source]) {
            sourceLogged[source] = true;
            LOG.info(
                    "Redis sink latency metrics: event-time reference source={} (subtask {})",
                    SOURCE_NAMES[source],
                    subtaskLabel);
        }
    }

    private void disable(Throwable e) {
        disableCause.compareAndSet(null, e);
        disabled = true;
    }

    /** Task thread only. Emits the single WARN once the metrics got disabled. */
    private boolean skipBecauseDisabled() {
        if (!disabled) {
            return false;
        }
        if (!disableWarned) {
            disableWarned = true;
            LOG.warn(
                    "Redis sink latency metrics disabled (subtask {}): {}",
                    subtaskLabel,
                    String.valueOf(disableCause.get()));
        }
        return true;
    }

    // ------------------------------------------------------------------------
    // Test accessors
    // ------------------------------------------------------------------------

    int eventTimeSource() {
        return eventTimeSource;
    }

    int pending() {
        return pending.get();
    }

    int buffered() {
        return buffered;
    }

    boolean isDisabled() {
        return disabled;
    }

    String subtaskLabel() {
        return subtaskLabel;
    }

    LatencyMetric invokeDelay() {
        return invokeDelay;
    }

    LatencyMetric bufferWait() {
        return bufferWait;
    }

    LatencyMetric cmdLatency() {
        return cmdLatency;
    }

    LatencyMetric ackFreshness() {
        return ackFreshness;
    }
}
