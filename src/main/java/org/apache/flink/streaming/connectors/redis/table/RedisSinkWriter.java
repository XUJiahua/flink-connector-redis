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
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisInsertCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.config.ZremType;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.util.TokenBucketRateLimiter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.Range;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisFuture;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Sink writer for Redis using Sink V2 API with mini-batch buffering and backpressure. */
public class RedisSinkWriter implements SinkWriter<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkWriter.class);
    private final int maxRetryTimes;
    private final boolean setIfAbsent;
    private final boolean ttlKeyNotAbsent;
    private final RedisSinkMapper<RowData> redisSinkMapper;
    private final RedisCommand redisCommand;
    private final FlinkConfigBase flinkConfigBase;
    private final List<DataType> columnDataTypes;
    private final RedisValueDataStructure redisValueDataStructure;
    private final String zremrangeby;
    private final boolean auditLog;
    protected Integer ttl;
    protected int expireTimeSeconds = -1;
    private transient volatile RedisCommandsContainer redisCommandsContainer;
    private transient Counter numRecordsSendCounter;
    private transient Counter numRecordsSendErrorsCounter;
    private transient Counter numBytesSendCounter;

    // --- Batching fields ---
    private final int batchSize;
    private final long batchFlushIntervalMs;
    private transient List<PendingRecord> batchBuffer;
    private transient long lastFlushTimeMs;

    // --- Backpressure fields ---
    private final int maxInFlightRequests;
    private transient Semaphore inFlightSemaphore;

    // Upper bound for how long flush()/close() waits on a single record's completion promise.
    // Derived from the per-command timeout and the retry budget so a record is not aborted by the
    // flush wait before its own bounded retries have had a chance to run.
    private final long flushWaitTimeoutMs;

    // --- Write QPS rate limiting ---
    private final long writeQps;
    private final double writeQpsBurstSeconds;
    private final int numParallelSubtasks;
    private transient TokenBucketRateLimiter rateLimiter;

    // --- Short-slice waiting ---
    private final transient MailboxExecutor mailboxExecutor;
    private static final long MAX_PARK_NANOS = 1_000_000L; // 1ms slices while waiting
    private static final long RETRY_BACKOFF_BASE_MS = 100L;
    private static final long RETRY_BACKOFF_MAX_MS = 2_000L;
    private final long maxInFlightAcquireTimeoutMs;
    private transient boolean flushing;

    // --- Async error tracking ---
    private transient AtomicReference<Throwable> asyncError;

    // --- Redis connection recovery ---
    private transient Object redisCommandsContainerLock;
    private transient ScheduledExecutorService retryExecutor;

    // --- In-flight per-record completion promises for flush ---
    // One promise per logical record write. It completes (normally) only once the whole write
    // finally resolves, i.e. after all bounded retries AND any follow-up command (e.g. the actual
    // SET/HSET for set.if.absent). Flush waits on these promises so a transient failure that is
    // still being retried does not abort the checkpoint; the terminal error, if any, is carried by
    // {@link #asyncError} and surfaced via {@link #checkAsyncError()}.
    private transient List<CompletableFuture<Void>> inFlightCompletions;

    /** Holds a buffered record before it is sent to Redis. */
    private static class PendingRecord {

        final String[] params;
        final RowKind kind;

        PendingRecord(String[] params, RowKind kind) {
            this.params = params;
            this.kind = kind;
        }
    }

    public RedisSinkWriter(
            FlinkConfigBase flinkConfigBase,
            RedisSinkMapper<RowData> redisSinkMapper,
            List<DataType> columnDataTypes,
            ReadableConfig readableConfig,
            SinkWriterMetricGroup metricGroup,
            int numParallelSubtasks,
            MailboxExecutor mailboxExecutor) {
        Objects.requireNonNull(flinkConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.mailboxExecutor = mailboxExecutor;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription =
                (RedisCommandDescription) redisSinkMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");

        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.ttl = redisCommandDescription.getTTL();
        this.ttlKeyNotAbsent = redisCommandDescription.getTtlKeyNotAbsent();
        this.setIfAbsent = redisCommandDescription.getSetIfAbsent();
        this.auditLog = redisCommandDescription.isAuditLog();
        if (redisCommandDescription.getExpireTime() != null) {
            this.expireTimeSeconds = redisCommandDescription.getExpireTime().toSecondOfDay();
        }

        this.columnDataTypes = columnDataTypes;
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);
        this.zremrangeby = readableConfig.get(RedisOptions.ZREM_RANGEBY);

        // Batching configuration
        this.batchSize = readableConfig.get(RedisOptions.SINK_BATCH_SIZE);
        this.batchFlushIntervalMs = readableConfig.get(RedisOptions.SINK_BATCH_FLUSH_INTERVAL);
        this.maxInFlightRequests = readableConfig.get(RedisOptions.SINK_MAX_IN_FLIGHT_REQUESTS);
        this.maxInFlightAcquireTimeoutMs =
                readableConfig.get(RedisOptions.SINK_MAX_IN_FLIGHT_ACQUIRE_TIMEOUT);

        // Write QPS rate-limiting configuration (total across all subtasks)
        this.writeQps = readableConfig.get(RedisOptions.SINK_WRITE_QPS);
        this.writeQpsBurstSeconds = readableConfig.get(RedisOptions.SINK_WRITE_QPS_BURST_SECONDS);
        this.numParallelSubtasks = Math.max(1, numParallelSubtasks);

        // Initialize metrics
        if (metricGroup != null) {
            this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
            this.numRecordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();
            this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        }

        // Initialize Redis connection
        Preconditions.checkArgument(
                redisCommand.getInsertCommand() != RedisInsertCommand.NONE,
                "the command %s do not support insert.",
                redisCommand.name());

        this.redisCommandsContainerLock = new Object();
        this.retryExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "redis-sink-retry-"
                                                    + System.identityHashCode(this));
                            thread.setDaemon(true);
                            return thread;
                        });

        try {
            this.redisCommandsContainer = createRedisCommandsContainer();
            LOG.info("success to create redis container for sink");
        } catch (Exception e) {
            this.retryExecutor.shutdownNow();
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        // Initialize batching and backpressure state
        this.batchBuffer = new ArrayList<>(batchSize);
        this.lastFlushTimeMs = System.currentTimeMillis();
        this.inFlightSemaphore = new Semaphore(maxInFlightRequests);
        this.asyncError = new AtomicReference<>(null);
        this.inFlightCompletions = new ArrayList<>();

        // Derive the flush-wait upper bound from the Lettuce per-command timeout and the retry
        // budget. A record's completion promise covers the whole write + TTL retry chain, where
        // each attempt may issue up to two sequential Redis commands (e.g. EXISTS+SET or
        // GETTTL+EXPIRE) for both the write and the TTL. So the worst-case sequential command count
        // is ~4 * (maxRetryTimes + 1). When no command timeout is configured (<= 0), fall back to a
        // 60s floor. This prevents flush from aborting a record before its own retries can run.
        Integer commandTimeoutMs =
                flinkConfigBase.getLettuceConfig() != null
                        ? flinkConfigBase.getLettuceConfig().getCommandTimeoutMs()
                        : null;
        long perCommandMs = (commandTimeoutMs != null && commandTimeoutMs > 0) ? commandTimeoutMs : 60_000L;
        this.flushWaitTimeoutMs =
                Math.max(60_000L, perCommandMs * 4L * (this.maxRetryTimes + 1L) + 5_000L);

        // Initialize write-QPS rate limiter. The configured QPS is the total budget for the whole
        // sink, so each subtask gets an equal share.
        if (writeQps > 0) {
            double perSubtaskQps = (double) writeQps / this.numParallelSubtasks;
            this.rateLimiter = new TokenBucketRateLimiter(perSubtaskQps, writeQpsBurstSeconds);
            LOG.info(
                    "Redis sink write QPS limiting enabled: total={}, subtasks={}, perSubtaskQps={}",
                    writeQps,
                    this.numParallelSubtasks,
                    perSubtaskQps);
        } else {
            this.rateLimiter = null;
        }
    }

    private RedisCommandsContainer createRedisCommandsContainer() throws Exception {
        RedisCommandsContainer container = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
        container.open();
        return container;
    }

    @Override
    public void write(RowData rowData, Context context)
            throws IOException, InterruptedException {
        // Check for async errors from previous writes
        checkAsyncError();

        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }
        String[] params = new String[calcParamNumByCommand(rowData.getArity())];
        for (int i = 0; i < params.length; i++) {
            params[i] =
                    redisSinkMapper.getKeyFromData(
                            rowData, columnDataTypes.get(i).getLogicalType(), i);
        }

        if (redisValueDataStructure == RedisValueDataStructure.row) {
            params[params.length - 1] = serializeWholeRow(rowData);
        }

        if (auditLog) {
            LOG.info("{}", rowData);
        }

        // Buffer the record
        batchBuffer.add(new PendingRecord(params, kind));

        // Flush if batch is full or interval has elapsed
        if (batchBuffer.size() >= batchSize) {
            flushBuffer();
        } else if (batchFlushIntervalMs > 0
                && (System.currentTimeMillis() - lastFlushTimeMs) >= batchFlushIntervalMs) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // Flush remaining buffered records
        flushBuffer();
        // Wait for ALL in-flight record writes (including retries and follow-up commands) to
        // complete before checking for errors.
        waitForInFlightCompletions();
        // Check for async errors
        checkAsyncError();
    }

    @Override
    public void close() throws Exception {
        // Flush remaining records before closing
        try {
            flushBuffer();
            waitForInFlightCompletions();
        } catch (Exception e) {
            LOG.warn("Error during final flush on close", e);
        }
        if (retryExecutor != null) {
            retryExecutor.shutdownNow();
        }
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    /**
     * Flushes all buffered records to Redis. Each record is sent as an async command
     * with backpressure controlled by the semaphore.
     */
    protected void flushBuffer() throws IOException, InterruptedException {
        if (batchBuffer.isEmpty() || flushing) {
            return;
        }
        // Guard against re-entrancy: yielding to the mailbox below may run mail that triggers
        // another flush; we must not iterate/clear the buffer concurrently.
        flushing = true;
        try {
            for (PendingRecord record : batchBuffer) {
                // Rate limiting: throttle the rate at which commands are issued to Redis (write
                // QPS). Wait in short slices so async Redis completion threads can release
                // in-flight permits promptly.
                if (rateLimiter != null) {
                    cooperativeWait(rateLimiter.reserve(1));
                }

                // Surface async errors BEFORE acquiring a permit, so a pending failure cannot leak
                // the just-acquired permit before it is handed off to the completion promise.
                checkAsyncError();

                // Backpressure: acquire a permit before sending the record.
                acquireInFlightPermit();

                // Ownership of the acquired permit is handed to the per-record completion promise
                // created inside sendRecord; it is released exactly once when that promise
                // completes. sendRecord does not throw for command failures (they are routed to
                // asyncError). The finally guard only releases the permit if the hand-off itself
                // failed before sendRecord could take ownership, so the permit is never leaked.
                boolean handedOff = false;
                try {
                    sendRecord(record);
                    handedOff = true;
                    if (numRecordsSendCounter != null) {
                        numRecordsSendCounter.inc();
                    }
                    if (numBytesSendCounter != null) {
                        long bytes = 0;
                        for (String param : record.params) {
                            if (param != null) {
                                bytes += param.getBytes(StandardCharsets.UTF_8).length;
                            }
                        }
                        numBytesSendCounter.inc(bytes);
                    }
                } finally {
                    if (!handedOff) {
                        inFlightSemaphore.release();
                    }
                }
            }

            batchBuffer.clear();
            lastFlushTimeMs = System.currentTimeMillis();
        } finally {
            flushing = false;
        }
    }

    /**
     * Waits for the given number of nanoseconds in short slices.
     *
     * <p>Do not yield the Flink mailbox while a batch is being flushed. Checkpoint mail could
     * re-enter {@link #flush(boolean)} before all records in the current batch have been issued and
     * registered in {@link #inFlightCompletions}, allowing the checkpoint to complete without those
     * records. Redis async callbacks complete on their own threads, so permit release does not
     * depend on mailbox execution.
     */
    private void cooperativeWait(long waitNanos) throws InterruptedException {
        if (waitNanos <= 0) {
            return;
        }
        long deadline = System.nanoTime() + waitNanos;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            if (!flushing && tryYieldMailbox()) {
                continue;
            }
            LockSupport.parkNanos(Math.min(remaining, MAX_PARK_NANOS));
        }
    }

    /** Acquires a backpressure permit. */
    private void acquireInFlightPermit() throws IOException, InterruptedException {
        if (inFlightSemaphore.tryAcquire()) {
            return;
        }
        long deadline =
                maxInFlightAcquireTimeoutMs > 0
                        ? System.nanoTime()
                                + TimeUnit.MILLISECONDS.toNanos(maxInFlightAcquireTimeoutMs)
                        : Long.MAX_VALUE;
        while (!inFlightSemaphore.tryAcquire()) {
            if (maxInFlightAcquireTimeoutMs > 0 && System.nanoTime() > deadline) {
                throw new IOException(
                        "Timeout waiting for available slot to send Redis command. "
                                + "Redis may be overloaded. Consider increasing sink.max-in-flight-requests "
                                + "or reducing throughput. To wait indefinitely, set "
                                + "sink.max-in-flight-acquire-timeout to 0.");
            }
            // Surface async failures promptly while waiting.
            checkAsyncError();
            if (!flushing && tryYieldMailbox()) {
                continue;
            }
            LockSupport.parkNanos(MAX_PARK_NANOS);
        }
    }

    /**
     * Runs a single piece of mailbox mail if any is pending. Returns {@code true} if mail was
     * executed. Safe no-op when no mailbox executor is available (e.g. in unit tests).
     */
    private boolean tryYieldMailbox() {
        if (mailboxExecutor == null) {
            return false;
        }
        try {
            return mailboxExecutor.tryYield();
        } catch (Throwable t) {
            // Mailbox may be closing; fall back to plain waiting.
            return false;
        }
    }

    /**
     * Sends a single record to Redis.
     *
     * <p>The backpressure permit acquired by {@link #acquireInFlightPermit()} in {@link
     * #flushBuffer()} is handed over to a per-record completion promise created here. The promise
     * completes (normally) exactly once, when the whole logical write has resolved — that is, after
     * all bounded retries and any follow-up command (e.g. the actual SET/HSET issued for
     * set.if.absent). The permit is released, and the promise removed from the in-flight set, only
     * on that completion. Any terminal error is recorded in {@link #asyncError} (never propagated
     * through the promise), so a transient-but-retried failure never aborts a checkpoint while a
     * genuinely failed write still restarts the job via {@link #checkAsyncError()}.
     *
     * <p>This method never throws for command failures; they are routed to {@link #asyncError}.
     */
    private void sendRecord(PendingRecord record) {
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        synchronized (inFlightCompletions) {
            inFlightCompletions.add(completion);
        }
        completion.whenComplete(
                (v, t) -> {
                    synchronized (inFlightCompletions) {
                        inFlightCompletions.remove(completion);
                    }
                    inFlightSemaphore.release();
                });

        try {
            attempt(record, this.maxRetryTimes, completion);
        } catch (Throwable t) {
            // attempt() handles its own errors; this is a last-resort guard so the permit (owned by
            // the completion promise) is never leaked on an unexpected failure.
            recordAsyncFailure(keyOf(record), t);
            completion.complete(null);
        }
    }

    /**
     * Issues the write for a record and, on failure, re-issues the whole write up to {@code
     * attemptsRemaining} more times before giving up. The shared {@code completion} promise stays
     * pending across the entire retry chain and is completed exactly once when the write finally
     * succeeds or all retries are exhausted. Bounded retry means a transient failure (a brief
     * connection blip or a command timeout) is retried instead of immediately failing the whole
     * Flink job, while at-least-once is preserved: once retries are exhausted the error is recorded
     * and later re-thrown from {@link #checkAsyncError()} so Flink restarts from the last
     * checkpoint.
     */
    private void attempt(
            PendingRecord record, int attemptsRemaining, CompletableFuture<Void> completion) {
        CompletionStage<?> writeStage;
        RedisCommandsContainer commandContainer = this.redisCommandsContainer;
        try {
            writeStage =
                    record.kind == RowKind.DELETE
                            ? rowKindDelete(commandContainer, record.params)
                            : sink(commandContainer, record.params);
        } catch (Exception e) {
            // Synchronous failure issuing the command (e.g. connection already closed, or a
            // non-retryable UnsupportedOperationException from an unsupported command).
            if (attemptsRemaining > 0 && !(e instanceof UnsupportedOperationException)) {
                logRetryableFailure(
                        "sink redis error when issuing command",
                        keyOf(record),
                        attemptsRemaining,
                        e);
                scheduleRetry(
                        keyOf(record),
                        commandContainer,
                        e,
                        attemptsRemaining,
                        () -> attempt(record, attemptsRemaining - 1, completion),
                        completion);
            } else {
                recordAsyncFailure(keyOf(record), e);
                completion.complete(null);
            }
            return;
        }

        if (writeStage == null) {
            // No command was issued (e.g. a no-op); nothing to wait for.
            completion.complete(null);
            return;
        }

        writeStage.whenComplete(
                (r, t) -> {
                    if (t == null) {
                        // Write succeeded: now apply TTL as part of the same logical write. The
                        // completion promise is resolved only after the TTL command finishes (or is
                        // skipped), and a TTL failure is retried / surfaced just like a write
                        // failure, so "written but not expired" data cannot slip past a checkpoint.
                        attemptTtl(record, this.maxRetryTimes, completion);
                        return;
                    }

                    if (attemptsRemaining > 0) {
                        // Transient async failure: re-issue the whole write. The permit stays held
                        // (the promise remains pending) until the retry chain resolves.
                        logRetryableFailure(
                                "Async Redis write failed",
                                keyOf(record),
                                attemptsRemaining,
                                (Throwable) t);
                        scheduleRetry(
                                keyOf(record),
                                commandContainer,
                                (Throwable) t,
                                attemptsRemaining,
                                () -> attempt(record, attemptsRemaining - 1, completion),
                                completion);
                    } else {
                        // Retries exhausted: record the error (surfaced by checkAsyncError) and
                        // resolve the promise.
                        recordAsyncFailure(keyOf(record), (Throwable) t);
                        completion.complete(null);
                    }
                });
    }

    private void scheduleRetry(
            String key,
            RedisCommandsContainer failedContainer,
            Throwable failure,
            int attemptsRemaining,
            Runnable retryAction,
            CompletableFuture<Void> completion) {
        long backoffMs = retryBackoffMs(attemptsRemaining);
        try {
            retryExecutor.schedule(
                    () -> {
                        try {
                            if (shouldRecreateConnection(failure)) {
                                reconnectRedisCommandsContainerIfCurrent(
                                        failedContainer, key, failure);
                            }
                            retryAction.run();
                        } catch (Throwable e) {
                            recordAsyncFailure(key, e);
                            completion.complete(null);
                        }
                    },
                    backoffMs,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            recordAsyncFailure(key, e);
            completion.complete(null);
        }
    }

    private long retryBackoffMs(int attemptsRemaining) {
        int retryIndex = Math.max(0, maxRetryTimes - attemptsRemaining);
        long backoffMs = RETRY_BACKOFF_BASE_MS << Math.min(retryIndex, 10);
        return Math.min(RETRY_BACKOFF_MAX_MS, backoffMs);
    }

    private void reconnectRedisCommandsContainerIfCurrent(
            RedisCommandsContainer failedContainer, String key, Throwable failure)
            throws Exception {
        synchronized (redisCommandsContainerLock) {
            if (this.redisCommandsContainer != failedContainer) {
                return;
            }

            LOG.warn(
                    "Rebuilding Redis connection before retrying key: {} after failure: {}",
                    key,
                    failure.toString());
            RedisCommandsContainer replacement = createRedisCommandsContainer();
            this.redisCommandsContainer = replacement;
            closeQuietly(failedContainer);
        }
    }

    private void closeQuietly(RedisCommandsContainer container) {
        if (container == null) {
            return;
        }
        try {
            container.close();
        } catch (Exception e) {
            LOG.warn("Error closing stale Redis connection", e);
        }
    }

    private boolean shouldRecreateConnection(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof RedisConnectionException
                    || current instanceof RedisCommandTimeoutException) {
                return true;
            }

            String message = current.getMessage();
            if (message != null) {
                String lowerCaseMessage = message.toLowerCase(Locale.ROOT);
                if (lowerCaseMessage.contains("connection closed")
                        || lowerCaseMessage.contains("connection reset")
                        || lowerCaseMessage.contains("connection refused")
                        || lowerCaseMessage.contains("connection timed out")
                        || lowerCaseMessage.contains("unable to connect")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private void logRetryableFailure(
            String message, String key, int attemptsRemaining, Throwable failure) {
        if (shouldRecreateConnection(failure)) {
            LOG.warn(
                    "{} for key: {}, retrying (remaining={}): {}",
                    message,
                    key,
                    attemptsRemaining,
                    failure.toString());
        } else {
            LOG.warn("{} for key: {}, retrying (remaining={})", message, key, attemptsRemaining, failure);
        }
    }

    private static String keyOf(PendingRecord record) {
        return record.params != null && record.params.length > 0 ? record.params[0] : "<unknown>";
    }

    /**
     * Applies the TTL for a record and, on failure, retries only the TTL (not the whole write, so
     * non-idempotent write commands are not re-applied) up to {@code attemptsRemaining} times. The
     * shared {@code completion} promise is resolved only after the TTL command finishes or is
     * skipped; a terminal TTL failure is recorded in {@link #asyncError}. This keeps TTL part of
     * the per-record completion/retry/error tracking so a checkpoint cannot succeed with a written
     * but not-yet-expired key.
     */
    private void attemptTtl(
            PendingRecord record, int attemptsRemaining, CompletableFuture<Void> completion) {
        final String key = keyOf(record);
        CompletionStage<?> ttlStage;
        RedisCommandsContainer commandContainer = this.redisCommandsContainer;
        try {
            ttlStage = issueTtl(commandContainer, key);
        } catch (Exception e) {
            if (attemptsRemaining > 0) {
                logRetryableFailure("set TTL failed", key, attemptsRemaining, e);
                scheduleRetry(
                        key,
                        commandContainer,
                        e,
                        attemptsRemaining,
                        () -> attemptTtl(record, attemptsRemaining - 1, completion),
                        completion);
            } else {
                recordAsyncFailure(key, e);
                completion.complete(null);
            }
            return;
        }

        if (ttlStage == null) {
            // No TTL configured: the write is the whole logical operation.
            completion.complete(null);
            return;
        }

        ttlStage.whenComplete(
                (r, t) -> {
                    if (t == null) {
                        completion.complete(null);
                    } else if (attemptsRemaining > 0) {
                        logRetryableFailure(
                                "set TTL failed", key, attemptsRemaining, (Throwable) t);
                        scheduleRetry(
                                key,
                                commandContainer,
                                (Throwable) t,
                                attemptsRemaining,
                                () -> attemptTtl(record, attemptsRemaining - 1, completion),
                                completion);
                    } else {
                        recordAsyncFailure(key, (Throwable) t);
                        completion.complete(null);
                    }
                });
    }

    /** Records an async failure so it is later propagated to Flink via {@link #checkAsyncError()}. */
    private void recordAsyncFailure(String key, Throwable t) {
        if (asyncError.compareAndSet(null, t)) {
            LOG.error("Async Redis write failed for key: {}", key, t);
        } else {
            LOG.warn("Additional async Redis write failure for key: {}: {}", key, t.toString());
        }
        if (numRecordsSendErrorsCounter != null) {
            numRecordsSendErrorsCounter.inc();
        }
    }

    /**
     * Composes a conditional ("if absent") write: runs {@code existsCheck} and, only if {@code
     * writeWhen} accepts its result, issues the actual {@code write}. The returned stage completes
     * when the actual write completes (or immediately if the write is skipped), so the real write —
     * not just the existence check — is covered by the sink's retry, flush and error tracking.
     */
    private CompletionStage<Object> conditionalWrite(
            RedisFuture<?> existsCheck,
            Predicate<Object> writeWhen,
            Supplier<RedisFuture<?>> write) {
        CompletableFuture<Object> result = new CompletableFuture<>();
        existsCheck.whenComplete(
                (val, thr) -> {
                    if (thr != null) {
                        result.completeExceptionally(thr);
                        return;
                    }
                    try {
                        if (writeWhen.test(val)) {
                            write.get()
                                    .whenComplete(
                                            (r, t) -> {
                                                if (t != null) {
                                                    result.completeExceptionally(t);
                                                } else {
                                                    result.complete(r);
                                                }
                                            });
                        } else {
                            // Key already present: nothing to write, treat as success.
                            result.complete(null);
                        }
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                });
        return result;
    }

    /**
     * Issues a ZADD and, when {@code zset.zremrangeby} is configured, chains the range-cleanup
     * command (ZREMRANGEBYSCORE/LEX/RANK) so the returned stage completes only after the cleanup
     * finishes. This folds the cleanup into the record's completion/retry/error tracking instead of
     * firing it and forgetting it, so a checkpoint cannot succeed while the range cleanup is still
     * pending or has failed.
     */
    private CompletionStage<?> zaddWithRangeCleanup(
            RedisCommandsContainer commandsContainer, String[] params) {
        RedisFuture<?> zaddFuture =
                commandsContainer.zadd(params[0], Double.parseDouble(params[1]), params[2]);
        if (zremrangeby == null) {
            return zaddFuture;
        }

        CompletableFuture<Object> result = new CompletableFuture<>();
        zaddFuture.whenComplete(
                (r, thr) -> {
                    if (thr != null) {
                        result.completeExceptionally(thr);
                        return;
                    }
                    try {
                        RedisFuture<?> cleanup = issueZremRange(commandsContainer, params);
                        if (cleanup == null) {
                            // Unrecognized zrem type: nothing to clean up, ZADD already succeeded.
                            result.complete(r);
                            return;
                        }
                        cleanup.whenComplete(
                                (cr, ct) -> {
                                    if (ct != null) {
                                        result.completeExceptionally(ct);
                                    } else {
                                        result.complete(cr);
                                    }
                                });
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                });
        return result;
    }

    /**
     * Issues the configured ZREMRANGEBY* cleanup command, or {@code null} when {@code
     * zset.zremrangeby} is not a recognized type.
     */
    private RedisFuture<?> issueZremRange(
            RedisCommandsContainer commandsContainer, String[] params) {
        if (zremrangeby.equalsIgnoreCase(ZremType.SCORE.name())) {
            Range<Double> range =
                    Range.create(Double.parseDouble(params[3]), Double.parseDouble(params[4]));
            return commandsContainer.zremRangeByScore(params[0], range);
        } else if (zremrangeby.equalsIgnoreCase(ZremType.LEX.name())) {
            Range<String> range = Range.create(params[3], params[4]);
            return commandsContainer.zremRangeByLex(params[0], range);
        } else if (zremrangeby.equalsIgnoreCase(ZremType.RANK.name())) {
            return commandsContainer.zremRangeByRank(
                    params[0], Long.parseLong(params[3]), Long.parseLong(params[4]));
        }
        LOG.warn("Unrecognized zrem type:{}", zremrangeby);
        return null;
    }

    /**
     * Waits for all currently in-flight record completion promises to resolve.
     * Called during flush and close to ensure data consistency at checkpoint boundaries. Promises
     * resolve only after the whole write (including retries and follow-up commands) finishes, so
     * waiting here never trips on an intermediate attempt that has already failed and is being
     * retried.
     */
    private void waitForInFlightCompletions() throws IOException {
        List<CompletableFuture<Void>> snapshot;
        synchronized (inFlightCompletions) {
            snapshot = new ArrayList<>(inFlightCompletions);
        }

        for (CompletableFuture<Void> completion : snapshot) {
            try {
                completion.get(flushWaitTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new IOException("Error waiting for in-flight Redis operation to complete", e);
            }
        }
    }

    /**
     * Checks if any async operation has failed and throws an IOException if so.
     * This propagates async errors back to Flink's processing pipeline.
     */
    private void checkAsyncError() throws IOException {
        Throwable error = asyncError.getAndSet(null);
        if (error != null) {
            throw new IOException("Async Redis write failed", error);
        }
    }

    private CompletionStage sink(RedisCommandsContainer commandsContainer, String[] params) {
        CompletionStage redisFuture = null;
        switch (redisCommand.getInsertCommand()) {
            case RPUSH:
                redisFuture = commandsContainer.rpush(params[0], params[1]);
                break;
            case LPUSH:
                redisFuture = commandsContainer.lpush(params[0], params[1]);
                break;
            case SADD:
                redisFuture = commandsContainer.sadd(params[0], params[1]);
                break;
            case SET: {
                if (!this.setIfAbsent) {
                    redisFuture = commandsContainer.set(params[0], params[1]);
                } else {
                    // Cover the real SET (not just EXISTS) with retry/flush/error tracking by
                    // completing only after the actual write finishes.
                    redisFuture =
                            conditionalWrite(
                                    commandsContainer.exists(params[0]),
                                    val -> ((Number) val).intValue() == 0,
                                    () -> commandsContainer.set(params[0], params[1]));
                }
            }
                break;
            case PFADD:
                redisFuture = commandsContainer.pfadd(params[0], params[1]);
                break;
            case PUBLISH:
                redisFuture = commandsContainer.publish(params[0], params[1]);
                break;
            case ZADD:
                redisFuture = zaddWithRangeCleanup(commandsContainer, params);
                break;
            case ZINCRBY:
                redisFuture =
                        commandsContainer.zincrBy(params[0], Double.valueOf(params[1]), params[2]);
                break;
            case ZREM:
                redisFuture = commandsContainer.zrem(params[0], params[1]);
                break;
            case SREM:
                redisFuture = commandsContainer.srem(params[0], params[1]);
                break;
            case HSET: {
                if (!this.setIfAbsent) {
                    redisFuture = commandsContainer.hset(params[0], params[1], params[2]);
                } else {
                    redisFuture =
                            conditionalWrite(
                                    commandsContainer.hexists(params[0], params[1]),
                                    exist -> Boolean.FALSE.equals(exist),
                                    () -> commandsContainer.hset(params[0], params[1], params[2]));
                }
            }
                break;
            case HMSET: {
                if (params.length < 2) {
                    throw new RuntimeException("params length must be greater than 2");
                }
                if (params.length % 2 != 1) {
                    throw new RuntimeException("params length must be odd");
                }
                Map<String, String> hashField = new HashMap<>();
                for (int i = 1; i < params.length; i++) {
                    hashField.put(params[i], params[++i]);
                }
                if (!this.setIfAbsent) {
                    redisFuture = commandsContainer.hmset(params[0], hashField);
                } else {
                    // EXISTS returns Long (0/1); the previous code cast it to Boolean, which threw
                    // ClassCastException at runtime. Use a numeric check and cover the real HMSET.
                    redisFuture =
                            conditionalWrite(
                                    commandsContainer.exists(params[0]),
                                    val -> ((Number) val).intValue() == 0,
                                    () -> commandsContainer.hmset(params[0], hashField));
                }
            }
                break;
            case HINCRBY:
                redisFuture =
                        commandsContainer.hincrBy(params[0], params[1], Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        commandsContainer.hincrByFloat(
                                params[0], params[1], Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture = commandsContainer.incrBy(params[0], Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        commandsContainer.incrByFloat(params[0], Double.valueOf(params[1]));
                break;
            case DECRBY:
                redisFuture = commandsContainer.decrBy(params[0], Long.valueOf(params[1]));
                break;
            case DEL:
                redisFuture = commandsContainer.del(params[0]);
                break;
            case HDEL:
                redisFuture = commandsContainer.hdel(params[0], params[1]);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot process such data type: " + redisCommand);
        }
        return redisFuture;
    }

    private RedisFuture rowKindDelete(RedisCommandsContainer commandsContainer, String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getDeleteCommand()) {
            case SREM:
                redisFuture = commandsContainer.srem(params[0], params[1]);
                break;
            case DEL:
                redisFuture = commandsContainer.del(params[0]);
                break;
            case ZREM:
                redisFuture = commandsContainer.zrem(params[0], params[2]);
                break;
            case ZINCRBY:
                Double d = -Double.valueOf(params[1]);
                redisFuture = commandsContainer.zincrBy(params[0], d, params[2]);
                break;
            case HDEL:
                redisFuture = commandsContainer.hdel(params[0], params[1]);
                break;
            case HINCRBY:
                redisFuture =
                        commandsContainer.hincrBy(params[0], params[1], -Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        commandsContainer.hincrByFloat(
                                params[0], params[1], -Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture = commandsContainer.incrBy(params[0], -Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        commandsContainer.incrByFloat(params[0], -Double.valueOf(params[1]));
                break;
            case NONE:
            default:
                throw new UnsupportedOperationException(
                        "Cannot process DELETE row for Redis command "
                                + redisCommand
                                + " because no delete command is defined.");
        }
        return redisFuture;
    }

    /**
     * Issues the TTL command(s) for a key and returns a stage that completes when the actual EXPIRE
     * finishes (or immediately when no EXPIRE is needed), or {@code null} when no TTL is configured.
     * The returned stage lets the caller fold TTL into the record's completion/retry/error
     * tracking instead of firing it and forgetting it.
     */
    private CompletionStage<?> issueTtl(RedisCommandsContainer commandsContainer, String key) {
        if (redisCommand == RedisCommand.DEL) {
            return null;
        }

        if (ttl != null) {
            if (ttlKeyNotAbsent) {
                return expireIfNoTtl(commandsContainer, key, () -> ttl);
            }
            return commandsContainer.expire(key, ttl);
        } else if (expireTimeSeconds != -1) {
            return expireIfNoTtl(
                    commandsContainer,
                    key,
                    () -> {
                        int now = LocalTime.now().toSecondOfDay();
                        return expireTimeSeconds > now
                                ? expireTimeSeconds - now
                                : 86400 + expireTimeSeconds - now;
                    });
        }
        return null;
    }

    /**
     * Sets an expiry on {@code key} only if it currently has no TTL. Returns a stage that completes
     * after the EXPIRE (or immediately when the key already has a TTL / the check fails).
     */
    private CompletionStage<?> expireIfNoTtl(
            RedisCommandsContainer commandsContainer, String key, Supplier<Integer> secondsSupplier) {
        CompletableFuture<Object> result = new CompletableFuture<>();
        commandsContainer
                .getTTL(key)
                .whenComplete(
                        (currentTtl, h) -> {
                            if (h != null) {
                                result.completeExceptionally(h);
                                return;
                            }
                            try {
                                if (currentTtl != null && currentTtl < 0) {
                                    commandsContainer
                                            .expire(key, secondsSupplier.get())
                                            .whenComplete(
                                                    (r, e) -> {
                                                        if (e != null) {
                                                            result.completeExceptionally(e);
                                                        } else {
                                                            result.complete(r);
                                                        }
                                                    });
                                } else {
                                    result.complete(null);
                                }
                            } catch (Throwable e) {
                                result.completeExceptionally(e);
                            }
                        });
        return result;
    }

    private String serializeWholeRow(RowData rowData) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnDataTypes.size(); i++) {
            stringBuilder.append(
                    RedisRowConverter.rowDataToString(
                            columnDataTypes.get(i).getLogicalType(), rowData, i));
            if (i != columnDataTypes.size() - 1) {
                stringBuilder.append(RedisDynamicTableFactory.CACHE_SEPERATOR);
            }
        }
        return stringBuilder.toString();
    }

    private int calcParamNumByCommand(int rowDataNum) {
        if (redisCommand == RedisCommand.DEL) {
            return 1;
        }

        if (redisCommand.getInsertCommand() == RedisInsertCommand.ZADD && zremrangeby != null) {
            return 5;
        } else if (redisCommand.getInsertCommand() == RedisInsertCommand.HSET
                || redisCommand.getInsertCommand() == RedisInsertCommand.ZADD
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBY
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBYFLOAT
                || redisCommand.getInsertCommand() == RedisInsertCommand.ZINCRBY) {
            return 3;
        } else if (redisCommand.getInsertCommand() == RedisInsertCommand.HMSET) {
            return rowDataNum;
        }

        return 2;
    }
}
