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
import io.lettuce.core.RedisFuture;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
    private transient RedisCommandsContainer redisCommandsContainer;
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

    // --- Write QPS rate limiting ---
    private final long writeQps;
    private final double writeQpsBurstSeconds;
    private final int numParallelSubtasks;
    private transient TokenBucketRateLimiter rateLimiter;

    // --- Cooperative waiting (do not freeze the Flink task/mailbox thread) ---
    private final transient MailboxExecutor mailboxExecutor;
    private static final long MAX_PARK_NANOS = 1_000_000L; // 1ms slices while waiting
    private static final long IN_FLIGHT_ACQUIRE_TIMEOUT_NANOS = 60_000_000_000L; // 60s
    private transient boolean flushing;

    // --- Async error tracking ---
    private transient AtomicReference<Throwable> asyncError;

    // --- In-flight futures for flush ---
    private transient List<RedisFuture<?>> inFlightFutures;

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

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container for sink");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        // Initialize batching and backpressure state
        this.batchBuffer = new ArrayList<>(batchSize);
        this.lastFlushTimeMs = System.currentTimeMillis();
        this.inFlightSemaphore = new Semaphore(maxInFlightRequests);
        this.asyncError = new AtomicReference<>(null);
        this.inFlightFutures = new ArrayList<>();

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
        // Wait for ALL in-flight futures to complete
        waitForInFlightFutures();
        // Check for async errors
        checkAsyncError();
    }

    @Override
    public void close() throws Exception {
        // Flush remaining records before closing
        try {
            flushBuffer();
            waitForInFlightFutures();
        } catch (Exception e) {
            LOG.warn("Error during final flush on close", e);
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
                // QPS). Wait cooperatively so the task/mailbox thread stays responsive to
                // checkpoint barriers instead of being frozen in Thread.sleep.
                if (rateLimiter != null) {
                    cooperativeWait(rateLimiter.reserve(1));
                }

                // Backpressure: acquire a permit, again yielding to the mailbox while waiting so a
                // slow Redis does not freeze the task thread for the whole timeout.
                acquireInFlightPermit();

                // Check for async errors before sending more
                checkAsyncError();

                try {
                    sendRecord(record);
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
                } catch (Exception e) {
                    inFlightSemaphore.release();
                    if (numRecordsSendErrorsCounter != null) {
                        numRecordsSendErrorsCounter.inc();
                    }
                    throw new IOException("Failed to write to Redis", e);
                }
            }

            batchBuffer.clear();
            lastFlushTimeMs = System.currentTimeMillis();
        } finally {
            flushing = false;
        }
    }

    /**
     * Cooperatively waits for the given number of nanoseconds, yielding to the Flink mailbox so
     * that checkpoint-related mail and async completion callbacks keep being processed instead of
     * freezing the task thread inside {@code Thread.sleep}.
     */
    private void cooperativeWait(long waitNanos) throws InterruptedException {
        if (waitNanos <= 0) {
            return;
        }
        long deadline = System.nanoTime() + waitNanos;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            if (!tryYieldMailbox()) {
                LockSupport.parkNanos(Math.min(remaining, MAX_PARK_NANOS));
            }
        }
    }

    /**
     * Acquires a backpressure permit, yielding to the mailbox while waiting so a slow Redis does
     * not freeze the task thread. Times out after {@link #IN_FLIGHT_ACQUIRE_TIMEOUT_NANOS}.
     */
    private void acquireInFlightPermit() throws IOException, InterruptedException {
        if (inFlightSemaphore.tryAcquire()) {
            return;
        }
        long deadline = System.nanoTime() + IN_FLIGHT_ACQUIRE_TIMEOUT_NANOS;
        while (!inFlightSemaphore.tryAcquire()) {
            if (System.nanoTime() > deadline) {
                throw new IOException(
                        "Timeout waiting for available slot to send Redis command. "
                                + "Redis may be overloaded. Consider increasing sink.max-in-flight-requests "
                                + "or reducing throughput.");
            }
            // Surface async failures promptly while waiting.
            checkAsyncError();
            if (!tryYieldMailbox()) {
                LockSupport.parkNanos(MAX_PARK_NANOS);
            }
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
     * Sends a single record to Redis with retry logic and tracks the in-flight future.
     *
     * <p>The backpressure permit acquired by {@link #acquireInFlightPermit()} in {@link
     * #flushBuffer()} is owned by this send: it is released exactly once when the command finally
     * completes (successfully or after all retries are exhausted). On a synchronous failure the
     * permit is released by the caller ({@link #flushBuffer()}), which then propagates the error.
     */
    private void sendRecord(PendingRecord record) throws Exception {
        submitWithRetry(record, this.maxRetryTimes);
    }

    /**
     * Issues the command for a record and, on failure, re-issues it up to {@code attemptsRemaining}
     * more times before surfacing the error. Bounded async retry means a transient async failure
     * (e.g. a brief connection blip or a command timeout) is retried instead of immediately failing
     * the whole Flink job, while still preserving at-least-once semantics: once all retries are
     * exhausted the error is recorded and later re-thrown from {@link #checkAsyncError()} so Flink
     * can restart from the last checkpoint.
     */
    private void submitWithRetry(PendingRecord record, int attemptsRemaining) throws Exception {
        RedisFuture redisFuture;
        try {
            if (record.kind == RowKind.DELETE) {
                redisFuture = rowKindDelete(record.params);
            } else {
                redisFuture = sink(record.params);
            }
        } catch (UnsupportedOperationException e) {
            // Non-retryable configuration error; permit released by flushBuffer's handler.
            throw e;
        } catch (Exception e) {
            // Synchronous failure issuing the command.
            if (attemptsRemaining > 0) {
                LOG.warn(
                        "sink redis error when issuing command, remaining retries:{}",
                        attemptsRemaining,
                        e);
                submitWithRetry(record, attemptsRemaining - 1);
                return;
            }
            throw new RuntimeException("sink redis error ", e);
        }

        if (redisFuture == null) {
            // No future returned, release permit immediately.
            inFlightSemaphore.release();
            return;
        }

        // Track the future for flush completion.
        synchronized (inFlightFutures) {
            inFlightFutures.add(redisFuture);
        }

        final RedisFuture<?> trackedFuture = redisFuture;
        redisFuture.whenComplete(
                (r, t) -> {
                    // Remove from tracked futures first.
                    synchronized (inFlightFutures) {
                        inFlightFutures.remove(trackedFuture);
                    }

                    if (t == null) {
                        // Success: set TTL and release the backpressure permit.
                        setTtl(record.params[0]);
                        inFlightSemaphore.release();
                        return;
                    }

                    if (attemptsRemaining > 0) {
                        // Transient async failure: re-issue instead of failing the whole job. The
                        // permit is held across the retry (released by the new future's callback).
                        LOG.warn(
                                "Async Redis write failed for key: {}, retrying (remaining={})",
                                record.params[0],
                                attemptsRemaining,
                                (Throwable) t);
                        try {
                            submitWithRetry(record, attemptsRemaining - 1);
                        } catch (Exception e) {
                            recordAsyncFailure(record.params[0], e);
                            inFlightSemaphore.release();
                        }
                    } else {
                        // Retries exhausted: record the error (surfaced by checkAsyncError) and
                        // release the permit.
                        recordAsyncFailure(record.params[0], (Throwable) t);
                        inFlightSemaphore.release();
                    }
                });
    }

    /** Records an async failure so it is later propagated to Flink via {@link #checkAsyncError()}. */
    private void recordAsyncFailure(String key, Throwable t) {
        LOG.error("Async Redis write failed for key: {}", key, t);
        asyncError.compareAndSet(null, t);
        if (numRecordsSendErrorsCounter != null) {
            numRecordsSendErrorsCounter.inc();
        }
    }

    /**
     * Waits for all currently in-flight futures to complete.
     * Called during flush and close to ensure data consistency at checkpoint boundaries.
     */
    private void waitForInFlightFutures() throws IOException {
        List<RedisFuture<?>> snapshot;
        synchronized (inFlightFutures) {
            snapshot = new ArrayList<>(inFlightFutures);
        }

        for (RedisFuture<?> future : snapshot) {
            try {
                future.get(60, TimeUnit.SECONDS);
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

    private RedisFuture sink(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getInsertCommand()) {
            case RPUSH:
                redisFuture = this.redisCommandsContainer.rpush(params[0], params[1]);
                break;
            case LPUSH:
                redisFuture = this.redisCommandsContainer.lpush(params[0], params[1]);
                break;
            case SADD:
                redisFuture = this.redisCommandsContainer.sadd(params[0], params[1]);
                break;
            case SET: {
                if (!this.setIfAbsent) {
                    redisFuture = this.redisCommandsContainer.set(params[0], params[1]);
                } else {
                    redisFuture = this.redisCommandsContainer.exists(params[0]);
                    redisFuture.whenComplete(
                            (existsVal, throwable) -> {
                                if ((int) existsVal == 0) {
                                    this.redisCommandsContainer.set(params[0], params[1]);
                                }
                            });
                }
            }
                break;
            case PFADD:
                redisFuture = this.redisCommandsContainer.pfadd(params[0], params[1]);
                break;
            case PUBLISH:
                redisFuture = this.redisCommandsContainer.publish(params[0], params[1]);
                break;
            case ZADD:
                redisFuture =
                        this.redisCommandsContainer.zadd(
                                params[0], Double.parseDouble(params[1]), params[2]);
                if (zremrangeby != null) {
                    redisFuture.whenComplete(
                            (ignore, throwable) -> {
                                try {
                                    if (zremrangeby.equalsIgnoreCase(ZremType.SCORE.name())) {
                                        Range<Double> range =
                                                Range.create(
                                                        Double.parseDouble(params[3]),
                                                        Double.parseDouble(params[4]));
                                        this.redisCommandsContainer.zremRangeByScore(
                                                params[0], range);
                                    } else if (zremrangeby.equalsIgnoreCase(ZremType.LEX.name())) {
                                        Range<String> range = Range.create(params[3], params[4]);
                                        this.redisCommandsContainer.zremRangeByLex(
                                                params[0], range);
                                    } else if (zremrangeby.equalsIgnoreCase(ZremType.RANK.name())) {
                                        this.redisCommandsContainer.zremRangeByRank(
                                                params[0],
                                                Long.parseLong(params[3]),
                                                Long.parseLong(params[4]));
                                    } else {
                                        LOG.warn("Unrecognized zrem type:{}", zremrangeby);
                                    }
                                } catch (Exception e) {
                                    LOG.error("{} zremRangeBy failed.", params[0], e);
                                }
                            });
                }
                break;
            case ZINCRBY:
                redisFuture =
                        this.redisCommandsContainer.zincrBy(
                                params[0], Double.valueOf(params[1]), params[2]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[1]);
                break;
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case HSET: {
                if (!this.setIfAbsent) {
                    redisFuture =
                            this.redisCommandsContainer.hset(params[0], params[1], params[2]);
                } else {
                    redisFuture = this.redisCommandsContainer.hexists(params[0], params[1]);
                    redisFuture.whenComplete(
                            (exist, throwable) -> {
                                if (!(Boolean) exist) {
                                    this.redisCommandsContainer.hset(
                                            params[0], params[1], params[2]);
                                }
                            });
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
                    redisFuture = this.redisCommandsContainer.hmset(params[0], hashField);
                } else {
                    redisFuture = this.redisCommandsContainer.exists(params[0]);
                    redisFuture.whenComplete(
                            (exist, throwable) -> {
                                if (!(Boolean) exist) {
                                    this.redisCommandsContainer.hmset(params[0], hashField);
                                }
                            });
                }
            }
                break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], Double.valueOf(params[1]));
                break;
            case DECRBY:
                redisFuture =
                        this.redisCommandsContainer.decrBy(params[0], Long.valueOf(params[1]));
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case HDEL:
                redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot process such data type: " + redisCommand);
        }
        return redisFuture;
    }

    private RedisFuture rowKindDelete(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getDeleteCommand()) {
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[2]);
                break;
            case ZINCRBY:
                Double d = -Double.valueOf(params[1]);
                redisFuture = this.redisCommandsContainer.zincrBy(params[0], d, params[2]);
                break;
            case HDEL:
                redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], -Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], -Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], -Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], -Double.valueOf(params[1]));
                break;
        }
        return redisFuture;
    }

    private void setTtl(String key) {
        if (redisCommand == RedisCommand.DEL) {
            return;
        }

        if (ttl != null) {
            if (ttlKeyNotAbsent) {
                this.redisCommandsContainer
                        .getTTL(key)
                        .whenComplete(
                                (t, h) -> {
                                    if (t < 0) {
                                        this.redisCommandsContainer.expire(key, ttl);
                                    }
                                });
            } else {
                this.redisCommandsContainer.expire(key, ttl);
            }
        } else if (expireTimeSeconds != -1) {
            this.redisCommandsContainer
                    .getTTL(key)
                    .whenComplete(
                            (t, h) -> {
                                if (t < 0) {
                                    int now = LocalTime.now().toSecondOfDay();
                                    this.redisCommandsContainer.expire(
                                            key,
                                            expireTimeSeconds > now
                                                    ? expireTimeSeconds - now
                                                    : 86400 + expireTimeSeconds - now);
                                }
                            });
        }
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
