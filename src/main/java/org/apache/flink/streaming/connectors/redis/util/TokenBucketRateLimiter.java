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

package org.apache.flink.streaming.connectors.redis.util;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * A lightweight, dependency-free token-bucket rate limiter used to throttle the rate at which
 * commands are issued to Redis (write QPS).
 *
 * <p>Tokens are replenished continuously at {@code permitsPerSecond}. Each {@link #acquire()}
 * consumes one token, blocking (sleeping) when not enough tokens are available so that the
 * long-run issue rate converges to the configured rate. A small burst capacity is allowed so that
 * mini-batch flushes are not artificially serialized.
 *
 * <p>This class is intended to be used per sink subtask. It is thread-safe via synchronization,
 * although in the Redis sink it is only accessed from the single task thread.
 */
public class TokenBucketRateLimiter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** Sustained refill rate in permits per second. */
    private final double permitsPerSecond;

    /** Maximum number of permits that can be accumulated (burst capacity). */
    private final double maxBurstPermits;

    /** Permits currently available for immediate consumption. */
    private double storedPermits;

    /**
     * The earliest wall-clock time (in nanos) at which a freshly-requested permit becomes
     * available. Pushing this value into the future "pre-spends" permits and yields a correct
     * sustained rate without double counting.
     */
    private long nextFreeTicketNanos;

    /**
     * @param permitsPerSecond the target rate (must be &gt; 0)
     * @param maxBurstSeconds how many seconds worth of permits may be accumulated as burst
     *     capacity (e.g. {@code 1.0} allows a one-second burst)
     */
    public TokenBucketRateLimiter(double permitsPerSecond, double maxBurstSeconds) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        this.permitsPerSecond = permitsPerSecond;
        this.maxBurstPermits = Math.max(1.0, permitsPerSecond * Math.max(0.0, maxBurstSeconds));
        this.storedPermits = 0.0;
        this.nextFreeTicketNanos = System.nanoTime();
    }

    /** Acquires a single permit, blocking until it is available. */
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    /** Acquires {@code permits} permits, blocking until they are available. */
    public void acquire(int permits) throws InterruptedException {
        long waitNanos = reserve(permits);
        if (waitNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(waitNanos);
        }
    }

    /**
     * Reserves {@code permits} permits and returns the number of nanoseconds the caller must wait
     * before the permits become available, <b>without sleeping</b>. This lets the caller perform a
     * cooperative wait (e.g. yielding to a Flink mailbox) instead of blocking a thread.
     *
     * @return nanoseconds to wait (0 if permits are immediately available)
     */
    public synchronized long reserve(int permits) {
        if (permits <= 0) {
            return 0L;
        }
        long now = System.nanoTime();
        refill(now);

        double consumeFromStored = Math.min(permits, storedPermits);
        double freshPermitsNeeded = permits - consumeFromStored;
        storedPermits -= consumeFromStored;

        long waitNanos = (long) ((freshPermitsNeeded / permitsPerSecond) * NANOS_PER_SECOND);
        // Pre-spend the fresh permits by moving the next-free-ticket marker forward.
        nextFreeTicketNanos = now + waitNanos;
        return waitNanos;
    }

    /** Replenishes stored permits based on the time elapsed since the last refill. */
    private void refill(long now) {
        if (now > nextFreeTicketNanos) {
            double newPermits = (now - nextFreeTicketNanos) / (double) NANOS_PER_SECOND * permitsPerSecond;
            storedPermits = Math.min(maxBurstPermits, storedPermits + newPermits);
            nextFreeTicketNanos = now;
        }
    }

    public double getPermitsPerSecond() {
        return permitsPerSecond;
    }
}
