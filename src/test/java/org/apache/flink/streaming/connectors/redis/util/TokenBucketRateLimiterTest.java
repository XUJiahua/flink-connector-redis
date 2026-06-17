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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link TokenBucketRateLimiter}. */
public class TokenBucketRateLimiterTest {

    @Test
    public void testInvalidRateRejected() {
        assertThrows(
                IllegalArgumentException.class, () -> new TokenBucketRateLimiter(0, 1.0));
        assertThrows(
                IllegalArgumentException.class, () -> new TokenBucketRateLimiter(-10, 1.0));
    }

    @Test
    public void testGetPermitsPerSecond() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 1.0);
        assertEquals(100.0, limiter.getPermitsPerSecond(), 0.0001);
    }

    @Test
    public void testSustainedRateIsThrottled() throws InterruptedException {
        // 50 permits/second, no burst. Acquiring 25 permits one-by-one should take
        // roughly (25 - 1) / 50 = ~0.48s (the first permit is essentially free).
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(50, 0.0);

        long start = System.nanoTime();
        for (int i = 0; i < 25; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        // Expected ~480ms. Allow generous lower bound to avoid flakiness, and an
        // upper bound to ensure throttling actually happened.
        assertTrue(
                elapsedMs >= 350,
                "Expected throttling to take at least 350ms but took " + elapsedMs + "ms");
        assertTrue(
                elapsedMs <= 1500,
                "Expected throttling to finish within 1500ms but took " + elapsedMs + "ms");
    }

    @Test
    public void testBurstAllowsImmediateAcquire() throws InterruptedException {
        // 10 permits/second with 2 seconds of burst => up to ~20 permits can accumulate.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 2.0);

        // Let the bucket accumulate burst tokens.
        Thread.sleep(2200);

        // The accumulated burst should let us acquire ~20 permits almost immediately.
        long start = System.nanoTime();
        for (int i = 0; i < 15; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue(
                elapsedMs < 300,
                "Burst acquire should be fast but took " + elapsedMs + "ms");
    }

    @Test
    public void testHighRateHasLowOverhead() throws InterruptedException {
        // A very high QPS should impose negligible throttling.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1_000_000, 1.0);

        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue(
                elapsedMs < 500,
                "High-rate limiter should add little overhead but took " + elapsedMs + "ms");
    }

    @Test
    public void testReserveDoesNotSleep() {
        // reserve() must return immediately (no blocking) so the caller can wait cooperatively.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 0.0);

        long start = System.nanoTime();
        // Reserve many permits worth several seconds of wait time.
        for (int i = 0; i < 100; i++) {
            limiter.reserve(1);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        // Even though the reserved permits represent ~10s of work, reserve() itself must return
        // almost instantly because it never sleeps.
        assertTrue(
                elapsedMs < 200,
                "reserve() must not block but took " + elapsedMs + "ms");
    }

    @Test
    public void testReserveReturnsZeroWhenPermitsAvailable() throws InterruptedException {
        // storedPermits starts at 0 and burst accumulates over time. After sleeping long enough
        // to accumulate burst tokens, a reserve should be immediately available (0 wait).
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 1.0);

        // Sleep to let burst tokens accumulate (rate=100, burst=1s => ~100 tokens).
        Thread.sleep(300);

        assertEquals(
                0L, limiter.reserve(1), "permit should be free after burst tokens accumulate");
    }

    @Test
    public void testReserveReturnsPositiveWaitWhenThrottled() {
        // 10 permits/second, no burst. After consuming the initial free permit, subsequent
        // reserves must return a positive wait time reflecting the configured rate.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 0.0);

        limiter.reserve(1); // consume the initial free permit
        long waitNanos = limiter.reserve(1);

        // At 10 permits/sec, each fresh permit costs ~100ms = 100_000_000 ns.
        assertTrue(
                waitNanos > 0,
                "throttled reserve must return positive wait, got " + waitNanos);
        // Sanity bound: should be in the right order of magnitude (<= 1s).
        assertTrue(
                waitNanos <= 1_000_000_000L,
                "wait should be on the order of ~100ms, got " + waitNanos + "ns");
    }

    @Test
    public void testReserveNonPositivePermitsReturnsZero() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 0.0);
        assertEquals(0L, limiter.reserve(0));
        assertEquals(0L, limiter.reserve(-5));
    }

    @Test
    public void testReserveSustainedRateAccumulatesWait() {
        // Reserving a burst of permits should accumulate proportionally larger wait times,
        // which is what the cooperative-wait loop relies on to throttle correctly.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100, 0.0);

        limiter.reserve(1); // consume initial free permit
        long waitForOne = limiter.reserve(1);
        long waitForTen = limiter.reserve(10);

        // Reserving 10 permits should require roughly 10x the wait of a single permit.
        assertTrue(
                waitForTen > waitForOne * 5,
                "reserving more permits should require proportionally more wait: one="
                        + waitForOne
                        + " ten="
                        + waitForTen);
    }
}
