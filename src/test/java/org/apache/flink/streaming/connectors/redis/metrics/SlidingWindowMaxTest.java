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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link SlidingWindowMax}. */
class SlidingWindowMaxTest {

    private static final long SEC = 1_000_000_000L;

    private final AtomicLong nanos = new AtomicLong();
    private final SlidingWindowMax max = new SlidingWindowMax(nanos::get);

    @Test
    void emptyWindowReadsZeroAtClockZero() {
        nanos.set(0);
        assertEquals(0, max.get());
    }

    @Test
    void negativeClockValues() {
        nanos.set(-5_500_000_000L);
        assertEquals(0, max.get());
        max.update(42);
        assertEquals(42, max.get());
        nanos.addAndGet(59 * SEC);
        assertEquals(42, max.get());
        nanos.addAndGet(SEC);
        assertEquals(0, max.get());
    }

    @Test
    void sameSecondKeepsMax() {
        max.update(3);
        max.update(9);
        max.update(5);
        assertEquals(9, max.get());
    }

    @Test
    void sampleExpiresAfterSixtySeconds() {
        nanos.set(0);
        max.update(100);
        nanos.set(59 * SEC);
        assertEquals(100, max.get());
        nanos.set(59 * SEC + SEC - 1);
        assertEquals(100, max.get());
        nanos.set(60 * SEC);
        assertEquals(0, max.get());
    }

    @Test
    void indexWrapOverwritesInsteadOfMerging() {
        nanos.set(SEC);
        max.update(100);
        nanos.set(61 * SEC);
        max.update(5);
        assertEquals(5, max.get());
    }

    @Test
    void readDoesNotReset() {
        max.update(7);
        assertEquals(7, max.get());
        assertEquals(7, max.get());
    }

    @Test
    void concurrentUpdatesAndReads() throws Exception {
        nanos.set(10 * SEC);
        int threads = 4;
        int perThread = 10_000;
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean writersDone = new AtomicBoolean(false);
        List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            Thread w =
                    new Thread(
                            () -> {
                                try {
                                    start.await();
                                    for (int i = 0; i < perThread; i++) {
                                        max.update(base + i);
                                    }
                                } catch (Throwable e) {
                                    failure.compareAndSet(null, e);
                                }
                            });
            writers.add(w);
            w.start();
        }
        Thread reader =
                new Thread(
                        () -> {
                            try {
                                start.await();
                                while (!writersDone.get()) {
                                    max.get();
                                }
                            } catch (Throwable e) {
                                failure.compareAndSet(null, e);
                            }
                        });
        reader.start();
        start.countDown();
        for (Thread w : writers) {
            w.join();
        }
        writersDone.set(true);
        reader.join();
        assertNull(failure.get());
        assertEquals(threads * perThread - 1, max.get());
    }
}
