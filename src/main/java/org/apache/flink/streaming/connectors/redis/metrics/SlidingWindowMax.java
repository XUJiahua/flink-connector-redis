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

import java.util.Arrays;
import java.util.function.LongSupplier;

/**
 * Maximum value over the last 60 seconds, kept in 60 one-second buckets. Reading never resets the
 * value; an empty window reads as 0. Time comes from an injected monotonic nanosecond clock.
 */
final class SlidingWindowMax {

    private static final int BUCKETS = 60;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final long[] bucketSecond = new long[BUCKETS];
    private final long[] bucketMax = new long[BUCKETS];
    private final LongSupplier nanoClock;

    SlidingWindowMax(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        // Explicit "never written" marker, independent of the 0 default.
        Arrays.fill(bucketSecond, Long.MIN_VALUE);
    }

    synchronized void update(long v) {
        long s = Math.floorDiv(nanoClock.getAsLong(), NANOS_PER_SECOND);
        int i = (int) Math.floorMod(s, BUCKETS);
        if (bucketSecond[i] != s) {
            // Bucket expired or never written: overwrite.
            bucketSecond[i] = s;
            bucketMax[i] = v;
        } else if (v > bucketMax[i]) {
            bucketMax[i] = v;
        }
    }

    synchronized long get() {
        long s = Math.floorDiv(nanoClock.getAsLong(), NANOS_PER_SECOND);
        long m = 0;
        for (int i = 0; i < BUCKETS; i++) {
            long b = bucketSecond[i];
            if (b != Long.MIN_VALUE && b > s - BUCKETS && b <= s && bucketMax[i] > m) {
                m = bucketMax[i];
            }
        }
        return m;
    }
}
