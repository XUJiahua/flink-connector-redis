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

import org.apache.flink.metrics.Histogram;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link LatencyMetric}. */
class LatencyMetricTest {

    private final AtomicLong nanos = new AtomicLong();
    private final LatencyMetric metric = new LatencyMetric(nanos::get);

    @Test
    void negativeIsClampedToZero() {
        metric.update(-5);
        assertEquals(0, metric.last());
        assertEquals(0, metric.max());
        assertEquals(0, metric.histogram().getStatistics().getMin());
        assertEquals(1, metric.histogram().getCount());
    }

    @Test
    void lastTracksLastWriteAndCountTracksUpdates() {
        metric.update(10);
        metric.update(30);
        metric.update(20);
        assertEquals(20, metric.last());
        assertEquals(30, metric.max());
        assertEquals(3, metric.histogram().getCount());
    }

    @Test
    void registersThreeForms() {
        CapturingMetricGroup capture = new CapturingMetricGroup();
        metric.register(capture.create(Collections.emptyMap()), "x");
        assertEquals(
                new HashSet<>(Arrays.asList("x_ms", "x_max_ms", "x_last_ms")),
                capture.metrics().keySet());
        assertTrue(capture.metrics().get("x_ms") instanceof Histogram);
        metric.update(12);
        assertEquals(12L, (long) capture.<Long>gaugeValue("x_max_ms"));
        assertEquals(12L, (long) capture.<Long>gaugeValue("x_last_ms"));
    }
}
