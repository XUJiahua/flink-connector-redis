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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.util.function.LongSupplier;

/**
 * One latency metric exported in three forms: a histogram ({@code <base>_ms}), a 60s sliding
 * window max gauge ({@code <base>_max_ms}) and a last-sample gauge ({@code <base>_last_ms}).
 * All values are milliseconds; negative samples are clamped to 0. Thread-safe.
 */
final class LatencyMetric {

    static final int HISTOGRAM_WINDOW_SIZE = 1024;

    private final DescriptiveStatisticsHistogram histogram;
    private final SlidingWindowMax max;
    private volatile long last;

    LatencyMetric(LongSupplier nanoClock) {
        this.histogram = new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE);
        this.max = new SlidingWindowMax(nanoClock);
    }

    void update(long ms) {
        long v = Math.max(0L, ms);
        histogram.update(v);
        max.update(v);
        last = v;
    }

    void register(MetricGroup g, String base) {
        g.histogram(base + "_ms", histogram);
        g.gauge(base + "_max_ms", (Gauge<Long>) max::get);
        g.gauge(base + "_last_ms", (Gauge<Long>) () -> last);
    }

    Histogram histogram() {
        return histogram;
    }

    long max() {
        return max.get();
    }

    long last() {
        return last;
    }
}
