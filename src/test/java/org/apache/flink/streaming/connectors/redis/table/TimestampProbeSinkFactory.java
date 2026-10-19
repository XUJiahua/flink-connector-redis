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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test-only {@code ts-probe} sink. Its Sink V2 writer records {@code Context#timestamp()}, {@code
 * Context#currentWatermark()} and the last column (a {@code TIMESTAMP(3)} window time) of every
 * row it receives.
 */
public class TimestampProbeSinkFactory implements DynamicTableSinkFactory {

    /** One observed row. */
    public static final class Observation {

        public final Long timestamp;
        public final long watermark;
        public final long windowTimeMs;

        Observation(Long timestamp, long watermark, long windowTimeMs) {
            this.timestamp = timestamp;
            this.watermark = watermark;
            this.windowTimeMs = windowTimeMs;
        }

        @Override
        public String toString() {
            return "Observation{ts=" + timestamp + ", wm=" + watermark + ", wt=" + windowTimeMs
                    + '}';
        }
    }

    public static final ConcurrentLinkedQueue<Observation> OBSERVATIONS =
            new ConcurrentLinkedQueue<>();

    static long epochMillis(TimestampData t) {
        return t.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public String factoryIdentifier() {
        return "ts-probe";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return new ProbeTableSink();
    }

    private static final class ProbeTableSink implements DynamicTableSink {

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkV2Provider.of(new ProbeSink());
        }

        @Override
        public DynamicTableSink copy() {
            return new ProbeTableSink();
        }

        @Override
        public String asSummaryString() {
            return "ts-probe";
        }
    }

    private static final class ProbeSink implements Sink<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<RowData>() {

                @Override
                public void write(RowData row, Context ctx) {
                    TimestampData wt = row.getTimestamp(row.getArity() - 1, 3);
                    OBSERVATIONS.add(
                            new Observation(ctx.timestamp(), ctx.currentWatermark(), epochMillis(wt)));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
