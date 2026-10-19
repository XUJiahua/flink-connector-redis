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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.table.TimestampProbeSinkFactory.Observation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies on a real Flink 2.2 MiniCluster which window-time reference a Sink V2 writer sees in
 * the production topology (CUMULATE -> LAG OVER -> Calc -> sink): {@code Context#timestamp()} is
 * null and {@code currentWatermark() + 1} is an upper-bound reference of the window time. Also
 * checks that keeping the rowtime column in the sink input makes the timestamp available.
 */
public class EventTimeReferenceTest {

    private static final int ROWS = 600;
    private static final long STEP_MS = 100L;

    private static final String SOURCE_SQL =
            "SELECT k, hk, SUM(amount) AS v, window_time "
                    + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(creationTime), "
                    + "INTERVAL '1' SECOND, INTERVAL '10' SECOND)) "
                    + "GROUP BY k, hk, window_start, window_end, window_time";

    private static final String COMPARE_SQL =
            "SELECT k, hk, v, window_time, "
                    + "LAG(v) OVER (PARTITION BY k, hk ORDER BY window_time) AS previous_value "
                    + "FROM s";

    @BeforeEach
    void clear() {
        TimestampProbeSinkFactory.OBSERVATIONS.clear();
    }

    @Test
    void productionShapeHasNoRecordTimestampAndWatermarkIsUpperBoundReference() throws Exception {
        StreamTableEnvironment tEnv = prepare();
        tEnv.executeSql(
                "CREATE TABLE probe_a (k STRING, hk STRING, hv STRING, wt TIMESTAMP(3)) "
                        + "WITH ('connector'='ts-probe')");
        tEnv.executeSql(
                "INSERT INTO probe_a SELECT k, hk, CAST(v AS STRING), "
                        + "CAST(window_time AS TIMESTAMP(3)) AS wt FROM c "
                        + "WHERE previous_value IS NULL OR v <> previous_value")
                .await();

        List<Observation> obs = new ArrayList<>(TimestampProbeSinkFactory.OBSERVATIONS);
        assertFalse(obs.isEmpty());
        int withWatermark = 0;
        for (Observation o : obs) {
            assertNull(o.timestamp, o.toString());
            if (o.watermark != Long.MIN_VALUE) {
                withWatermark++;
                assertTrue(o.watermark + 1 <= o.windowTimeMs, "upper bound: " + o);
                assertTrue(o.windowTimeMs - (o.watermark + 1) < 10_000, "lower bound: " + o);
            }
        }
        assertTrue(withWatermark > 0, "no row observed a watermark: " + obs);
    }

    @Test
    void keepingRowtimeColumnProvidesRecordTimestamp() throws Exception {
        StreamTableEnvironment tEnv = prepare();
        tEnv.executeSql(
                "CREATE TABLE probe_b (k STRING, hk STRING, hv STRING, wt TIMESTAMP(3)) "
                        + "WITH ('connector'='ts-probe')");
        tEnv.executeSql(
                "INSERT INTO probe_b SELECT k, hk, CAST(v AS STRING), "
                        + "window_time AS wt FROM c "
                        + "WHERE previous_value IS NULL OR v <> previous_value")
                .await();

        List<Observation> obs = new ArrayList<>(TimestampProbeSinkFactory.OBSERVATIONS);
        assertFalse(obs.isEmpty());
        for (Observation o : obs) {
            assertEquals(Long.valueOf(o.windowTimeMs), o.timestamp, o.toString());
        }
    }

    private static StreamTableEnvironment prepare() {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(20));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);

        TypeInformation<Row> typeInfo =
                Types.ROW_NAMED(
                        new String[]{"k", "hk", "amount", "creationTime"},
                        Types.STRING,
                        Types.STRING,
                        Types.LONG,
                        Types.LOCAL_DATE_TIME);
        List<Row> rows = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            long ms = i * STEP_MS;
            rows.add(
                    Row.of(
                            "k",
                            i % 2 == 0 ? "hk0" : "hk1",
                            1L,
                            LocalDateTime.ofEpochSecond(
                                    ms / 1000, (int) (ms % 1000) * 1_000_000, ZoneOffset.UTC)));
        }

        DataStream<Row> ds =
                env.fromData(rows, typeInfo)
                        .map(
                                r -> {
                                    Thread.sleep(5);
                                    return r;
                                })
                        .returns(typeInfo)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Row>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (r, p) -> ((LocalDateTime) r.getField(3))
                                                        .toInstant(ZoneOffset.UTC)
                                                        .toEpochMilli()));

        tEnv.createTemporaryView(
                "src",
                tEnv.fromDataStream(
                        ds,
                        Schema.newBuilder()
                                .column("k", "STRING")
                                .column("hk", "STRING")
                                .column("amount", "BIGINT")
                                .column("creationTime", "TIMESTAMP(3)")
                                .watermark("creationTime", "SOURCE_WATERMARK()")
                                .build()));
        tEnv.createTemporaryView("s", tEnv.sqlQuery(SOURCE_SQL));
        tEnv.createTemporaryView("c", tEnv.sqlQuery(COMPARE_SQL));
        return tEnv;
    }
}
