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
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds {@link MetricGroup} proxies that record {@code histogram}/{@code gauge} registrations by
 * name. Other methods can be answered by an optional fallback handler.
 */
public final class CapturingMetricGroup {

    /** Answers methods not handled by the capturing logic. */
    public interface Fallback {

        Object invoke(Method method, Object[] args) throws Throwable;
    }

    private final Map<String, Metric> metrics = Collections.synchronizedMap(new LinkedHashMap<>());

    public Map<String, Metric> metrics() {
        return metrics;
    }

    @SuppressWarnings("unchecked")
    public <T> T gaugeValue(String name) {
        Metric m = metrics.get(name);
        if (m == null) {
            throw new AssertionError("metric not registered: " + name);
        }
        return ((Gauge<T>) m).getValue();
    }

    public Histogram histogram(String name) {
        Metric m = metrics.get(name);
        if (m == null) {
            throw new AssertionError("metric not registered: " + name);
        }
        return (Histogram) m;
    }

    public MetricGroup create(Map<String, String> variables) {
        return create(MetricGroup.class, variables, null);
    }

    @SuppressWarnings("unchecked")
    public <G extends MetricGroup> G create(
            Class<G> type, Map<String, String> variables, Fallback fallback) {
        return (G) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "histogram":
                        case "gauge":
                        case "counter":
                        case "meter":
                            if (args != null
                                    && args.length == 2
                                    && args[0] instanceof String
                                    && args[1] instanceof Metric) {
                                metrics.put((String) args[0], (Metric) args[1]);
                                return args[1];
                            }
                            break;
                        case "getAllVariables":
                            return variables;
                        case "toString":
                            return "CapturingMetricGroup";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            break;
                    }
                    if (fallback != null) {
                        return fallback.invoke(method, args);
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }
}
