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

package org.apache.flink.streaming.connectors.redis.container;

import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSentinelConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.util.StringUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Jeff Zou
 * @date 2024/3/20 17:08
 */
public class RedisClientBuilder {

    /**
     * Initialize the {@link RedisCommandsContainer} based on the instance type.
     *
     * @param flinkConfigBase configuration base
     * @return @throws IllegalArgumentException if Config, ClusterConfig and SentinelConfig are all
     *     null
     */
    public static AbstractRedisClient build(FlinkConfigBase flinkConfigBase) {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        if (flinkConfigBase.getLettuceConfig() != null) {
            if (flinkConfigBase.getLettuceConfig().getNettyIoPoolSize() != null) {
                builder.ioThreadPoolSize(flinkConfigBase.getLettuceConfig().getNettyIoPoolSize());
            }
            if (flinkConfigBase.getLettuceConfig().getNettyEventPoolSize() != null) {
                builder.computationThreadPoolSize(
                        flinkConfigBase.getLettuceConfig().getNettyEventPoolSize());
            }
        }

        ClientResources clientResources = builder.build();

        if (flinkConfigBase instanceof FlinkSingleConfig) {
            return build((FlinkSingleConfig) flinkConfigBase, clientResources);
        } else if (flinkConfigBase instanceof FlinkClusterConfig) {
            return build((FlinkClusterConfig) flinkConfigBase, clientResources);
        } else if (flinkConfigBase instanceof FlinkSentinelConfig) {
            return build((FlinkSentinelConfig) flinkConfigBase, clientResources);
        } else {
            throw new IllegalArgumentException("configuration not found!");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param singleConfig configuration for redis
     * @return container for single Redis environment
     * @throws NullPointerException if singleConfig is null
     */
    private static RedisClient build(
            FlinkSingleConfig singleConfig, ClientResources clientResources) {
        Objects.requireNonNull(singleConfig, "Redis config should not be Null");

        RedisURI.Builder builder =
                RedisURI.builder()
                        .withHost(singleConfig.getHost())
                        .withPort(singleConfig.getPort())
                        .withDatabase(singleConfig.getDatabase());
        if (singleConfig.getConnectionTimeout() > 0) {
            builder.withTimeout(Duration.ofMillis(singleConfig.getConnectionTimeout()));
        }
        applyAuthentication(builder, singleConfig.getUsername(), singleConfig.getPassword());

        RedisClient redisClient = RedisClient.create(clientResources, builder.build());
        redisClient.setOptions(buildClientOptions(singleConfig));
        return redisClient;
    }

    /**
     * Builds container for Redis Cluster environment.
     *
     * @param clusterConfig configuration for Cluster
     * @return container for Redis Cluster environment
     * @throws NullPointerException if ClusterConfig is null
     */
    private static RedisClusterClient build(
            FlinkClusterConfig clusterConfig, ClientResources clientResources) {
        Objects.requireNonNull(clusterConfig, "Redis cluster config should not be Null");

        List<RedisURI> redisURIS =
                Arrays.stream(clusterConfig.getNodesInfo().split(","))
                        .map(
                                node -> {
                                    String[] redis = node.split(":");
                                    RedisURI.Builder builder =
                                            RedisURI.builder()
                                                    .withHost(redis[0])
                                                    .withPort(Integer.parseInt(redis[1]));
                                    if (clusterConfig.getConnectionTimeout() > 0) {
                                        builder.withTimeout(
                                                Duration.ofMillis(
                                                        clusterConfig.getConnectionTimeout()));
                                    }
                                    applyAuthentication(
                                            builder,
                                            clusterConfig.getUsername(),
                                            clusterConfig.getPassword());
                                    return builder.build();
                                })
                        .collect(Collectors.toList());

        RedisClusterClient clusterClient = RedisClusterClient.create(clientResources, redisURIS);

        ClusterTopologyRefreshOptions topologyRefreshOptions =
                ClusterTopologyRefreshOptions.builder()
                        .enableAdaptiveRefreshTrigger(
                                ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
                                ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(10L))
                        .build();

        ClusterClientOptions.Builder clusterOptionsBuilder =
                ClusterClientOptions.builder().topologyRefreshOptions(topologyRefreshOptions);
        applyResilienceOptions(clusterOptionsBuilder, clusterConfig);
        clusterClient.setOptions(clusterOptionsBuilder.build());

        return clusterClient;
    }

    /**
     * @param sentinelConfig
     * @param clientResources
     * @return
     */
    private static RedisClient build(
            FlinkSentinelConfig sentinelConfig, ClientResources clientResources) {
        Objects.requireNonNull(sentinelConfig, "Redis sentinel config should not be Null");

        RedisURI.Builder builder =
                RedisURI.builder()
                        .withSentinelMasterId(sentinelConfig.getMasterName())
                        .withDatabase(sentinelConfig.getDatabase());
        if (sentinelConfig.getConnectionTimeout() > 0) {
            builder.withTimeout(Duration.ofMillis(sentinelConfig.getConnectionTimeout()));
        }

        Arrays.stream(sentinelConfig.getSentinelsInfo().split(","))
                .forEach(
                        node -> {
                            String[] redis = node.split(":");
                            builder.withSentinel(
                                    redis[0],
                                    Integer.parseInt(redis[1]),
                                    sentinelConfig.getSentinelsPassword());
                        });

        // Authentication against the master/replica data nodes (Redis 6.0+ ACL supports username).
        applyAuthentication(builder, sentinelConfig.getUsername(), sentinelConfig.getPassword());

        RedisClient redisClient = RedisClient.create(clientResources, builder.build());
        redisClient.setOptions(buildClientOptions(sentinelConfig));
        return redisClient;
    }

    /**
     * Applies authentication to a {@link RedisURI.Builder}. When a username is provided (Redis 6.0+
     * with ACL enabled) it uses {@code withAuthentication(username, password)}, otherwise it falls
     * back to the legacy password-only {@code withPassword(...)} which authenticates as the default
     * user. When neither username nor password is set, no authentication is applied.
     */
    static void applyAuthentication(
            RedisURI.Builder builder, String username, String password) {
        boolean hasPassword = !StringUtils.isNullOrWhitespaceOnly(password);
        if (!StringUtils.isNullOrWhitespaceOnly(username)) {
            // Lettuce requires a (possibly empty) password when authenticating with a username.
            char[] passwordChars = hasPassword ? password.toCharArray() : new char[0];
            builder.withAuthentication(username, passwordChars);
        } else if (hasPassword) {
            builder.withPassword(password.toCharArray());
        }
    }

    /**
     * Builds {@link ClientOptions} for non-cluster clients with connection resiliency settings:
     * TCP connect timeout, auto-reconnect and a per-command timeout. Without a command timeout, a
     * stalled or half-open connection leaves async command futures pending forever, which starves
     * the sink's in-flight backpressure permits and eventually fails the whole task/job.
     */
    private static ClientOptions buildClientOptions(FlinkConfigBase config) {
        ClientOptions.Builder builder = ClientOptions.builder();
        applyResilienceOptions(builder, config);
        return builder.build();
    }

    /**
     * Applies shared connection resiliency settings to any {@link ClientOptions.Builder} (including
     * {@link ClusterClientOptions.Builder}).
     */
    private static void applyResilienceOptions(
            ClientOptions.Builder builder, FlinkConfigBase config) {
        builder.autoReconnect(true);

        int connectTimeoutMs = config.getConnectionTimeout();
        if (connectTimeoutMs > 0) {
            builder.socketOptions(
                    SocketOptions.builder()
                            .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                            .build());
        }

        Integer commandTimeoutMs =
                config.getLettuceConfig() != null
                        ? config.getLettuceConfig().getCommandTimeoutMs()
                        : null;
        if (commandTimeoutMs != null && commandTimeoutMs > 0) {
            builder.timeoutOptions(
                    TimeoutOptions.enabled(Duration.ofMillis(commandTimeoutMs)));
        }
    }
}
