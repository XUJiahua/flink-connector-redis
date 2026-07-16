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

package org.apache.flink.streaming.connectors.redis.config;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests verifying that the {@code username} option (Redis 6.0+ ACL) is propagated from the
 * table options into the generated {@link FlinkConfigBase} for single, cluster and sentinel modes.
 */
public class FlinkConfigHandlerUsernameTest {

    @Test
    public void testSingleConfigCarriesUsernameAndPassword() {
        Configuration config = new Configuration();
        config.set(RedisOptions.HOST, "127.0.0.1");
        config.set(RedisOptions.USERNAME, "acl-user");
        config.set(RedisOptions.PASSWORD, "secret");

        FlinkConfigBase result = new FlinkSingleConfigHandler().createFlinkConfig(config);

        assertEquals("acl-user", result.getUsername());
        assertEquals("secret", result.getPassword());
    }

    @Test
    public void testSingleConfigUsernameDefaultsToNull() {
        Configuration config = new Configuration();
        config.set(RedisOptions.HOST, "127.0.0.1");
        config.set(RedisOptions.PASSWORD, "secret");

        FlinkConfigBase result = new FlinkSingleConfigHandler().createFlinkConfig(config);

        assertNull(result.getUsername());
        assertEquals("secret", result.getPassword());
    }

    @Test
    public void testClusterConfigCarriesUsername() {
        Configuration config = new Configuration();
        config.set(RedisOptions.CLUSTERNODES, "127.0.0.1:7000,127.0.0.1:7001");
        config.set(RedisOptions.USERNAME, "acl-user");
        config.set(RedisOptions.PASSWORD, "secret");

        FlinkConfigBase result = new FlinkClusterConfigHandler().createFlinkConfig(config);

        assertEquals("acl-user", result.getUsername());
        assertEquals("secret", result.getPassword());
    }

    @Test
    public void testSentinelConfigCarriesUsername() {
        Configuration config = new Configuration();
        config.set(RedisOptions.REDIS_MASTER_NAME, "mymaster");
        config.set(RedisOptions.SENTINELS_INFO, "127.0.0.1:26379,127.0.0.1:26380");
        config.set(RedisOptions.USERNAME, "acl-user");
        config.set(RedisOptions.PASSWORD, "secret");
        config.set(RedisOptions.SENTINELS_PASSWORD, "sentinel-secret");

        FlinkSentinelConfig result =
                (FlinkSentinelConfig) new FlinkSentinelConfigHandler().createFlinkConfig(config);

        assertEquals("acl-user", result.getUsername());
        assertEquals("secret", result.getPassword());
        assertEquals("sentinel-secret", result.getSentinelsPassword());
    }
}
