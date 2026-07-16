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

import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the authentication branch logic in {@link RedisClientBuilder#applyAuthentication}.
 */
public class RedisClientBuilderAuthTest {

    private static RedisURI build(String username, String password) {
        RedisURI.Builder builder = RedisURI.builder().withHost("127.0.0.1").withPort(6379);
        RedisClientBuilder.applyAuthentication(builder, username, password);
        return builder.build();
    }

    @Test
    public void testUsernameAndPasswordUseAclAuthentication() {
        RedisURI uri = build("acl-user", "secret");

        assertEquals("acl-user", uri.getUsername());
        assertArrayEquals("secret".toCharArray(), uri.getPassword());
    }

    @Test
    public void testPasswordOnlyFallsBackToDefaultUser() {
        RedisURI uri = build(null, "secret");

        assertNull(uri.getUsername());
        assertArrayEquals("secret".toCharArray(), uri.getPassword());
    }

    @Test
    public void testUsernameWithBlankPasswordStillAuthenticates() {
        RedisURI uri = build("acl-user", null);

        assertEquals("acl-user", uri.getUsername());
        assertArrayEquals(new char[0], uri.getPassword());
    }

    @Test
    public void testNoCredentialsAppliesNoAuthentication() {
        RedisURI uri = build(null, null);

        assertNull(uri.getUsername());
        assertNull(uri.getPassword());
    }

    @Test
    public void testBlankUsernameFallsBackToPasswordOnly() {
        RedisURI uri = build("   ", "secret");

        assertNull(uri.getUsername());
        assertArrayEquals("secret".toCharArray(), uri.getPassword());
    }
}
