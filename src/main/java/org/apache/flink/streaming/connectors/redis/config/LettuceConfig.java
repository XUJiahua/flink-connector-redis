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

import java.io.Serializable;

public class LettuceConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Integer nettyIoPoolSize;

    private final Integer nettyEventPoolSize;

    /** Per-command timeout in milliseconds; {@code null} or <= 0 disables per-command timeouts. */
    private final Integer commandTimeoutMs;

    public LettuceConfig(Integer nettyIoPoolSize, Integer nettyEventPoolSize) {
        this(nettyIoPoolSize, nettyEventPoolSize, null);
    }

    public LettuceConfig(
            Integer nettyIoPoolSize, Integer nettyEventPoolSize, Integer commandTimeoutMs) {
        this.nettyIoPoolSize = nettyIoPoolSize;
        this.nettyEventPoolSize = nettyEventPoolSize;
        this.commandTimeoutMs = commandTimeoutMs;
    }

    public Integer getNettyIoPoolSize() {
        return nettyIoPoolSize;
    }

    public Integer getNettyEventPoolSize() {
        return nettyEventPoolSize;
    }

    public Integer getCommandTimeoutMs() {
        return commandTimeoutMs;
    }

    @Override
    public String toString() {
        return "LettuceConfig{"
                + "nettyIoPoolSize="
                + nettyIoPoolSize
                + ", nettyEventPoolSize="
                + nettyEventPoolSize
                + ", commandTimeoutMs="
                + commandTimeoutMs
                + '}';
    }
}
