#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Both endpoints are disposable and published only on random loopback ports.
redis_image="${REDIS_VERIFY_IMAGE:-redis:7-alpine}"
redis_primary_id=''
redis_outage_id=''
cleanup() {
    if [[ -n "$redis_primary_id" ]]; then
        docker rm -f "$redis_primary_id" >/dev/null 2>&1 || true
    fi
    if [[ -n "$redis_outage_id" ]]; then
        docker rm -f "$redis_outage_id" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

docker info >/dev/null
redis_primary_id=$(docker run -d -p 127.0.0.1::6379 "$redis_image" redis-server --save '' --appendonly no)
redis_outage_id=$(docker run -d -p 127.0.0.1::6379 "$redis_image" redis-server --save '' --appendonly no)
redis_primary_mapping=$(docker port "$redis_primary_id" 6379/tcp)
redis_outage_mapping=$(docker port "$redis_outage_id" 6379/tcp)
redis_primary_port=${redis_primary_mapping##*:}
redis_outage_port=${redis_outage_mapping##*:}
# Docker can choose a different random published port on restart. Recreate this endpoint
# with an explicit binding so Lettuce reconnects to the same address after the outage.
docker rm -f "$redis_outage_id" >/dev/null
redis_outage_id=$(docker run -d -p "127.0.0.1:$redis_outage_port:6379" "$redis_image" redis-server --save '' --appendonly no)
for redis_endpoint in "$redis_primary_id" "$redis_outage_id"; do
    redis_ready=false
    for attempt in {1..50}; do
        if docker exec "$redis_endpoint" redis-cli ping >/dev/null 2>&1; then
            redis_ready=true
            break
        fi
        sleep 0.1
    done
    if [[ "$redis_ready" != true ]]; then
        printf 'Redis did not become ready: %s\n' "$redis_endpoint" >&2
        exit 1
    fi
done

mkdir -p target
redis_acceptance_log=target/redis-timed-flush-acceptance.log
{
    date -Iseconds
    git rev-parse HEAD
    java -version 2>&1
    docker version --format 'Docker server {{.Server.Version}}'
    docker image inspect "$redis_image" --format 'Redis image {{.Id}}'
    docker exec "$redis_primary_id" redis-server --version
    printf 'Primary Redis: 127.0.0.1:%s; outage Redis: 127.0.0.1:%s\n' "$redis_primary_port" "$redis_outage_port"
} | tee "$redis_acceptance_log"

# Additional Maven arguments (for example -s /path/to/settings.xml) are passed through.
# The outage case deliberately waits for the existing 60-second checkpoint timeout.
mvn -B "-Dredis.it.port=$redis_primary_port" \
    "-Dredis.it.outage.port=$redis_outage_port" \
    "-Dredis.it.outage.container=$redis_outage_id" \
    -Dtest=RedisSinkRealRedisTest,RedisSinkWriterLatencyMetricsTest,RedisSinkTimedFlushIntegrationTest,RedisSinkLatencyMetricsTest,LatencyMetricTest,SlidingWindowMaxTest,TokenBucketRateLimiterTest \
    test "$@" 2>&1 | tee -a "$redis_acceptance_log"
