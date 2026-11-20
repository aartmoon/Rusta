#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
export RUSTA_PORT_1="${RUSTA_PORT_1:-19392}"
export RUSTA_PORT_2="${RUSTA_PORT_2:-19393}"
export RUSTA_PORT_3="${RUSTA_PORT_3:-19394}"
export RUSTA_CLUSTER_SNAPSHOT_INTERVAL=2
scratch="$(mktemp -d /tmp/rusta-groups.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
export RUSTA_CLUSTER_PKI="$scratch/pki"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
compose=(docker compose -f compose.cluster.yaml -p "$project")
spring_pid=""
cleanup() {
    outcome=$?
    if [ -n "$spring_pid" ]; then
        kill "$spring_pid" 2>/dev/null || true
        wait "$spring_pid" 2>/dev/null || true
    fi
    if [ "$outcome" -ne 0 ]; then "${compose[@]}" logs || true; fi
    "${compose[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT

scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI"
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke,GroupWireSmoke,SpringSmoke}.java
nodes="127.0.0.1:$RUSTA_PORT_1,127.0.0.1:$RUSTA_PORT_2,127.0.0.1:$RUSTA_PORT_3"
java_test() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" "$@"
}

"${compose[@]}" up -d --wait --build
java_test GroupSmoke write "$nodes" 3
java_test GroupSmoke verify "$nodes"
java_test SpringSmoke cluster "$nodes" "$scratch" &
spring_pid=$!
for _ in $(seq 1 90); do
    if [ -s "$scratch/spring-coordinator" ]; then break; fi
    kill -0 "$spring_pid"
    sleep 1
done
test -s "$scratch/spring-coordinator"
coordinator="$(cat "$scratch/spring-coordinator")"
"${compose[@]}" kill -s SIGKILL "broker$coordinator"
touch "$scratch/spring-failover"
wait "$spring_pid"
spring_pid=""

"${compose[@]}" up -d --wait "broker$coordinator"
java_test GroupWireSmoke prepare "$nodes" "$scratch"
coordinator="$(cat "$scratch/wire-coordinator")"
ports=("$RUSTA_PORT_1" "$RUSTA_PORT_2" "$RUSTA_PORT_3")
minority="127.0.0.1:${ports[$((coordinator - 1))]}"
majority=""
for id in 1 2 3; do
    if [ "$id" -ne "$coordinator" ]; then
        majority="${majority}${majority:+,}127.0.0.1:${ports[$((id - 1))]}"
    fi
done
container="$("${compose[@]}" ps -q "broker$coordinator")"
docker network disconnect "${project}_raft" "$container"
java_test GroupWireSmoke minority "$minority" "$scratch"
java_test GroupWireSmoke recover "$majority" "$scratch"
docker network connect --alias "raft$coordinator" "${project}_raft" "$container"
"${compose[@]}" kill -s SIGKILL
"${compose[@]}" up -d --wait
java_test GroupSmoke verify "$nodes"
java_test GroupWireSmoke verify "$nodes" "$scratch"
java_test SpringSmoke verify-cluster "$nodes"
docker run --rm --network "${project}_default" --mount "type=bind,src=$scratch,dst=/smoke,readonly" \
    eclipse-temurin:21-jdk-noble@sha256:bc12ebd88ffa6dda0092940e179365b05aef8d813b867f9df5a1f168c9a44feb java -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
    -cp '/smoke:/smoke/deps/*' GroupWireSmoke verify broker1:29092,broker2:29092,broker3:29092 /smoke
