#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
export RUSTA_PORT_1="${RUSTA_PORT_1:-19192}"
export RUSTA_PORT_2="${RUSTA_PORT_2:-19193}"
export RUSTA_PORT_3="${RUSTA_PORT_3:-19194}"
scratch="$(mktemp -d /tmp/rusta-cluster.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
export RUSTA_CLUSTER_PKI="$scratch/pki"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
compose=(docker compose -f compose.cluster.yaml -p "$project")
cleanup() {
    outcome=$?
    if [ "$outcome" -ne 0 ]; then "${compose[@]}" logs || true; fi
    "${compose[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT

scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI"
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/Smoke.java tests/java/GroupSmoke.java tests/java/ClusterSmoke.java tests/java/ClusterDataPathSmoke.java
nodes="127.0.0.1:$RUSTA_PORT_1,127.0.0.1:$RUSTA_PORT_2,127.0.0.1:$RUSTA_PORT_3"
java_test() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" ClusterSmoke "$1" "$2" "$scratch" "$nodes"
}
"${compose[@]}" up -d --wait --build
java_test write "$nodes"
first="$(cat "$scratch/leader")"
"${compose[@]}" kill -s SIGKILL "broker$first"
java_test failover "$nodes"
second="$(cat "$scratch/leader")"
"${compose[@]}" up -d --wait "broker$first"
"${compose[@]}" kill -s SIGKILL "broker$second"
java_test rejoined "$nodes"
endpoint() {
    case "$1" in
        1) printf '127.0.0.1:%s' "$RUSTA_PORT_1" ;;
        2) printf '127.0.0.1:%s' "$RUSTA_PORT_2" ;;
        3) printf '127.0.0.1:%s' "$RUSTA_PORT_3" ;;
    esac
}
# Partition the Raft network while the old leader still accepts Kafka connections.
"${compose[@]}" up -d --wait
java_test leader "$nodes"
isolated="$(cat "$scratch/leader")"
container="$("${compose[@]}" ps -q "broker$isolated")"
docker network disconnect "${project}_raft" "$container"
java_test minority "$(endpoint "$isolated")"
majority=""
for id in 1 2 3; do
    if [ "$id" != "$isolated" ]; then
        majority="${majority}${majority:+,}$(endpoint "$id")"
    fi
done
java_test rejoined "$majority"
docker network connect --alias "raft$isolated" "${project}_raft" "$container"
java_test leader "$nodes"

third="$(cat "$scratch/leader")"
for id in 1 2 3; do
    if [ "$id" != "$third" ]; then
        "${compose[@]}" kill -s SIGKILL "broker$id"
    fi
done
java_test minority "$(endpoint "$third")"


data_test() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" ClusterDataPathSmoke "$1" "$2" "$scratch"
}
"${compose[@]}" up -d --wait
data_test write "$nodes"
retention_leader="$(head -1 "$scratch/retention-leader")"
"${compose[@]}" kill -s SIGKILL "broker$retention_leader"
majority=""
for id in 1 2 3; do
    if [ "$id" != "$retention_leader" ]; then
        majority="${majority}${majority:+,}$(endpoint "$id")"
    fi
done
data_test failover "$majority"
"${compose[@]}" up -d --wait "broker$retention_leader"
"${compose[@]}" kill -s SIGKILL
"${compose[@]}" up -d --wait
data_test restart "$nodes"
