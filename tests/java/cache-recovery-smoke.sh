#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
scratch="$(mktemp -d /tmp/rusta-auto-rejoin.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
build=--no-build
if [[ -z "${RUSTA_IMAGE:-}" ]]; then
    export RUSTA_IMAGE="rusta:$project"
    build=--build
fi
export RUSTA_CLUSTER_PKI="$scratch/pki"
export RUSTA_PORT_1="${RUSTA_PORT_1:-19452}"
export RUSTA_PORT_2="${RUSTA_PORT_2:-19453}"
export RUSTA_PORT_3="${RUSTA_PORT_3:-19454}"
unset RUSTA_CLUSTER_ACK_MODE
compose=(docker compose -f compose.cluster.yaml -p "$project")
cleanup() {
    result=$?
    if [[ $result -ne 0 ]]; then "${compose[@]}" logs || true; fi
    "${compose[@]}" down --volumes --remove-orphans
    if [[ "$build" == --build ]]; then docker image rm "$RUSTA_IMAGE" >/dev/null 2>&1 || true; fi
    rm -r -- "$scratch"
}
trap cleanup EXIT
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 3
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
deps="$scratch/deps/*"
javac -cp "$deps" -d "$scratch" tests/java/CacheRecoverySmoke.java
nodes="127.0.0.1:$RUSTA_PORT_1,127.0.0.1:$RUSTA_PORT_2,127.0.0.1:$RUSTA_PORT_3"
java_test() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$deps" CacheRecoverySmoke "$1" "$nodes" "${@:2}"
}
volume() {
    local id="$1"
    shift
    docker run --rm -i --user 0:0 --entrypoint sh \
        --mount "type=volume,src=${project}_data$id,dst=/volume" "$RUSTA_IMAGE" -ec "$1"
}
"${compose[@]}" up "$build" -d --wait
java_test create
java_test write 0 16
leader="$(java_test leader)"
follower="$((leader % 3 + 1))"
sleep 2
volume "$follower" 'test -s /volume/page-cache-boot; file=$(find /volume/raft -name manifest.stable); test -f "$file"; cat "$file"' > "$scratch/prefix"
tail_records="${RUSTA_CACHE_RECOVERY_TAIL_RECORDS:-32}"
[[ "$tail_records" =~ ^[1-9][0-9]{0,4}$ ]]
tail_end=$((16 + tail_records))
java_test write 16 "$tail_end"
"${compose[@]}" kill -s SIGKILL "broker$follower"
# Simulate power loss on this disposable volume by retaining an older synced prefix.
volume "$follower" 'file=$(find /volume/raft -name manifest.stable); cat > "$file"; printf corrupt > "${file%.stable}"; printf "%016d" 0 > /volume/page-cache-boot' < "$scratch/prefix"
printf 'benchmark-phase cache_rejoin_started\n'
"${compose[@]}" up --no-build -d --wait "broker$follower"
for attempt in $(seq 1 80); do
    if "${compose[@]}" logs "broker$follower" | rg 'page-cache replica recovery complete'; then break; fi
    sleep 0.25
done
"${compose[@]}" logs "broker$follower" | rg 'page-cache replica recovery complete'
volume "$follower" 'test ! -e /volume/page-cache-recovering'
printf 'benchmark-phase cache_rejoin_completed\n'
leader="$(java_test leader)"
"${compose[@]}" kill -s SIGKILL "broker$leader"
java_test write "$tail_end" "$((tail_end + 16))"
java_test read "$((tail_end + 16))"
"${compose[@]}" up --no-build -d --wait "broker$leader"
"${compose[@]}" kill -s SIGKILL
for id in 1 2 3; do
    volume "$id" 'printf "%016d" 0 > /volume/page-cache-boot'
done
"${compose[@]}" up --no-build -d --wait
# The data election timeout must elapse while every replica stays in recovery.
sleep 2
for id in 1 2 3; do
    volume "$id" 'test -s /volume/page-cache-recovering'
done
java_test no-leader
echo 'RF3 default mode, automatic rejoin, post-rejoin quorum, and no election after loss of all eligible replicas passed'
