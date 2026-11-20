#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
scratch="$(mktemp -d /tmp/rusta-membership.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
export RUSTA_CLUSTER_PKI="$scratch/pki"
compose=(docker compose --project-directory "$root" -f "$scratch/compose.yaml" -p "$project")
worker=""
cleanup() {
    outcome=$?
    if [ -n "$worker" ]; then kill "$worker" 2>/dev/null || true; fi
    if [ "$outcome" -ne 0 ]; then
        cat "$scratch/traffic.log" 2>/dev/null || true
        "${compose[@]}" logs || true
    fi
    "${compose[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT
scripts/cluster-compose.sh 4 19392 1,2,3 > "$scratch/compose.yaml"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 4
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke,MembershipSmoke}.java
run() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" MembershipSmoke "$@" "$scratch"
}
"${compose[@]}" up -d --build --wait broker1 broker2 broker3
run setup 127.0.0.1:19392
run traffic 127.0.0.1:19392 > "$scratch/traffic.log" 2>&1 &
worker=$!
for ((attempt=0; attempt<100; attempt++)); do
    if [ -f "$scratch/ready" ]; then break; fi
    kill -0 "$worker"
    sleep 0.1
done
test -f "$scratch/ready"
"${compose[@]}" up -d --wait broker4
run expanded 127.0.0.1:19395
touch "$scratch/stop"
wait "$worker"
worker=""
cat "$scratch/traffic.log"
"${compose[@]}" kill -s SIGKILL broker4
"${compose[@]}" up -d --wait broker4
"${compose[@]}" kill -s SIGKILL broker1
run verify 127.0.0.1:19395
