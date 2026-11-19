#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
export RUSTA_PORT="${RUSTA_PORT:-19092}"
scratch="$(mktemp -d /tmp/rusta-smoke.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="rusta-smoke-$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
compose=(docker compose -p "$project")
cleanup() {
    outcome=$?
    if [ "$outcome" -ne 0 ]; then "${compose[@]}" logs broker; fi
    "${compose[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT

mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/Smoke.java

"${compose[@]}" up -d --wait --build
broker="$("${compose[@]}" ps -q broker)"
docker exec "$broker" sh -c 'awk "/^(VmRSS|VmHWM):/" /proc/1/status; wc -c /usr/local/bin/rusta'
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$scratch:$scratch/deps/*" \
    Smoke write "127.0.0.1:$RUSTA_PORT" "$scratch/identity"

"${compose[@]}" kill -s SIGKILL broker
"${compose[@]}" up -d --wait --force-recreate
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$scratch:$scratch/deps/*" \
    Smoke verify "127.0.0.1:$RUSTA_PORT" "$scratch/identity"
docker run --rm --network "${project}_default" --mount "type=bind,src=$scratch,dst=/smoke,readonly" \
    eclipse-temurin:21-jdk-noble java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp '/smoke:/smoke/deps/*' Smoke internal broker:29092
