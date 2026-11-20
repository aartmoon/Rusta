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
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke,GroupWireSmoke,SpringSmoke}.java

build=--build
if [[ -n "${RUSTA_IMAGE:-}" ]]; then build=--no-build; fi
"${compose[@]}" up -d --wait "$build"
broker="$("${compose[@]}" ps -q broker)"
broker_image="$(docker inspect --format '{{.Image}}' "$broker")"
test "$(docker exec "$broker" id -u)" = 65532
test "$(docker exec "$broker" id -g)" = 65532
test "$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/var/lib/rusta"}}{{.Type}}{{end}}{{end}}' "$broker")" = volume
docker exec "$broker" sh -c 'awk "/^(VmRSS|VmHWM):/" /proc/1/status; wc -c /usr/local/bin/rusta'
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$scratch:$scratch/deps/*" \
    Smoke write "127.0.0.1:$RUSTA_PORT" "$scratch/identity"

for test_class in GroupSmoke SpringSmoke; do
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$scratch:$scratch/deps/*" \
        "$test_class" write "127.0.0.1:$RUSTA_PORT"
done
"${compose[@]}" kill -s SIGKILL broker
"${compose[@]}" up -d --wait --no-build --force-recreate
broker="$("${compose[@]}" ps -q broker)"
test "$(docker inspect --format '{{.Image}}' "$broker")" = "$broker_image"
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$scratch:$scratch/deps/*" \
    Smoke verify "127.0.0.1:$RUSTA_PORT" "$scratch/identity"
for test_class in GroupSmoke SpringSmoke; do
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$scratch:$scratch/deps/*" \
        "$test_class" verify "127.0.0.1:$RUSTA_PORT"
done
docker run --rm --network "${project}_default" --mount "type=bind,src=$scratch,dst=/smoke,readonly" \
    eclipse-temurin:21-jdk-noble@sha256:bc12ebd88ffa6dda0092940e179365b05aef8d813b867f9df5a1f168c9a44feb java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -cp '/smoke:/smoke/deps/*' Smoke internal broker:29092
