#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
scratch="$(mktemp -d /tmp/rusta-topology.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
export RUSTA_CLUSTER_PKI="$scratch/pki"
compose=(docker compose --project-directory "$root" -f "$scratch/compose.yaml" -p "$project")
cleanup() {
    outcome=$?
    if [ "$outcome" -ne 0 ]; then "${compose[@]}" logs || true; fi
    "${compose[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT
scripts/cluster-compose.sh 5 19292 > "$scratch/compose.yaml"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 5
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke,TopologySmoke}.java
run() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" TopologySmoke "$@"
}
two="127.0.0.1:19292,127.0.0.1:19293"
three="$two,127.0.0.1:19294"
"${compose[@]}" up -d --wait --build
run setup "$three,127.0.0.1:19295,127.0.0.1:19296" 127.0.0.1:19296
"${compose[@]}" kill -s SIGKILL broker4 broker5
run majority "$three"
"${compose[@]}" kill -s SIGKILL broker3
run independent "$two" 127.0.0.1:19292
