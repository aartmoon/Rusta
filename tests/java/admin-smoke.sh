#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
scratch="$(mktemp -d /tmp/rusta-admin.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
export RUSTA_PORT=19792
export RUSTA_CLUSTER_SNAPSHOT_INTERVAL=2
export RUSTA_CLUSTER_PKI="$scratch/pki"
single=(docker compose -f compose.yaml -p "$project-single")
cluster=(docker compose --project-directory "$root" -f "$scratch/cluster.yaml" -p "$project")
cleanup() {
    result=$?
    if [[ "$result" -ne 0 ]]; then "${single[@]}" logs || true; "${cluster[@]}" logs || true; fi
    "${single[@]}" down --volumes --remove-orphans
    "${cluster[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 3
scripts/cluster-compose.sh 3 19793 1,2,3 > "$scratch/cluster.yaml"
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke,AdminSmoke}.java
run() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" AdminSmoke "$1" "$2" "$scratch" "$3"
}

"${single[@]}" up -d --build --wait
run write 127.0.0.1:19792 1
"${single[@]}" kill -s SIGKILL
"${single[@]}" up -d --wait
run verify 127.0.0.1:19792 1
deleted="$(cat "$scratch/deleted-directory")"
"${single[@]}" exec -T broker test ! -e "/var/lib/rusta/partitions/$deleted"
"${single[@]}" down --volumes

seeds=127.0.0.1:19793,127.0.0.1:19794,127.0.0.1:19795
"${cluster[@]}" up -d --wait
run write "$seeds" 3
deleted="$(cat "$scratch/deleted-directory")"
# Model an interrupted local cleanup after the controller committed deletion.
"${cluster[@]}" exec -T broker1 sh -ec '
    mkdir -p "$1"
    printf "partial" > "$1/00000000000000000001.log"
' sh "/var/lib/rusta/raft/$deleted-0"
"${cluster[@]}" kill -s SIGKILL
"${cluster[@]}" up -d --wait
run verify "$seeds" 3
for broker in broker1 broker2 broker3; do
    "${cluster[@]}" exec -T "$broker" test ! -e "/var/lib/rusta/raft/$deleted-0"
done
