#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

scratch="$(mktemp -d /tmp/rusta-migration.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
export RUSTA_PORT=19592
export RUSTA_PORT_1=19593 RUSTA_PORT_2=19594 RUSTA_PORT_3=19595
export RUSTA_CLUSTER_SNAPSHOT_INTERVAL=2
export RUSTA_CLUSTER_PKI="$scratch/pki"
single=(docker compose -f compose.yaml -p "$project-source")
cluster=(docker compose -f compose.cluster.yaml -p "$project")

cleanup() {
    outcome=$?
    if [[ "$outcome" -ne 0 ]]; then
        "${single[@]}" logs || true
        "${cluster[@]}" logs || true
    fi
    "${cluster[@]}" down --volumes --remove-orphans
    "${single[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT

scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI"
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke}.java
java_test() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" "$@"
}
import_broker() {
    "${cluster[@]}" run --rm --no-deps -T -v "${project}-source_broker-data:/migration-source" \
        "broker$1" migrate-single-node /migration-source
}

"${single[@]}" up -d --wait --build
java_test Smoke write "127.0.0.1:$RUSTA_PORT" "$scratch/identity"
java_test GroupSmoke write "127.0.0.1:$RUSTA_PORT" 1
if import_broker 1 > "$scratch/active-source" 2>&1; then
    echo "Migration opened a running source" >&2
    exit 1
fi
grep -Fq 'stop the source broker before migration' "$scratch/active-source"
"${single[@]}" stop
for broker in 1 2 3; do import_broker "$broker"; done
import_broker 1
if "${single[@]}" run --no-deps --rm -T broker > "$scratch/fenced-source" 2>&1; then
    echo "Migrated source restarted as a singleton" >&2
    exit 1
fi
grep -Fq 'source volume is fenced' "$scratch/fenced-source"

nodes="127.0.0.1:$RUSTA_PORT_1,127.0.0.1:$RUSTA_PORT_2,127.0.0.1:$RUSTA_PORT_3"
"${cluster[@]}" up -d --wait
java_test Smoke verify-migration "$nodes" "$scratch/identity"
java_test GroupSmoke verify "$nodes"
"${cluster[@]}" exec -T broker1 sh -c '
    test -n "$(find /var/lib/rusta/raft/control -name "snapshot-*" ! -name "*.tmp")"
'
"${cluster[@]}" kill -s SIGKILL
"${cluster[@]}" up -d --wait
java_test Smoke verify-migration "$nodes" "$scratch/identity"
java_test GroupSmoke verify "$nodes"
