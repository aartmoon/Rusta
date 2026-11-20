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
run leadership 127.0.0.1:19395
source="$(sed -n '1p' "$scratch/transfer")"
target="$(sed -n '2p' "$scratch/transfer")"
admin() {
    local broker="$1"
    shift
    "${compose[@]}" exec -T "broker$broker" /usr/local/bin/rusta "$@"
}
if admin "$source" transfer-leadership before-node-four 0 4; then
    printf 'Unexpected transfer to a nonvoter
' >&2
    exit 1
fi
# Let the catch-up precondition settle between concurrent Produce operations.
for ((attempt=0; attempt<80; attempt++)); do
    if admin "$source" transfer-leadership before-node-four 0 "$target"; then break; fi
    sleep 0.1
done
run transferred 127.0.0.1:19395

"${compose[@]}" kill -s SIGKILL broker4
run controller 127.0.0.1:19392
controller="$(cat "$scratch/controller")"
request=6e0c4ca8-c1d9-4a76-9b45-a67badbd582d
admin "$controller" rebalance "$request" | tee "$scratch/accepted"
grep -Fq '"result": "accepted"' "$scratch/accepted"
admin "$controller" rebalance "$request" | tee "$scratch/repeated"
grep -Fq '"result": "accepted"' "$scratch/repeated"
admin "$controller" relocation-status | tee "$scratch/paused"
grep -Fq '"kind": "rebalance"' "$scratch/paused"
grep -Fq '"to": 4' "$scratch/paused"

"${compose[@]}" kill -s SIGKILL "broker$controller"
"${compose[@]}" up -d --wait broker4
run controller 127.0.0.1:19395
replacement="$(cat "$scratch/controller")"
test "$replacement" -ne "$controller"
"${compose[@]}" up -d --wait "broker$controller"
for ((attempt=0; attempt<150; attempt++)); do
    admin 4 relocation-status > "$scratch/status"
    if grep -Fq "\"last_rebalance\": \"$request\"" "$scratch/status"; then break; fi
    kill -0 "$worker"
    sleep 0.2
done
grep -Fq "\"last_rebalance\": \"$request\"" "$scratch/status"
run controller 127.0.0.1:19395
controller="$(cat "$scratch/controller")"
admin "$controller" rebalance "$request" | tee "$scratch/completed"
grep -Fq '"result": "completed"' "$scratch/completed"
admin "$controller" rebalance 5a7a116f-8edc-417d-9755-d139c293f4fc | tee "$scratch/balanced"
grep -Fq '"result": "balanced"' "$scratch/balanced"

touch "$scratch/stop"
wait "$worker"
worker=""
cat "$scratch/traffic.log"
"${compose[@]}" kill -s SIGKILL broker4
"${compose[@]}" up -d --wait broker4
"${compose[@]}" kill -s SIGKILL broker1
run verify 127.0.0.1:19395
