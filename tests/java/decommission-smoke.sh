#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
scratch="$(mktemp -d /tmp/rusta-decommission.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
export RUSTA_CLUSTER_PKI="$scratch/pki"
export RUSTA_CLUSTER_SNAPSHOT_INTERVAL=2
compose=(docker compose --project-directory "$root" -f "$scratch/compose.yaml" -f "$scratch/initial-peers.yaml" -p "$project")
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
scripts/cluster-compose.sh 4 19492 1,2,3 > "$scratch/compose.yaml"
initial_peers="$(scripts/cluster-compose.sh 3 19492 1,2,3 | sed -n '/^  RUSTA_CLUSTER_PEERS:/p')"
{
    printf 'services:\n'
    for broker in 1 2 3; do
        printf '  broker%s:\n    environment:\n    %s\n' "$broker" "$initial_peers"
    done
} > "$scratch/initial-peers.yaml"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 3
mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies     "-DoutputDirectory=$scratch/deps"
javac -cp "$scratch/deps/*" -d "$scratch" tests/java/{Smoke,GroupSmoke,DecommissionSmoke}.java
run() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$scratch:$scratch/deps/*" DecommissionSmoke "$1" "127.0.0.1:${2:-19495}" "$scratch"
}
"${compose[@]}" up -d --build --wait broker1 broker2 broker3
run setup 19492
run traffic 19492 > "$scratch/traffic.log" 2>&1 &
worker=$!
for ((attempt=0; attempt<150; attempt++)); do
    if [ -f "$scratch/ready" ]; then break; fi
    kill -0 "$worker"
    sleep 0.1
done
test -f "$scratch/ready"
admin() {
    local broker="$1"
    shift
    "${compose[@]}" exec -T "broker$broker" /usr/local/bin/rusta "$@"
}
inventory() {
    admin "$1" peer-inventory | tr -d '[:space:]'
}
inventory 1 | grep -Fq '"configured_peers":[1,2,3]'
if admin 1 stage-peer '4,raft4:7000,127.0.0.1:19495,broker4:29092'; then
    echo "Peer accepted without a pinned certificate" >&2
    exit 1
fi
cp -R "$RUSTA_CLUSTER_PKI/public" "$scratch/original-pins"
scripts/cluster-certs.sh --add "$RUSTA_CLUSTER_PKI" 4
cp "$RUSTA_CLUSTER_PKI/public/node-4.pem" "$scratch/added-pin"
scripts/cluster-certs.sh --add "$RUSTA_CLUSTER_PKI" 4
cmp "$scratch/added-pin" "$RUSTA_CLUSTER_PKI/public/node-4.pem"
for pin in "$scratch/original-pins/"*.pem; do
    cmp "$pin" "$RUSTA_CLUSTER_PKI/public/${pin##*/}"
done
containers_before="$("${compose[@]}" ps -q broker1 broker2 broker3 | sort)"
holdout=3
if [[ "$(cat "$scratch/initial-controller")" == 3 ]]; then holdout=2; fi
spec='4,raft4:7000,127.0.0.1:19495,broker4:29092'
for broker in 1 2 3; do
    if [[ "$broker" -eq "$holdout" ]]; then continue; fi
    admin "$broker" stage-peer "$spec"
    admin "$broker" stage-peer "$spec"
    state="$(inventory "$broker")"
    [[ "$state" == *'"configured_peers":[1,2,3,4]'* ]]
    [[ "$state" == *'"bootstrap_voters":[1,2,3]'* ]]
done
if admin 1 stage-peer '4,raft4:7000,127.0.0.1:19496,broker4:29092'; then
    echo "Peer accepted with conflicting endpoints" >&2
    exit 1
fi
"${compose[@]}" up -d --no-deps --wait broker4
run pending 19492
run before-decommission 19492
source="$(cat "$scratch/source")"
admin "$source" decommission
run pending 19492
admin "$holdout" stage-peer "$spec"
test "$containers_before" = "$("${compose[@]}" ps -q broker1 broker2 broker3 | sort)"
run partly-moved 19492
controller="$(cat "$scratch/controller")"
"${compose[@]}" kill -s SIGKILL "broker$controller"
"${compose[@]}" up -d --wait "broker$controller"
run placement
touch "$scratch/stop"
wait "$worker"
worker=""
cat "$scratch/traffic.log"
"${compose[@]}" kill -s SIGKILL "broker$source" broker4
"${compose[@]}" up -d --wait broker4
other=1
if [ "$source" -eq "$other" ]; then other=2; fi
printf '%s\n' "$other" > "$scratch/stopped"
"${compose[@]}" kill -s SIGKILL "broker$other"
run quorum
"${compose[@]}" up -d --wait "broker$source" "broker$other"
run verify
"${compose[@]}" stop
"${compose[@]}" up -d --wait broker1 broker2 broker3 broker4
run verify
inventory 4 | grep -Fq "\"retired\":[$source]"
"${compose[@]}" exec -T broker4 sh -c '
    test -n "$(find /var/lib/rusta/raft/control -name "snapshot-*" ! -name "*.tmp")"
    test -n "$(find /var/lib/rusta/raft -path "*/00000000-0000-0000-0000-000000000000-*/snapshot-*" ! -name "*.tmp")"
'
if admin 4 stage-peer "$source,raft$source:7000,127.0.0.1:$((19491 + source)),broker$source:29092"; then
    echo "Retired peer was accepted" >&2
    exit 1
fi

if admin 4 refresh-peer-pins; then
    echo "Refreshed peer pins while the broker was running" >&2
    exit 1
fi
scripts/cluster-certs.sh "$scratch/new-pki" 4
"${compose[@]}" stop
export RUSTA_CLUSTER_PKI="$scratch/new-pki"
for broker in 1 2 3 4; do
    "${compose[@]}" run -T --rm --no-deps "broker$broker" refresh-peer-pins
done
"${compose[@]}" up -d --force-recreate --wait broker1 broker2 broker3 broker4
run verify
