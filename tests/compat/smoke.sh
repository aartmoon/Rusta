#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
scratch="$(mktemp -d /tmp/rusta-compat.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
export RUSTA_CLUSTER_PKI="$scratch/pki"
export RUSTA_PORT_1=19893 RUSTA_PORT_2=19894 RUSTA_PORT_3=19895
compose=(docker compose -f compose.cluster.yaml -p "$project")
worker=""
cleanup() {
    result=$?
    if [[ -n "$worker" ]]; then kill "$worker" 2>/dev/null || true; fi
    if [[ "$result" -ne 0 ]]; then
        for log in "$scratch"/compat-*.log; do if [[ -f "$log" ]]; then cat "$log"; fi; done
        "${compose[@]}" logs || true
    fi
    "${compose[@]}" down --volumes --remove-orphans
    rm -r -- "$scratch"
}
trap cleanup EXIT
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 3
"${PYTHON:-python3}" -m venv "$scratch/venv"
"$scratch/venv/bin/pip" install --disable-pip-version-check --only-binary=:all: -r tests/compat/requirements.txt
(cd tests/compat/go && go build -mod=readonly -o "$scratch/franz-go" .)
docker run --rm --mount "type=bind,src=$root/tests/compat/rust,dst=/fixture,readonly" \
    --mount "type=bind,src=$scratch,dst=/work" -w /fixture \
    -v rusta-cargo-registry:/usr/local/cargo/registry -v rusta-compat-target:/target \
    rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea bash -ec 'cargo build --locked --target-dir /target
        cp /target/debug/rusta-compat /work/rust-rdkafka'

"${compose[@]}" up -d --build --wait
seeds="127.0.0.1:19893,127.0.0.1:19894,127.0.0.1:19895"
"$scratch/venv/bin/python" tests/compat/wire_probe.py 127.0.0.1:19893

exercise() {
    local topic="$1"
    shift
    "$@" > "$scratch/$topic.log" 2>&1 &
    worker=$!
    for ((attempt=0; attempt<450; attempt++)); do
        if [[ -f "$scratch/$topic.ready" ]]; then break; fi
        kill -0 "$worker"
        sleep 0.1
    done
    test -f "$scratch/$topic.ready"
    local leader
    leader="$("$scratch/venv/bin/python" tests/compat/python_client.py leader "$seeds" "$topic")"
    [[ "$leader" =~ ^[123]$ ]]
    "${compose[@]}" kill -s SIGKILL "broker$leader"
    touch "$scratch/$topic.go"
    wait "$worker"
    worker=""
    cat "$scratch/$topic.log"
    "${compose[@]}" up -d --wait "broker$leader"
}

exercise compat-franz "$scratch/franz-go" "$seeds" compat-franz "$scratch"
exercise compat-python "$scratch/venv/bin/python" tests/compat/python_client.py "$seeds" compat-python "$scratch"
exercise compat-rust docker run --rm --network "${project}_default" \
    --mount "type=bind,src=$scratch,dst=/work" rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea \
    /work/rust-rdkafka broker1:29092,broker2:29092,broker3:29092 compat-rust /work
