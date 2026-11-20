#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
scratch="$(mktemp -d /tmp/rusta-upgrade.XXXXXXXX)"
scratch="$(cd "$scratch" && pwd -P)"
project="$(basename "$scratch" | tr '[:upper:]_.' '[:lower:]--')"
candidate="${RUSTA_IMAGE:-rusta:$project}"
baseline="rusta:$project-baseline"
export RUSTA_PORT=19792 RUSTA_PORT_1=19793 RUSTA_PORT_2=19794 RUSTA_PORT_3=19795
export RUSTA_CLUSTER_PKI="$scratch/pki"
export RUSTA_CLUSTER_SNAPSHOT_INTERVAL=4
compose=()
cleanup() {
    result=$?
    if [[ ${#compose[@]} -gt 0 ]]; then
        if [[ $result -ne 0 ]]; then "${compose[@]}" logs || true; fi
        "${compose[@]}" down --volumes --remove-orphans || true
    fi
    docker image rm "$baseline" >/dev/null 2>&1 || true
    if [[ -z "${RUSTA_IMAGE:-}" ]]; then docker image rm "$candidate" >/dev/null 2>&1 || true; fi
    rm -r -- "$scratch"
}
trap cleanup EXIT

mvn -B -q -f tests/java/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    "-DoutputDirectory=$scratch/deps"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 3
if [[ -z "${RUSTA_IMAGE:-}" ]]; then docker build -t "$candidate" .; fi

mkdir "$scratch/storage"
git archive 2fd1a65 | tar -x -C "$scratch/storage"
docker run --rm --mount "type=bind,src=$scratch/storage,dst=/src,readonly" -w /src \
    -e CARGO_TARGET_DIR=/target \
    rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea \
    cargo test --locked --workspace

run_java() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$source/classes:$scratch/deps/*" "$@"
}

for revision in abb95e7 7281ca3 639b77d; do
    echo "Testing build and offline upgrade from $revision"
    source="$scratch/$revision"
    mkdir -p "$source/classes"
    git archive "$revision" | tar -x -C "$source"
    docker build -t "$baseline" "$source"
    manifest=compose.cluster.yaml
    if [[ "$revision" == abb95e7 ]]; then manifest=compose.yaml; fi
    compose=(docker compose --project-directory "$root" -f "$manifest" -p "$project-$revision")
    javac -cp "$scratch/deps/*" -d "$source/classes" "$source/tests/java/"*.java
    RUSTA_IMAGE="$baseline" "${compose[@]}" up -d --no-build --wait
    if [[ "$manifest" == compose.yaml ]]; then
        run_java Smoke write "127.0.0.1:$RUSTA_PORT" "$source/identity"
    else
        seeds="127.0.0.1:$RUSTA_PORT_1,127.0.0.1:$RUSTA_PORT_2,127.0.0.1:$RUSTA_PORT_3"
        run_java ClusterSmoke write "$seeds" "$source" "$seeds"
        if [[ "$revision" == 639b77d ]]; then run_java GroupSmoke write "$seeds" 3; fi
    fi
    RUSTA_IMAGE="$baseline" "${compose[@]}" stop
    RUSTA_IMAGE="$candidate" "${compose[@]}" up -d --no-build --force-recreate --wait
    if [[ "$manifest" == compose.yaml ]]; then
        run_java Smoke verify "127.0.0.1:$RUSTA_PORT" "$source/identity"
    else
        printf '0' > "$source/leader"
        run_java ClusterSmoke failover "$seeds" "$source" "$seeds"
        if [[ "$revision" == 639b77d ]]; then run_java GroupSmoke verify "$seeds"; fi
    fi
    "${compose[@]}" down --volumes --remove-orphans
    compose=()
done
