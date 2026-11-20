# Rusta

Rusta is a lightweight Kafka-compatible broker written in Rust. It persists Kafka record batches on disk and runs as a single broker or as a Raft cluster. See the [architecture](docs/architecture.md) and the [Kafka API contract](docs/kafka-api.md) for the release scope. See the [development roadmap](docs/roadmap.md) for the current status and release gates.

## Run locally

On Linux or on a Mac with Docker Desktop, start the broker from this directory.

```sh
docker compose up -d --build --wait
```

Use `127.0.0.1:9092` from the host and `broker:29092` from containers on the Compose network. Set `RUSTA_PORT` to change the host port. The named data volume survives `docker compose down` and container recreation. The Kafka listener is PLAINTEXT and the host port binds to loopback by default.

For a prebuilt versioned image, see the [deployment and operations guide](docs/operations.md). It covers configuration, coordinated upgrades, scaling, monitoring, backup and restore, and the security boundaries of the PLAINTEXT listeners.

## Verify with Java

```sh
tests/java/smoke.sh
```

The smoke test requires Docker Compose, a host JDK 21 and Maven. It uses a dedicated Compose project and port 19092 by default. It runs Kafka Java Client 4.1.2 on the host and verifies the Docker-network listener from a container. It covers topic creation, offsets, keys and headers, the five codec modes, idempotent retries, retention, and recovery after killing and recreating the broker container. It also exercises eager and cooperative group rebalances, manual offset commits and Spring Kafka 4.0.8 topic creation and consumption with an annotated listener.

See the [Java and Spring Kafka examples](docs/java-clients.md) for client configuration and application code.

## Build and test

The pinned Rust toolchain is in `rust-toolchain.toml`. Cargo dependencies are locked for CI and container builds.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
docker buildx build --platform linux/amd64,linux/arm64 --load -t rusta:0.1.0-local .
```

The multi-platform load requires the containerd image store. The regular CI workflow also tests the MSRV from `Cargo.toml` and runs the Java and Spring Kafka smoke on both Linux architectures. Run the manual fault and upgrade workflow for cluster failures, compatibility, disk exhaustion and historical upgrades. The manual benchmark workflow records Kafka throughput, latency, broker RSS, CPU and cgroup memory in the downloadable artifact. Run it locally with `python3 tests/bench/run.py /tmp/rusta-benchmark` on Docker with cgroup v2 and a host JDK 21, Maven and Python 3.

After publication is approved, dispatch **Publish versioned image** on the Git tag matching the Cargo workspace version, such as `v0.1.0`. The workflow runs CI and publishes the semver image tag to GHCR. Use the published digest to reproduce a deployment.

## Single-node behavior

Create topics explicitly with AdminClient or a Spring Kafka `NewTopic` bean. Consumers can assign partitions manually or subscribe using dynamic classic groups. Group commits persist the next offset, optional metadata and leader epoch. The broker acknowledges them after syncing its group journal. After restart, members rejoin and resume from their last committed positions. `RUSTA_GROUP_OFFSET_RETENTION_MS` defaults to seven days after a group becomes empty.

The single-node broker supports up to 256 groups, 32 members per group and 128 pending or active members per broker. The group journal checkpoints periodically as it grows. The [API contract](docs/kafka-api.md) lists the exact ranges.

The default `acks=all` waits for a durable local commit. A topic can explicitly allow weaker acknowledgements with `rusta.allow.weak.acks=true`. Fetch returns the committed prefix in either mode. Topic creation accepts `retention.ms` and `retention.bytes`. The broker rejects producer sequences it cannot verify and limits active producer histories to 256 per partition. Retirement of producer IDs is planned. This iteration uses a single storage worker, with up to 512 partitions, 128 TCP connections and a 1 MiB batch limit.

## Run a local cluster

Create certificates in a new directory for the local Compose cluster. Keep the generated keys and use the same data volumes when restarting the brokers.

```sh
export RUSTA_CLUSTER_PKI="$(pwd)/.rusta-cluster-pki"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI"
docker compose -f compose.cluster.yaml up -d --build --wait
```

Use `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094` as Kafka bootstrap servers. Within the Compose network use `broker1:29092,broker2:29092,broker3:29092`. Broker-to-broker traffic uses mutual TLS with pinned node certificates on a separate network. Create topics with the replication factor you need and use `acks=all`. The leader responds after its own disk and a voting majority have persisted the batch. Fetch and offset lookups use a Raft quorum barrier. A failed or timed-out Produce can have an unknown result, so use an idempotent producer for retries.

For a different node count, generate a Compose configuration. This example starts five brokers on ports 19092 through 19096.

```sh
export RUSTA_CLUSTER_PKI="$PWD/.rusta-cluster-pki"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 5
scripts/cluster-compose.sh 5 19092 > "$RUSTA_CLUSTER_PKI/compose.yaml"
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-five up -d --build --wait
```

A new cluster accepts 1–255 configured node IDs. By default, its controller starts with the full roster. Topic replication factors can range from 1 to the number of brokers, with a default of three or the node count if smaller. Each partition uses its assigned replicas and a majority of those replicas for reads and writes. Keep the same bootstrap voters for an existing cluster. The peer protocol validates the bootstrap voters, and an upgrade from the original fixed three-member peer protocol requires restarting all nodes with the new image.

Cluster consumers can assign partitions manually or subscribe with dynamic classic groups. Four internal Raft shards route group requests and persist commits to a voting majority. After coordinator failover, members rejoin and resume from committed offsets. Each shard supports up to 64 groups and 32 pending or active members, with the same default seven-day offset retention as single-node. To enable groups on an existing cluster, restart all brokers on the new image while keeping the original volumes and bootstrap configuration. See [distributed classic consumer groups](docs/cluster-groups.md).

The cluster supports up to 128 user partitions. By default, records are retained indefinitely. Set `retention.ms` or `retention.bytes` at topic creation to advance the Kafka-visible start by whole segments. Time retention uses broker append time. Size retention keeps the active segment and applies independently of filesystem overhead. After a retention boundary commits, replicas durably snapshot the expired prefix before reclaiming its segments. Lagging replicas install the snapshot and resume appending from the retained suffix. The controller and group shards checkpoint after 4,096 committed entries by default. Set `RUSTA_CLUSTER_SNAPSHOT_INTERVAL` to change the internal interval.

Cluster Fetch supports full requests with a wait capped at two seconds before the final quorum read. It wakes on committed appends or leadership changes and preserves the first-batch exception to byte limits. A partition keeps up to 256 producer histories. `RUSTA_CLUSTER_PRODUCER_RETENTION_MS` sets the inactivity window and defaults to 24 hours. Use the same setting on all brokers. Retired histories release capacity and old IDs covered by their fences can get `UNKNOWN_PRODUCER_ID`.

Use a coordinated stop and restart when upgrading a cluster from a build without snapshots. Keep the original volumes, cluster ID, bootstrap voters and pins. Keep a backup before upgrading. Once a snapshot replaces journal entries, the upgraded broker is required to open the volume.

Run `tests/java/cluster-smoke.sh` for Fetch long polling, quorum failures and snapshot catch-up after a physical deletion. Run `tests/java/topology-smoke.sh` to check independent RF3 and RF5 quorums on five brokers. Run `tests/java/cluster-groups-smoke.sh` for Java and Spring consumer failover, classic group API versions, coordinator network isolation and offset recovery with internal compaction enabled.

## Topic administration and client compatibility

Both broker modes support deleting topics by name or ID, describing topic configs, and setting or removing the two retention overrides. `IncrementalAlterConfigs` supports validation without applying the changes. Topic expansion is a separate unsupported operation. A name can be reused after deletion and the recreated topic receives a new ID.

Run `tests/java/admin-smoke.sh` to verify durable mutations and cleanup after restart. Run `tests/compat/smoke.sh` for the pinned Go, Python and Rust clients and wire negotiation. The latter needs Go 1.24 or later, Docker and a Python interpreter with a wheel for the pinned client. Set `PYTHON` to select the interpreter. The [API contract and client matrix](docs/kafka-api.md) list the supported request versions and tested flows.

Upgrade all cluster members before applying the new topic mutations so every member can replay the new controller events.

## Add a broker to a running cluster

Keep the original cluster ID, bootstrap voters and data volumes. This Compose example starts with three brokers and provisions broker 4 later. On an existing cluster, start with the certificate directory, port assignments and Compose project already in use.

```sh
export RUSTA_CLUSTER_PKI="$PWD/.rusta-cluster-pki"
rusta_grow() {
  docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow "$@"
}
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 3
scripts/cluster-compose.sh 3 19092 1,2,3 > "$RUSTA_CLUSTER_PKI/compose.yaml"
rusta_grow up -d --build --wait broker1 broker2 broker3
```

Choose an unused broker ID. The local certificate helper uses the existing Compose CA and preserves previously issued pins. It is intended for local use. In other deployments, distribute the new broker's public leaf certificate as `node-4.pem` under each broker's `RUSTA_CLUSTER_CERTS` directory. Use a leaf signed by the cluster CA, with the Raft DNS name in its SAN and both client and server authentication usages. Give each broker access to its own private key.

```sh
scripts/cluster-certs.sh --add "$RUSTA_CLUSTER_PKI" 4
spec=4,raft4:7000,127.0.0.1:19095,broker4:29092
for broker in broker1 broker2 broker3; do
  rusta_grow exec -T "$broker" /usr/local/bin/rusta stage-peer "$spec"
  rusta_grow exec -T "$broker" /usr/local/bin/rusta peer-inventory
done
scripts/cluster-compose.sh 4 19092 1,2,3 > "$RUSTA_CLUSTER_PKI/compose.yaml.next"
mv "$RUSTA_CLUSTER_PKI/compose.yaml.next" "$RUSTA_CLUSTER_PKI/compose.yaml"
rusta_grow up -d --no-deps --wait broker4
rusta_grow exec -T broker4 /usr/local/bin/rusta peer-inventory
```

The local Unix admin socket limits staging to operators with access to the broker account or private data directory. A repeated staging command with the same endpoints and pin is idempotent. For a dynamically staged broker, the controller waits for every current controller voter and learner to confirm the same endpoints and pin before admission. The broker then registers over mTLS, catches up as a learner and is promoted. Check that 4 appears in `voters` and Kafka Metadata. Existing partitions keep their replica sets until an operator requests a rebalance. The staging operation applies on the running brokers and survives restart on their original volumes.

## Rebalance replicas and transfer leadership

Run the local commands inside the broker containers with the same Compose project used for expansion. `relocation-status` shows the broker's current controller view, the committed plan, progress and the last completed rebalance. Run `rebalance` on the active controller and `transfer-leadership` on the current partition leader.

```sh
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow \
  exec -T broker1 /usr/local/bin/rusta relocation-status
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow \
  exec -T broker1 /usr/local/bin/rusta rebalance 6e0c4ca8-c1d9-4a76-9b45-a67badbd582d
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow \
  exec -T broker2 /usr/local/bin/rusta transfer-leadership my-topic 0 4
```

Supply your own request UUID and reuse it if the result is uncertain. The controller recognizes the active plan and its last completed plan. It balances total replica counts across committed brokers, including internal group shards. Topic IDs, partition IDs and replication factors stay stable. Only one plan runs at a time, with one learner catching up at up to 8 MiB/s plus burst capacity. The committed plan resumes after controller restart and waits for an unavailable destination.

Explicit transfer requires a caught-up voting replica. The command reports success after the requesting broker observes the requested leader. The local administration socket is available only to the broker account or an operator with access to its private data directory. Upgrade all brokers before invoking the new relocation commands.

## Remove a broker from a running cluster

Run the decommission command inside the broker you want to remove. This example uses the same Compose project as the scale-out example.

```sh
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow \
  exec -T broker1 /usr/local/bin/rusta decommission
```

The command records a request on the broker's data volume and returns. The controller moves one replica at a time. A replacement catches up from the retained log before it gains a vote. Leadership moves off the departing broker before it loses its data or controller vote. The replication factor of each partition is preserved. Kafka clients can retry during leadership changes.

Keep the broker running until Kafka Metadata from a surviving broker lists neither the departing broker nor any partition replica on it. Then stop its container. Keep the original bootstrap voter list, peer inventory and volumes. The retired ID cannot automatically rejoin. Every affected partition must have an eligible replacement outside its current replica set. For example, add a fourth broker before decommissioning a broker in a three-broker cluster with RF3 topics. A request waits if it lacks capacity or the needed Raft majorities.

Run `tests/java/decommission-smoke.sh` to exercise live leader removal with Java producer traffic, a controller crash during relocation, readback on a surviving majority and a full restart with the retired broker's original volume.

Retired broker volumes can be archived with their original cluster and node identity for diagnosis and backup. Provision a replacement with a fresh volume and an unused ID. Keep retired IDs and their public pins in the inventory for replay of historical membership. Remove access to retired private keys.

## Rotate peer certificates

Run this operation during a maintenance window with the cluster stopped. Prepare a complete replacement set of public pins, trust roots, and per-node private keys. Keep the node IDs, Raft names and endpoints. Archive the old bundle for recovery and distribute the new bundle while the brokers are stopped. Apply the replacement pins to every stopped volume before starting any broker. In Compose, mount the new bundle with the normal certificate settings. If using the earlier `rusta_grow` function, keep a copy of the same Compose file in the replacement bundle before updating `RUSTA_CLUSTER_PKI`.

```sh
rusta_grow run --rm --no-deps broker1 refresh-peer-pins
# Repeat for each broker using its own volume and key mount.
rusta_grow up -d --force-recreate --wait broker1 broker2 broker3 broker4
```

The refresh validates the certificate names, trust and usages and rewrites the durable staged fingerprints. It rejects an active volume. If the operation stops partway through, keep the cluster stopped and repeat the refresh with the same bundle. To revert before startup, restore the old bundle and repeat the refresh on every volume. Before restoring traffic, verify controller election and read and write with a Kafka client.

## Migrate single-node data to a cluster

This is an offline import to a new cluster. Build the same image for the source and targets so the source understands migration fencing. Stop clients and the source broker. Back up the stopped source volume. Prepare fresh cluster volumes, a destination cluster ID, bootstrap voter IDs and peer certificates. Keep them fixed throughout the operation. Each imported partition and group shard starts with all destination bootstrap voters as replicas, so reserve enough disk space on each.

```sh
export RUSTA_CLUSTER_ID=8852c28c-98f3-4330-a6c5-90d73592ccf0
source_volume=my-single-node_broker-data
for broker in broker1 broker2 broker3; do
  docker compose -f compose.cluster.yaml -p rusta-import run --rm --no-deps \
    -v "$source_volume:/migration-source" "$broker" migrate-single-node /migration-source
done
docker compose -f compose.cluster.yaml -p rusta-import up -d --wait
```

Replace the example destination ID and source volume name with your own. Use a writable mount for the source because the importer records a durable fence. The import preserves topic IDs, Kafka offsets and log-start offsets, retained batches, retry histories and group commits. Consumers rejoin their groups on the destination. The importer rejects a running source, a nonempty target and source layouts exceeding cluster limits. Topics configured for weak acknowledgements also require preparation before import.

If an import fails or is interrupted, keep clients and the destination stopped and repeat the same command with the same source and target volumes. An unfinished target refuses startup. Start the cluster only after all voter imports report readiness, then verify topic IDs, offsets and records before switching clients to the new listeners. Use the backed-up source for rollback before the destination accepts client writes. After cutover, restoring that backup would discard writes made to the destination. Run `tests/java/migration-smoke.sh` for the end-to-end procedure.
