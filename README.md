# Rusta

Rusta is a lightweight Kafka-compatible broker written in Rust. It persists Kafka record batches on disk and runs as a single broker or as a Raft cluster. See the [architecture](docs/architecture.md) and the [Kafka API contract](docs/kafka-api.md) for the release scope. See the [development roadmap](docs/roadmap.md) for the current status and release gates.

## Run locally

On Linux or on a Mac with Docker Desktop, start the broker from this directory.

```sh
docker compose up -d --build --wait
```

Use `127.0.0.1:9092` from the host and `broker:29092` from containers on the Compose network. Set `RUSTA_PORT` to change the host port. The named data volume survives `docker compose down` and container recreation. The Kafka listener is PLAINTEXT and the host port binds to loopback by default.

## Verify with Java

```sh
tests/java/smoke.sh
```

The smoke test requires Docker Compose, a host JDK 21 and Maven. It uses a dedicated Compose project and port 19092 by default. It runs Kafka Java Client 4.1.2 on the host and verifies the Docker-network listener from a container. It covers topic creation, offsets, keys and headers, the five codec modes, idempotent retries, retention, and recovery after killing and recreating the broker container. It also exercises eager and cooperative group rebalances, manual offset commits and Spring Kafka 4.0.8 topic creation and consumption with an annotated listener.

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

Cluster consumers currently use manual partition assignment. The cluster supports up to 128 partitions and topics with unlimited retention. Cluster consumer group failover, log retention, snapshots, general-purpose replica rebalancing and conversion of single-node volumes remain in the release plan. Each node keeps its original volume, including its producer-ID counter and election state.

Run `tests/java/cluster-smoke.sh` to exercise leader termination, replica rejoin, network partition and loss of quorum. Run `tests/java/topology-smoke.sh` to check independent RF3 and RF5 quorums on five brokers. Both use Kafka Java Client 4.1.2.

## Add a broker to a running cluster

List reserve broker addresses and pinned certificates in the startup peer inventory. For a local example, generate four certificates and a four-service Compose file with three bootstrap voters.

```sh
export RUSTA_CLUSTER_PKI="$PWD/.rusta-cluster-pki"
scripts/cluster-certs.sh "$RUSTA_CLUSTER_PKI" 4
scripts/cluster-compose.sh 4 19092 1,2,3 > "$RUSTA_CLUSTER_PKI/compose.yaml"
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow up -d --build --wait broker1 broker2 broker3
```

Later, start the reserve broker in the same Compose project.

```sh
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow up -d --wait broker4
```

The broker registers over mTLS and joins the controller log as a learner. After it catches up durably, the controller commits its promotion. The Kafka broker list and new topic placement use the committed membership. Existing partitions keep their replica sets. Keep the original bootstrap voter list and data volumes across restarts. Run `tests/java/membership-smoke.sh` to verify promotion while writes are active and membership recovery after restart.

## Remove a broker from a running cluster

Run the decommission command inside the broker you want to remove. This example uses the same Compose project as the scale-out example.

```sh
docker compose --project-directory "$PWD" -f "$RUSTA_CLUSTER_PKI/compose.yaml" -p rusta-grow \
  exec -T broker1 /usr/local/bin/rusta decommission
```

The command records a request on the broker's data volume and returns. The controller moves one replica at a time. A replacement catches up from the retained log before it gains a vote. Leadership moves off the departing broker before it loses its data or controller vote. The replication factor of each partition is preserved. Kafka clients can retry during leadership changes.

Keep the broker running until Kafka Metadata from a surviving broker lists neither the departing broker nor any partition replica on it. Then stop its container. Keep the original bootstrap voter list, peer inventory and volumes. The retired ID cannot automatically rejoin. Every affected partition must have an eligible replacement outside its current replica set. For example, add a fourth broker before decommissioning a broker in a three-broker cluster with RF3 topics. A request waits if it lacks capacity or the needed Raft majorities.

Run `tests/java/decommission-smoke.sh` to exercise live leader removal with Java producer traffic, a controller crash during relocation and readback on a surviving majority.
