# Rusta

Rusta is a lightweight Kafka-compatible broker written in Rust. The current iteration runs on a single node and persists Kafka record batches on disk. See the [architecture](docs/architecture.md) and the [Kafka API contract](docs/kafka-api.md) for the cluster design and release scope.

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

The smoke test requires Docker Compose, a host JDK 21 and Maven. It uses a dedicated Compose project and port 19092 by default. It runs Kafka Java Client 4.1.2 on the host and verifies the Docker-network listener from a container. It covers topic creation, offsets, keys and headers, the five codec modes, an idempotent retry, retention, and recovery after killing and recreating the broker container.

## Single-node behavior

Create topics explicitly with AdminClient. The running broker advertises version negotiation, metadata, topic creation, producer ID allocation, Produce, Fetch, ListOffsets, and leader-epoch offset lookup. Consumers use manual partition assignment. Group coordination, consumer offset commits, and the remaining Admin APIs are planned.

The default `acks=all` waits for a durable local commit. A topic can explicitly allow weaker acknowledgements with `rusta.allow.weak.acks=true`. Fetch returns the committed prefix in either mode. Topic creation accepts `retention.ms` and `retention.bytes`. The broker rejects producer sequences it cannot verify and limits active producer histories to 256 per partition. Retirement of producer IDs is planned. This iteration uses a single storage worker, with up to 512 partitions, 128 TCP connections and a 1 MiB batch limit.
