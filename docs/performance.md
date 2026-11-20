# Performance and release evidence

The comparison uses fixed offered load and checks delivered records and consumer progress. It reports the profile and acknowledgment boundary alongside every result. Rusta runs the same image for its single-broker and Raft profiles.

## Reproduce the comparison

Use Docker Engine with cgroup v2 and Compose, a host JDK 21, Maven, and Python. The native client harness also requires Python 3.11–3.13 with the frozen packages in `tests/bench/requirements-native.txt`. Each profile creates its own bridge, containers and named volumes. The runner publishes Kafka and native client ports on loopback.

```sh
docker build -t rusta:roadmap6 .
docker pull apache/kafka:4.1.2
docker pull docker.redpanda.com/redpandadata/redpanda:v26.3.1
docker pull apache/iggy:0.10.0
docker pull nats:2.12.4-alpine
python3.13 -m venv /tmp/rusta-bench-venv
/tmp/rusta-bench-venv/bin/pip install -r tests/bench/requirements-native.txt
python3 tests/bench/compare.py /tmp/rusta-comparison \
  --native-python /tmp/rusta-bench-venv/bin/python --seconds 6 --repeat 2
```

Use `--profiles` to select individual profiles and `--shape-matrix` to include 4 KiB workloads with one or two producer/consumer pairs. When automatic Docker subnet allocation is exhausted, supply an unused subnet such as `--subnet 10.231.91.0/24`. Run the profiles sequentially on an idle host for latency comparisons. The output records Docker resources, image IDs and available registry digests. The output also contains private certificates for its disposable Rusta cluster.

## Matching the acknowledgment boundary

| Profile | Client and replication | Acknowledgment boundary |
| --- | --- | --- |
| Rusta durable RF1 and RF3 | Kafka Java 4.1.2 with `acks=all`, idempotence and six partitions | The singleton syncs locally. RF3 syncs the leader and a voting majority. |
| Rusta page-cache RF3 | Same Kafka client | A voting majority including the leader appends to the OS page cache. The broker flushes data in the background. |
| Kafka cache RF1 and RF3 | Same Kafka client. `min.insync.replicas` is 1 or 2 respectively | All current ISR members receive the write. The broker uses the OS flush policy. |
| Kafka flush RF1 and RF3 | Same Kafka client and ISR settings, with `flush.messages=1` | Forced topic flush. The ISR acknowledgment protocol differs from a Raft durable majority. |
| Redpanda cache RF1 and RF3 | Same Kafka client with topic `write.caching=true` | A majority accepts the write with background flush. |
| Redpanda durable RF1 and RF3 | Same Kafka client with the server default write-caching policy | Acknowledgment waits for the durable majority. The benchmark uses explicit resource flags. |
| JetStream cache and sync RF1 and RF3 | nats-py 2.16.0, one file-backed stream, one durable push consumer for mixed load | One JetStream PubAck per message. Sync profiles set `sync_interval: always`. The consumer uses explicit per-message ACKs. |
| Iggy buffered RF1 | Apache Iggy Python SDK 0.10.0, native TCP, one partition | One acknowledged send request for the records accumulated in each 10 ms submission slot. The broker uses the image's default storage settings. The mixed workload uses explicit polling and saves the final consumer offset. |

Compare Kafka-wire latency within the same replication factor and acknowledgment class. Interpret each native profile with its API and storage boundary. The Iggy profile covers a single server. NATS and Iggy package records differently from the Kafka producer, so those rows describe their application workload rather than protocol overhead in isolation. See [Kafka replication](https://kafka.apache.org/41/design/design/#replication), [Kafka flush settings](https://kafka.apache.org/41/configuration/topic-configs/#flush.messages), [Redpanda write caching](https://docs.redpanda.com/streaming/25.3/reference/properties/cluster-properties/#write_caching_default), [JetStream](https://docs.nats.io/concepts/jetstream) and [Iggy Python SDK](https://iggy.apache.org/docs/sdk/python/intro/).

## Metrics and interpretation

- The common profile uses a 512-byte write workload and a 1 KiB produce-and-consume workload at 2,000 messages/s. Each measured window follows four seconds of warmup. Repetitions use new topics on the same running profile. The default Kafka producer uses `linger.ms=0`, 32 KiB batches and no compression. The consumer uses a 10 ms maximum fetch wait and commits periodically.
- ACK latency starts immediately before submitting the payload to the client. End-to-end latency spans submission through delivery to the consumer in the same process clock. A throughput window can finish with outstanding work. The harness drains it and checks sent/acked/read totals and record ordering. Payload bandwidth counts application bytes only.
- Per-broker window samples come from Linux `/proc` and cgroup v2. Sampled RSS peak, lifetime cgroup peak, anonymous RSS, file cache, charged socket memory, CPU as a fraction of one core, and cgroup block I/O deltas have different boundaries. The `[heap]` VMA covers the traditional `brk` heap. Native allocators, mapped regions and JVM heaps require separate telemetry for a total live heap.
- Startup is measured from first container creation to TCP availability and, for Kafka-wire profiles, metadata reporting the expected number of brokers. The durability suite separately checks ready data paths. Loopback results on a single Apple Silicon host's Docker VM include virtualization and local storage effects. Deployment SLOs require measurements across the intended failure domains.

## Local baseline, 2026-11-20

Apple M5, macOS 27.2.1, Docker Desktop 4.80.0 with Engine 29.6.1 and Linux 6.12.76. The VM had 4 CPUs and 7.75 GiB RAM, used overlayfs for container layers and reported `ext2/ext3` for the named volumes. Each broker had a 1 vCPU quota and a 1 GiB memory limit. Kafka used a 256 MiB JVM heap and Redpanda used one shard and a 512 MiB allocator budget. The Rusta image was built from the local working tree based on `230d7f7`. Exact image IDs, registry digests, PID 1 counters and cgroup samples are in the [raw results](benchmarks/roadmap6.json.gz).

Six partitions, 2,000 messages/s and two repetitions per Kafka-wire row. The latency ranges show the two run percentiles. RSS figures are per broker. Startup uses the readiness check described above.

| Kafka-wire profile | 512 B p95 ACK, ms | 1 KiB p99 end-to-end, ms | 1 KiB ACK/s | Idle RSS, MiB | Peak sampled RSS, MiB | Startup, s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rusta RF1 durable | 8.4–21.5 | 18.0–21.3 | 2,000 | 4.8 | 5.4 | 0.6 |
| Rusta RF3 page cache | 5.2–5.3 | 15.5–18.8 | 2,000 | 6.5–6.8 | 8.4 | 0.8 |
| Rusta RF3 durable | 101.0–105.4 | 208.2–313.9 | 1,996–2,007 | 6.4–6.7 | 8.6 | 0.8 |
| Kafka RF1 page cache | 3.4 | 5.9–6.7 | 2,000 | 323.6 | 393.8 | 3.6 |
| Kafka RF3 page cache | 4.3–5.0 | 20.7–70.8 | 2,000–2,001 | 335.7–357.2 | 405.9 | 4.5 |
| Kafka RF1 forced flush | 5.4–5.7 | 11.6–16.6 | 1,999–2,000 | 330.0 | 398.7 | 3.3 |
| Kafka RF3 forced flush | 7.4–8.5 | 25.8–33.2 | 2,000–2,001 | 344.1–346.7 | 407.9 | 4.7 |
| Redpanda RF1 cache | 2.6–2.8 | 4.7–8.5 | 2,000–2,001 | 111.8 | 279.2 | 0.7 |
| Redpanda RF3 cache | 3.4–3.6 | 6.5–8.3 | 1,999–2,001 | 112.5–113.5 | 251.7 | 0.8 |
| Redpanda RF1 durable | 2.7–2.9 | 6.0–6.6 | 2,001 | 112.1 | 278.9 | 0.5 |
| Redpanda RF3 durable | 3.3–3.6 | 5.8–6.3 | 1,999–2,000 | 112.7–114.3 | 251.4 | 0.8 |

Rusta stayed within its original RSS targets. In the mixed RF3 page-cache windows, broker CPU was 21.6–27.5% of one core and file cache reached 60.7 MiB per broker. The highest lifetime cgroup peak was 68.0 MiB in this profile. At 2,000 messages/s the 1 KiB payload rate is about 1.95 MiB/s. Each measurement window used a fixed offered load, so the table gives no saturation limit. The Rusta page-cache latency targets of 4 ms ACK p95 and 14 ms end-to-end p99 were missed in both repetitions.

### Native client observations

The native tests each used one partition or stream. JetStream used two six-second windows for each workload. Iggy used one four-second window for each workload after correcting the SDK polling fixture. The Iggy retry and part of the JetStream RF3 cache profile ran concurrently on the same Docker VM.

| Native profile | 512 B p95 ACK, ms | 1 KiB p99 end-to-end, ms | 1 KiB ACK/s | Idle RSS, MiB | Peak sampled RSS, MiB | Startup, s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Apache Iggy RF1 buffered | 3.7 | 12.7 | 2,003.7 | 117.7 | 129.8 | 0.2 |
| JetStream RF1 cache | 3.5–3.9 | 6.7–7.4 | 1,999.7–1,999.8 | 14.4 | 65.2 | 0.2 |
| JetStream RF3 cache | 4.4–4.5 | 6.7–7.7 | 1,999.9–2,000.0 | 16.7–17.5 | 90.3 | 0.5 |
| JetStream RF1 sync | 7.7–7.9 | 14.2–23.7 | 1,999.7–1,999.8 | 14.2 | 65.9 | 0.1 |
| JetStream RF3 sync | 84.4–93.4 | 2498.1–3248.7 | 1267.5–1357.4 | 16.5–17.4 | 109.1 | 0.5 |

### Release image

| Rusta release asset | Linux arm64 | Linux amd64 |
| --- | ---: | ---: |
| Local image, bytes from Docker inspect | 31,429,526 | 31,686,971 |
| Stripped executable, bytes | 7,542,576 | 8,451,648 |

Both images use UID/GID `65532:65532`. The amd64 image was built and inspected on Apple Silicon.

## Release gates in this checkout

The local Rust workspace regressions, bounded full-disk test and Java group and cache-recovery fault suites passed. The cache-recovery test restores an older durable prefix, corrupts the live manifest and changes the kernel marker on a disposable broker volume. It verifies that the returning replica catches up, the cluster can lose its leader after rejoin, and recovery-only replicas cannot elect a data leader. The Kafka group test verifies committed offsets after losing the coordinator.

Measure the 8 MiB replica catch-up, node addition and rebalance locally with `python3 tests/bench/lifecycle.py /tmp/rusta-lifecycle --image rusta:roadmap6`. Catch-up time includes restarting the replica and verifying recovery. `--shape-matrix` in the comparison runner adds a 4 KiB run on a single partition and a run split across two topics and two producer/consumer pairs.

Release evidence still needs the timings from those lifecycle and 4 KiB profiles, allocator-specific live heap telemetry, and a repeat across separate Linux hosts. GHCR publication is a separate release action.

Use the observed RAM budget from the sustained profile and a clean target host. Compare RSS to the initial 64 MiB singleton and 128 MiB per cluster broker targets, and size the memory limit for page cache and the measured cgroup peak. Capture a workload meeting the target ACK and end-to-end latency across repeated runs before making that target a release promise.
