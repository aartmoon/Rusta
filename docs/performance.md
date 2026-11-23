# Performance and release evidence

The comparison uses fixed offered load and checks delivered records and consumer progress. It reports the profile and acknowledgment boundary alongside every result. Rusta runs the same image for its single-broker and Raft profiles.

## RF3 24-partition best result

Rusta durable recorded a best observed **43.3 ms ACK p95** at 10,000 offered messages/s with 24 partitions and a 1 KiB mixed workload. This is the best measured 60-second window from the benchmark series, not a repeatability estimate. See the [Rusta-only benchmark report](benchmarks/rf3-24p-10k-2026-11-23.md) for the run configuration and measurements.

## Current local RF3 comparison

The 2026-11-22 UTC run compared Rusta with independent durable partition queues, Redpanda 26.3.1 in durable mode, and Kafka 4.1.2 with forced topic flush sequentially on the same Apple Silicon Docker VM. Each profile used three brokers, one vCPU and 1 GiB per broker, and the same Java Kafka client with idempotence and `acks=all`. The runner verified Redpanda's `fsync` process flag and the topic's disabled write caching. Kafka's forced flush still uses ISR acknowledgment semantics. [Raw JSON, gzip](benchmarks/durable-executor-comparison.json.gz)

Both tables use 2,000 offered messages/s, two six-second windows per workload, and four seconds of warmup per window. Every broker acknowledged all 20,000 submitted records per run. The mixed workloads also consumed and committed all 20,000. Latency ranges show the two run percentiles. Idle RSS is the range of per-broker sampled peaks during the idle window.

### Six partitions, `linger.ms=0`

| RF3 profile | 512 B write p95 ACK, ms | 1 KiB mixed p95 ACK, ms | 1 KiB mixed p99 ACK, ms | Idle RSS per broker, MiB |
| --- | ---: | ---: | ---: | ---: |
| Rusta durable | 4.9–5.4 | 5.5 | 6.8–6.9 | 7.4–7.6 |
| Redpanda durable | 5.4–5.6 | 6.2–6.3 | 7.9–8.0 | 110.2–113.3 |
| Kafka forced flush | 6.7–7.8 | 9.4–15.8 | 32.0–43.8 | 363.9–366.4 |

### Forty-eight partitions, `linger.ms=1`

| RF3 profile | 1 KiB mixed p95 ACK, ms | 1 KiB mixed p99 ACK, ms | 1 KiB mixed p99 end-to-end, ms | Idle RSS per broker, MiB |
| --- | ---: | ---: | ---: | ---: |
| Rusta durable | 13.7–19.4 | 24.0–28.6 | 77.3–110.5 | 7.4–7.7 |
| Redpanda durable | 6.4–6.5 | 8.1 | 8.3–9.1 | 111.6–113.7 |
| Kafka forced flush | 100.1–131.7 | 152.3–173.5 | 153.1–168.2 | 364.4–366.1 |

To reproduce the tables, select `--profiles rusta-rf3-durable redpanda-rf3-durable kafka-rf3-flush --seconds 6 --repeat 2` in the runner below. Add `--partitions 48 --workloads mixed --linger-ms 1` for the second table. The 48-partition workload has one producer and one consumer on the Docker VM. Production acceptance uses the [three-host OMB Small profile](#rf3-durable-acceptance).

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

Use `--profiles` to select broker profiles, `--partitions` and `--workloads` to select the Kafka workload shape, and `--shape-matrix` to include 4 KiB workloads with one or two producer/consumer pairs. When automatic Docker subnet allocation is exhausted, supply an unused subnet such as `--subnet 10.231.91.0/24`. Run the profiles sequentially on an idle host for latency comparisons. The output records Docker resources, image IDs and available registry digests. The output also contains private certificates for its disposable Rusta cluster.

## Matching the acknowledgment boundary

| Profile | Client and replication | Acknowledgment boundary |
| --- | --- | --- |
| Rusta durable RF1 and RF3 | Kafka Java 4.1.2 with `acks=all`, idempotence and six partitions | The singleton syncs locally. RF3 syncs the leader and at least one voting follower. |
| Rusta page-cache RF3 | Same Kafka client | A voting majority including the leader appends to the OS page cache. The broker flushes data in the background. |
| Kafka cache RF1 and RF3 | Same Kafka client. `min.insync.replicas` is 1 or 2 respectively | All current ISR members receive the write. The broker uses the OS flush policy. |
| Kafka flush RF1 and RF3 | Same Kafka client and ISR settings, with `flush.messages=1` | Forced topic flush. The ISR acknowledgment protocol differs from a Raft durable majority. |
| Redpanda cache RF1 and RF3 | Same Kafka client with topic `write.caching=true` | A majority accepts the write with background flush. |
| Redpanda durable RF1 and RF3 | Same Kafka client with verified topic `write.caching=false` | Acknowledgment waits for the durable majority. The runner verifies that the broker process has fsync enabled. |
| JetStream cache and sync RF1 and RF3 | nats-py 2.16.0, one file-backed stream, one durable push consumer for mixed load | One JetStream PubAck per message. Sync profiles set `sync_interval: always`. The consumer uses explicit per-message ACKs. |
| Iggy buffered RF1 | Apache Iggy Python SDK 0.10.0, native TCP, one partition | One acknowledged send request for the records accumulated in each 10 ms submission slot. The broker uses the image's default storage settings. The mixed workload uses explicit polling and saves the final consumer offset. |

Compare Kafka-wire latency within the same replication factor and acknowledgment class. Interpret each native profile with its API and storage boundary. The Iggy profile covers a single server. NATS and Iggy package records differently from the Kafka producer, so those rows describe their application workload rather than protocol overhead in isolation. See [Kafka replication](https://kafka.apache.org/41/design/design/#replication), [Kafka flush settings](https://kafka.apache.org/41/configuration/topic-configs/#flush.messages), [Redpanda write caching](https://docs.redpanda.com/streaming/25.3/reference/properties/cluster-properties/#write_caching_default), [JetStream](https://docs.nats.io/concepts/jetstream) and [Iggy Python SDK](https://iggy.apache.org/docs/sdk/python/intro/).

## Metrics and interpretation

- The common profile uses a 512-byte write workload and a 1 KiB produce-and-consume workload at 2,000 messages/s. Each measured window follows four seconds of warmup. Repetitions use new topics on the same running profile. The default Kafka producer uses `linger.ms=0`, 32 KiB batches and no compression. The consumer uses a 10 ms maximum fetch wait and commits periodically.
- ACK latency starts immediately before submitting the payload to the client. End-to-end latency spans submission through delivery to the consumer in the same process clock. A throughput window can finish with outstanding work. The harness drains it and checks sent/acked/read totals and record ordering. Payload bandwidth counts application bytes only.
- Per-broker window samples come from Linux `/proc` and cgroup v2. Sampled RSS peak, lifetime cgroup peak, anonymous RSS, file cache, charged socket memory, CPU as a fraction of one core, and cgroup block I/O deltas have different boundaries. The `[heap]` VMA covers the traditional `brk` heap. Native allocators, mapped regions and JVM heaps require separate telemetry for a total live heap.
- Startup is measured from first container creation to TCP availability and, for Kafka-wire profiles, metadata reporting the expected number of brokers. The durability suite separately checks ready data paths. Loopback results on a single Apple Silicon host's Docker VM include virtualization and local storage effects. Deployment SLOs require measurements across the intended failure domains.

## RF3 durable acceptance

Run [OMB Small](https://docs.redpanda.com/streaming/current/develop/benchmark/#run-benchmark) with [the same workload](../tests/bench/omb/small.yaml) and [driver settings](../tests/bench/omb/durable.yaml) for Rusta and Redpanda. Build the [Redpanda OMB fork](https://github.com/redpanda-data/openmessaging-benchmark/tree/b0fa9236b16701f5f951f1b14db32e4da4b1393d) at the pinned revision and run each cluster in turn on the same three Linux hosts, with identical resource limits, filesystem and storage. Run the OMB client separately from the brokers. Record image digests, effective broker and topic settings, and OMB JSON output.

Use `RUSTA_CLUSTER_ACK_MODE=durable` for Rusta. Set `redpanda.developer_mode=false`, `redpanda.write_caching_default=false` and `--unsafe-bypass-fsync=false` for Redpanda. Confirm the OMB-created topic uses `write.caching=false` before accepting results. Copy the shared driver settings to one file per cluster, change its name and bootstrap addresses, and pass each driver to `bin/benchmark -d` with the same workload file. Both clusters must sustain 50,000 messages/s with bounded consumer backlog. Rusta's aggregate publish p95 and p99 must each be at most 1.2 times Redpanda's. Verify acknowledged records survive an abrupt crash with page-cache loss before accepting a durability-path change.

For local diagnosis, build the current Rusta image and run `compare.py` with `--rusta-image`, `--profiles rusta-rf3-durable` and `--rusta-ack-profile`. Broker logs contain one-second histograms with counts and sums in microseconds. Each bucket `i` covers `2^i` through `2^(i+1)-1` microseconds, with bucket zero also covering sub-microsecond samples. `local_persist_to_commit` includes peer response and engine scheduling. `commit_to_reply` ends at the broker's internal Produce completion. Compare client ACK latencies without the profiler enabled.

### Corrected local comparison, 2026-11-22

Three 12-second measured windows per profile and workload followed four seconds of warmup on the local Docker VM. Each used six partitions at 2,000 messages/s. Redpanda's process arguments and the effective topic setting confirmed fsync enabled and write caching disabled. Every profile drained its writes successfully. The OMB acceptance profile requires a three-host run.

| RF3 profile | 512 B write p95 ACK, ms | 1 KiB mixed p95 ACK, ms | 1 KiB mixed p99 ACK, ms |
| --- | ---: | ---: | ---: |
| Rusta durable | 96.6–255.7 | 91.5–113.7 | 130.8–159.7 |
| Redpanda durable | 5.6–6.2 | 5.9–6.0 | 8.2–12.4 |
| Kafka forced flush | 6.5–7.5 | 9.3–10.1 | 28.8–35.1 |

A diagnostic run on the baseline measured 54–55 ms mean Kafka-client request latency against Rusta and 11 ms mean processing time inside the broker's Produce handler. The producer reported zero record retries. The worker queue p95 was 2–4 ms, local persistence to Raft commit p95 was 8–16 ms, and commit to internal reply p95 was 0.5–1 ms. Each baseline Kafka connection completed Produce and wrote its reply before reading the next frame.

### Produce pipeline and group commit

Cluster connections admit Produce requests in wire order and hold up to four replies per connection within the shared request budget. Responses are written in that same order after durable Raft completion. The worker groups already queued writes for each partition in one local journal sync, with immediate dispatch when the worker is idle. Durable data followers can learn the commit index from the next append or heartbeat. This lets the follower publish the append and commit positions in one manifest update.

Two seven-second measured windows per workload at 2,000 messages/s on the same Docker VM used the same client settings and enabled Rusta's ACK profiler throughout the A/B. All confirmed records drained. The aggregate count of progress-manifest saves across the three replicas per completed leader entry fell from about 5.8 to 4.0.

| RF3 profile | 512 B write p95 ACK, ms | 1 KiB mixed p95 ACK, ms | 1 KiB mixed p99 ACK, ms |
| --- | ---: | ---: | ---: |
| Rusta baseline | 101.3–106.1 | 103.4–156.3 | 147.5–297.9 |
| Rusta Produce pipeline | 80.2–96.1 | 83.8–90.8 | 110.2–120.0 |
| Rusta pipeline and group commit | 67.0–82.8 | 79.3–87.9 | 109.3–146.1 |
| Redpanda durable | 5.6–5.9 | 5.9–6.0 | 7.0–9.7 |

The local 48-partition checks used 2,000 messages/s with `linger.ms=1` and one producer and consumer. Increasing the Raft outbound queue removed observed frame drops during partition-wide broadcasts. Fetch starts quorum barriers together and uses `OFFSET_NOT_AVAILABLE` when the barrier times out, which lets Kafka consumers retry. In the instrumented run, the average interval from leader disk persistence to Raft commit was 239 ms. The final two mixed runs with profiling off drained all 18,000 submitted records per run on each broker. Rusta's ACK p95 was 796–839 ms and Redpanda's was 6.5–6.9 ms. The three-host OMB Small acceptance run remains pending.

### Durable manifest and parallel disk batches

Durable Raft data partitions publish progress with one `fdatasync` on a pre-existing manifest file. Separate checksummed generations protect the previous checkpoint during each update. A segment's directory entry is synchronized before the manifest can reference it. This benchmark candidate processed Ready work for up to four independent data partitions in parallel in an admission batch. The batch joined before the shared Engine advanced to its next operation. Fetch started quorum barriers in one Engine turn and materialized the authorized partitions in another, with the response byte budget applied in wire order. Cluster connections admitted up to 16 requests with a shared 48 MiB request-frame budget. Raft bulk frames left queue capacity and bytes for control messages.

At 48 partitions, 1 KiB and 2,000 messages/s on the local Docker VM, one matched short run measured 459 ms ACK p95 on the preceding code and 236 ms with the manifest change alone. The combined changes produced the following short mixed runs. Each profile verified every submitted record after draining. Idle RSS across the runs was 6.9–7.3 MiB per broker.

| Offered messages/s | ACK p95, ms | ACK p99, ms | Verified records including warmup |
| --- | ---: | ---: | ---: |
| 2,000 | 109 | 158 | 18,000 |
| 5,000 | 172 | 211 | 45,000 |
| 10,000 | 175 | 197 | 90,000 |
| 20,000 | 231 | 298 | 160,000 |
| 50,000 | 2,014 | 2,151 | 400,000 |

The 20,000 and 50,000 profiles used four-second measurement windows. The 50,000 profile had 40,858 unread records at the end of the window and delivered the remainder during drain. In a separate six-partition run at 2,000 messages/s, 512 B write p95 was 5.9 ms for Rusta and 6.0 ms for Redpanda. Mixed 1 KiB p95 was 6.6 ms and 8.5 ms respectively. These are single short measurements on one shared Docker VM.

The local 4 KiB mixed runs at 2,000 messages/s verified every record on RF1 and RF3. ACK p95 with one producer/consumer pair and one partition was 4.6 ms on RF1 and 9.0 ms on RF3. With two producer/consumer pairs and six partitions split across two topics, their p95 values were 5.3–5.6 ms and 35.8–36.5 ms respectively.

`RUSTA_DURABLE_GROUP_COMMIT_US` sets the extra group commit wait when durable requests are already queued. It accepts 0–2,000 microseconds and defaults to zero. A local 1 ms trial at 2,000 messages/s measured 108 ms ACK p95 and 162 ms p99. Use `--rusta-group-commit-us` to repeat that comparison. Saturation and the three-host OMB Small target require their own runs.

### Independent durable partition queues

Each durable data partition has a bounded FIFO and uses a shared pool of four blocking workers. The Engine mutex is released while a worker owns the partition. Produce, Fetch, and data Raft messages all use its queue. An authenticated peer connection can reclaim pipeline capacity when any data partition finishes. Control operations fence the registry for topology changes and snapshots. A partition undergoing relocation uses the fenced path.

In two five-second local mixed windows at 48 partitions, 1 KiB, and 2,000 messages/s, the previous batch-based engine measured 56–114 ms ACK p95 and 82–168 ms p99. Independent queues measured 35–57 ms p95 and 104–118 ms p99. Each broker configuration verified all 18,000 records per run including warmup. Idle RSS rose from 6.8–7.0 MiB to 7.4–7.6 MiB per broker. The p99 ranges overlap. Production comparison still requires the three-host OMB Small run.

## Local baseline, 2026-11-20

Apple M5, macOS 27.2.1, Docker Desktop 4.80.0 with Engine 29.6.1 and Linux 6.12.76. The VM had 4 CPUs and 7.75 GiB RAM, used overlayfs for container layers and reported `ext2/ext3` for the named volumes. Each broker had a 1 vCPU quota and a 1 GiB memory limit. Kafka used a 256 MiB JVM heap and Redpanda used one shard and a 512 MiB allocator budget. The Rusta image was built from the local working tree based on `230d7f7`. Exact image IDs, registry digests, PID 1 counters and cgroup samples are in the [raw results](benchmarks/roadmap6.json.gz).

Six partitions, 2,000 messages/s and two repetitions per Kafka-wire row. The latency ranges show the two run percentiles. RSS figures are per broker. Startup uses the readiness check described above.

The Redpanda image used for this historical run started with `--unsafe-bypass-fsync=true`, including the rows originally labelled durable. Those rows measured replication with fsync bypassed. The corrected runner explicitly enables fsync for both Redpanda profiles.

| Kafka-wire profile | 512 B p95 ACK, ms | 1 KiB p99 end-to-end, ms | 1 KiB ACK/s | Idle RSS, MiB | Peak sampled RSS, MiB | Startup, s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rusta RF1 durable | 8.4–21.5 | 18.0–21.3 | 2,000 | 4.8 | 5.4 | 0.6 |
| Rusta RF3 page cache | 5.2–5.3 | 15.5–18.8 | 2,000 | 6.5–6.8 | 8.4 | 0.8 |
| Rusta RF3 durable | 101.0–105.4 | 208.2–313.9 | 1,996–2,007 | 6.4–6.7 | 8.6 | 0.8 |
| Kafka RF1 page cache | 3.4 | 5.9–6.7 | 2,000 | 323.6 | 393.8 | 3.6 |
| Kafka RF3 page cache | 4.3–5.0 | 20.7–70.8 | 2,000–2,001 | 335.7–357.2 | 405.9 | 4.5 |
| Kafka RF1 forced flush | 5.4–5.7 | 11.6–16.6 | 1,999–2,000 | 330.0 | 398.7 | 3.3 |
| Kafka RF3 forced flush | 7.4–8.5 | 25.8–33.2 | 2,000–2,001 | 344.1–346.7 | 407.9 | 4.7 |
| Redpanda RF1 cache, fsync bypassed | 2.6–2.8 | 4.7–8.5 | 2,000–2,001 | 111.8 | 279.2 | 0.7 |
| Redpanda RF3 cache, fsync bypassed | 3.4–3.6 | 6.5–8.3 | 1,999–2,001 | 112.5–113.5 | 251.7 | 0.8 |
| Redpanda RF1 image defaults, fsync bypassed | 2.7–2.9 | 6.0–6.6 | 2,001 | 112.1 | 278.9 | 0.5 |
| Redpanda RF3 image defaults, fsync bypassed | 3.3–3.6 | 5.8–6.3 | 1,999–2,000 | 112.7–114.3 | 251.4 | 0.8 |

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
