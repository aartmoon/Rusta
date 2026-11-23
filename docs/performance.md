# Performance and release evidence

## Best local RF3 result

Rusta durable recorded a best observed **43.3 ms ACK p95** at 10,000 offered messages/s with 24 partitions and a 1 KiB mixed produce-and-consume workload. This is the best measured 60-second window from the benchmark series, not a repeatability estimate or a production SLO.

The [Rusta-only benchmark report](benchmarks/rf3-24p-10k-2026-11-23.md) documents the workload, resource limits, best-window throughput, consumer progress and reproduction command.

## Measurement method

The benchmark used one Kafka Java producer and consumer, idempotence, `acks=all`, `linger.ms=1`, no compression and a 10 ms maximum fetch wait. The broker cluster had three nodes with one vCPU and 1 GiB RAM per node. The client ran on an Apple M5 host; brokers ran in an ARM64 Docker VM with four vCPUs and 7.75 GiB RAM. The Rusta ACK profiler was disabled during measurement.

ACK latency starts immediately before record submission and ends when the client receives the broker acknowledgment. End-to-end latency spans submission through consumer delivery in the same client process. The runner drains the workload and checks acknowledged and consumed totals, partition offsets and payload order after the measurement. A local Docker VM includes virtualization and local storage effects; production acceptance requires measurements on the intended failure domains.

## Acknowledgment boundary

Rusta durable RF3 waits for the leader and a voting majority to persist the record before acknowledging. Page-cache mode waits for a voting majority to append to the operating system page cache and flushes in the background. Use durable mode for durability measurements.

For Kafka-wire measurements, the benchmark client uses Kafka Java with `acks=all` and idempotence. The benchmark harness records broker image IDs, effective topic settings and the configured acknowledgment mode with each run. Do not compare latency results unless workload shape, resource limits and acknowledgment boundaries match.

## RF3 durable acceptance

Run [OMB Small](https://docs.redpanda.com/streaming/current/develop/benchmark/#run-benchmark) with the same [workload](../tests/bench/omb/small.yaml) and [driver settings](../tests/bench/omb/durable.yaml) for Rusta and Redpanda. Build the [Redpanda OMB fork](https://github.com/redpanda-data/openmessaging-benchmark/tree/b0fa9236b16701f5f951f1b14db32e4da4b1393d) at its pinned revision. Run each cluster in turn on the same three Linux hosts with identical resource limits, filesystem and storage. Run the OMB client separately from the brokers and record image digests, effective settings and OMB output.

Use `RUSTA_CLUSTER_ACK_MODE=durable` for Rusta. For Redpanda, set `redpanda.developer_mode=false`, `redpanda.write_caching_default=false` and `--unsafe-bypass-fsync=false`, then verify the OMB-created topic has `write.caching=false`. Both clusters must sustain 50,000 messages/s with bounded consumer backlog. Rusta's aggregate publish p95 and p99 must each be at most 1.2 times Redpanda's. Verify acknowledged records survive abrupt crash and page-cache loss before accepting a durability-path change.

## Release gates

Measure replica catch-up, node addition, rebalance and the 4 KiB workload on the target deployment hardware. Capture repeated runs that meet the target ACK and end-to-end latency while consumer backlog remains bounded. Record allocator-specific live heap telemetry and verify crash recovery before making a performance target a release promise.
