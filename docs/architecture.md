# Rusta architecture proposal

**Status:** Single-node storage, Kafka traffic and classic groups are implemented. The cluster supports distributed classic groups, configurable bootstrap voters, hot addition of preauthorized nodes and online decommission with replica relocation. The controller and partition replica sets have separate quorums. The remaining cluster sections describe the release design. Numerical tuning defaults in this document are starting points for implementation and measurement.

Rusta is an Apache-2.0 Rust broker with a native Kafka TCP endpoint, a segmented disk log and no mandatory external service. Release artifacts are versioned images in GHCR for `linux/amd64` and `linux/arm64`. The same image and binary run a singleton or a cluster. The [Kafka API contract](kafka-api.md) defines the first release's client surface.

## Components and traffic

| Workspace crate | Responsibility |
|---|---|
| `rusta-broker` | Binary, configuration, listener lifecycle, Kafka request routing, byte admission and diagnostics |
| `rusta-kafka` | Kafka request and response framing, version registry, errors, record-batch validation |
| `rusta-log` | Partition and internal log storage, segment recovery, sparse indexes, retention and snapshot files |
| `rusta-raft` | Raft integration, disk storage adapter, mTLS peer transport, scheduling and durable commit |
| `rusta-coordinator` | Controller metadata, replica placement, classic groups and offset state machines |

A configured number of Tokio workers handles networking. A bounded pool performs blocking disk I/O and a bounded CPU queue validates compressed batches. Each partition has one logical owner that serializes mutations. A fixed set of scheduler shards owns these partitions and drives their timers and I/O completions. There is no dedicated thread, socket or permanently allocated I/O buffer per partition. The separate internal listener authenticates broker identities and handles peer RPCs. Kafka replies identify client-reachable broker addresses for the listener that received the request.

The control Raft group holds the cluster ID, broker identities and endpoints, topic IDs, partition membership, configuration, reassignment progress and producer ID allocations. Each data partition has its own Raft group, term, replication membership and ordered log. A data append takes no controller round trip. Controllers reconcile partition leadership from current reports tagged with terms. A stale routing reply is recoverable by client metadata refresh. The partition's Raft state decides whether it can serve a request.

Use an established Raft implementation. TiKV's `raft` crate and its `RawNode` interface are the first candidate for the durable-storage spike. Reuse the consensus algorithm and integrate persistence, transport and membership at the documented boundary. Verify disk-backed reads, bounded handling of Ready entries, safe ReadIndex and joint reconfiguration before pinning the dependency. The initial transport multiplexes Raft groups over a bounded number of mTLS TCP connections per peer. Every envelope carries a protocol version, cluster ID, group ID and bounded payload length. Snapshot transfers have their own flow control so catch-up cannot block votes and heartbeats. [Raft paper](https://raft.github.io/raft.pdf), [Raft Ready persistence contract](https://docs.rs/raft/latest/raft/raw_node/struct.Ready.html)

A singleton uses the same identities, journals and commit model with one voter and no peer transport. Its local data directories are ready for in-place conversion to a cluster.

## Storage and durable commits

Fresh Linux clusters with three bootstrap voters default to page-cache Produce acknowledgments for RF3 data partitions. A voting majority including the leader must append before acknowledgment. A replica that loses its kernel cache enters recovery and waits for the live quorum to restore its log before voting again. The controller and consumer offsets use durable progress. See the [acknowledgment and recovery policy](operations.md#page-cache-acknowledgements).

The first implementation lives in `crates/rusta-log`. Its checkpoint records the retained start, durable end, committed end and term before the retained range. Recovery validates the durable prefix and discards bytes beyond it. Time lookup narrows the search with segment and batch bounds and calls a record decoder for the exact match. The storage iteration tests that boundary with Kafka v2 fixtures. Full compressed-record admission, coordinator state and networked Raft remain later integration work. The on-disk format is provisional during the Raft adapter spike.

A topic ID and partition number identify a directory containing a versioned manifest, immutable sealed segments and one active segment. Each data entry stores a length-delimited, checksummed envelope with Raft index and term and one Kafka record batch. Internal entries, including configuration changes and retention commands, consume Raft indexes. Kafka offsets count user records. Maintain a mapping between the two index spaces. A data append writes the payload once to the on-disk replicated log. The Raft adapter reads bounded slices from this storage. No second full payload journal is required.

At admission validate batch structure, CRC, record counts, compression and timestamp fields. Bound both compressed bytes and expanded bytes. Assign base offset and leader epoch at the partition owner. Preserve keys, nullable values, headers, timestamps and compressed record bytes. Keep the Kafka CreateTime representation and a separate broker append timestamp for retention. Sparse indexes map offsets and Raft indexes to physical positions. Timestamp lookup uses conservative per-segment bounds followed by a scan for the first retained offset meeting the timestamp. Indexes are disposable and a crash cannot make an index authoritative over the log. [Kafka record-batch format](https://kafka.apache.org/41/implementation/message-format/)

A partition tracks three boundaries: local append, local durable Raft progress and committed visibility. For a durable acknowledgement:

1. The leader appends the batch. The leader and followers synchronize the entries and any required Raft term, vote or configuration state. New segment names and manifest replacements are synchronized with the containing directory.
2. A follower advances its reported durable position after sync succeeds. The leader advances commit using a durable voting majority and its own durable storage. During joint configuration, both majorities are required.
3. The leader applies the committed entry in order. It can acknowledge `acks=all` and publish the corresponding high watermark. A network timeout at any earlier or later point may leave the result unknown to the client.

Group commit shares synchronization across waiting appends with a bounded byte total and maximum wait. Start tuning around a 1 ms batching delay. Flush failure, including ENOSPC and I/O error, prevents success and takes the affected replica out of service until recovery. A single-node commit boundary must be durable and recoverable before acknowledgement. A cluster recovery must also recover the acknowledged prefix from an eligible voting majority even if an individual commit checkpoint lagged. Use ordered file sync and atomic manifest replacement with directory sync. [Linux fsync semantics](https://www.man7.org/linux/man-pages/man2/fdatasync.2.html)

After a crash, read the last valid manifest, validate the journal and rebuild missing indexes with bounded I/O. A demonstrably incomplete uncommitted tail can be discarded. Corruption inside the retained durable prefix makes the local copy unavailable. A cluster replaces it from a healthy peer. An affected singleton reports the error and needs operator recovery. Successful writes can subsequently disappear from reads through retention or topic deletion.

Retention advances a replicated log-start offset on segment boundaries. A user partition snapshot covers the removed prefix. Its position includes the last included Raft index and term, the next Kafka offset, the last compacted batch epoch, membership and producer history. The live Raft suffix carries all retained records. A learner that needs the removed prefix installs the snapshot first and then receives the suffix in bounded appends.

## Cluster retention boundary

Each user partition has a Kafka-visible start and a physical Raft log start.
A partition leader selects a committed segment boundary and proposes the
next Raft index and Kafka offset as an internal entry. Replicas validate the
position against their committed prefix and recover it on replay. Time-based
retention uses the original leader's append timestamp carried in the Raft
entry. Size-based retention keeps the last segment. Earliest offset, timestamp
search and Fetch use the Kafka-visible start. Leader epoch lookups use the
preserved history and clamp end offsets to the visible range.

The version-1 `RUSTAD01` checkpoint contains a CRC32C-protected JSON payload. A partition uses up to 512 KiB. An internal checkpoint wraps that metadata and a Zstandard-compressed JSON state in a CRC32C-protected `RUSTAI01` envelope. The controller captures topic and producer allocation metadata, original and current placements, retired IDs and the last completed rebalance. It checkpoints between relocations, so the journal still carries any active plan and its progress. Group shards capture offset metadata, generation fences and empty-group times. Brokers preserve the public Kafka epoch stored in each batch independently of the Raft term.

The peer transport transfers snapshots in 256 KiB chunks on separate authenticated connections, with up to 8 MiB/s per transfer and a 12 MiB serialized state cap. The receiver reserves bytes before buffering the payload and validates the complete snapshot before installation. The snapshot file is synced before the Raft manifest changes. The old prefix remains recoverable until publication succeeds. A dropped transfer releases its reservation, and Raft retries after its snapshot deadline. A destination absent at the snapshot index enters the restored configuration as a nonvoting learner and replays its membership change from the suffix.

## Kafka reads, producer retries and group state

- **Reads.** In cluster mode the leader uses Raft Safe ReadIndex and serves only after applying through the returned index. A long-poll `Fetch` joins a fresh barrier when it becomes ready to return. Requests already queued can share a barrier started after they enrolled. A disconnected minority cannot authorize fresh client reads. Reads already authorized at a quorum may complete after a network fault. Singleton reads use the local committed boundary. The initial server handles full Fetch requests with session ID 0. Codecs, wire limits and error cases are specified in the [API contract](kafka-api.md). [Raft Safe ReadIndex](https://docs.rs/raft/latest/raft/enum.ReadOnlyOption.html)
- **Visibility and ordering.** Fetch returns complete batches below the committed high watermark. The first eligible batch may exceed a caller's requested fetch size to allow progress. The broker reserves bytes for that case before responding. Key routing follows the client's Kafka partitioner. Replica reassignment leaves the partition and its offsets intact. A non-idempotent producer retry can create duplicates and parallel retries can change the order in which its sends reach the partition.
- **Weaker writes.** Topics configured to allow `acks=1` respond after the leader completes the local append. An allowed `acks=0` sends no response. Both may be lost before durable commit. Disallowed `acks=1` returns the configured policy error. Disallowed `acks=0` may close the connection and writes nothing to that partition. The topic-wide switch never weakens `acks=all`. All modes apply the same committed-read visibility.
- **Idempotent writes.** Allocate producer IDs through durable controller ranges. For each producer and partition track the epoch, next sequence and recent accepted batch ranges and their original offsets. Keep up to five recent batches for Kafka's in-flight retry window. Coalesce duplicates of pending appends. Reject gaps and stale epochs. Rebuild deduplication state from checkpoints and logs after recovery and before serving writes after election. Expiry and retirement are replicated. A request for forgotten state uses the Kafka recovery error path to renew producer state. It is not accepted as a duplicate or silently appended under the old sequence. State has a hard admission budget and must not be evicted in a way that makes a stale retry match a new producer.
- **Classic groups.** Hash group ID to an internal partition whose leader acts as coordinator. Its Raft log persists stable generations, assignments and committed offsets. Keep transient joins and heartbeat deadlines in memory with bounded limits. On leadership change fence pending operations from the old coordinator and require members to rejoin when liveness is uncertain. Eager and cooperative assignment stays in the clients. The coordinator implements their classic generation and join/sync rules and rejects stale managed commits. Also support the Kafka offset path for an explicitly assigned consumer without a managed generation. Four internal group shards migrate alongside user partitions. [Distributed classic consumer groups](cluster-groups.md)
- **Offset retention.** An empty group retains offsets for seven days by default. The active coordinator proposes expiration through the replicated log after checking the current stable state. The coordinator serializes expiration and commits on each shard, and expiration is proposed only against applied state with no pending proposal. Fetch below the retained start or with a missing committed offset follows the client's configured reset behavior. End-to-end at-least-once requires the consumer application to commit after processing and tolerate repeats.

The single-node coordinator uses a local segmented journal for group generations and offset commits. Its checksummed checkpoint preserves offsets and generation fences before old journal segments are removed. Membership liveness is re-established by a new join after restart, while consumers resume from their committed offsets.

## Node lifecycle and migration

The current cluster stores a fixed volume identity, term, vote and durable log for each Raft group. It replicates the topic catalog across the startup roster and writes partition records to the replica sets recorded in catalog events. A restarting replica catches up from retained entries. The control log and each partition log have independent elections. Brokers without a partition replica learn its leader from expiring reports authenticated by mTLS. Producer IDs use a durable per-node counter and a node prefix. Reuse a node's existing volume on restart.

A provisioned broker registers over the authenticated peer channel. Operators can stage a node on a running broker through the private admin socket. Each stage-peer operation stores the endpoint specification and its SHA-256 binding to a CA-validated leaf on that broker's volume. Current controller members report that binding over mTLS. The controller waits for matching reports before admitting a dynamically staged node, adds it as a learner and promotes it after durable catch-up. Kafka metadata and new topic placement use committed voters.

A joining broker proves its node identity using mTLS, the expected cluster ID and the original bootstrap voter mask. A data volume binds to its original cluster, node ID and bootstrap voter list. The controller permanently records retired IDs. Certificate rotation updates pinned identities under the stopped broker's volume lock. See the [operator procedures](../README.md#rotate-peer-certificates).

The current decommission command persists its request on the departing broker and sends it over the authenticated peer transport. The controller commits the move plan and serializes partition moves. Each partition commits separate single-member Raft changes for learner addition, promotion and removal. The temporary voting set contains both the source and the replacement, so each transition has overlapping majorities. The partition leader checks durable progress on the replacement and a majority of the surviving voters before removal. The controller publishes the new placement only after a surviving partition leader reports the committed membership. It removes the departing controller voter after all partition moves finish and records the retired broker ID in the controller log. Recovery resumes active plans and reinstalls snapshots when a replacement needs a compacted prefix.

An operator-requested rebalance uses the same relocation state machine. It
balances replica counts across committed controller voters and retains each
partition's replication factor. The controller commits the full plan before
accepting it, and a plan UUID scopes partition progress reports. Decommission
and rebalance share one active plan and relocate one replica at a time.
Learner append traffic has an 8 MiB/s per-leader budget and a 3 MiB burst.

Cluster operators query committed placement progress and request rebalance or
leader transfer through a local Unix socket in the broker's private data
directory. The controller accepts rebalances. The partition leader starts
explicit transfer after checking that the target is a caught-up voter. The
caller observes the requested leader before success. A committed rebalance
resumes after restart or a destination outage.

For singleton conversion, stop clients and the source broker and import into fresh voter volumes. The operator supplies the destination cluster ID. The importer locks both volumes, records a durable source fence and builds Raft files in a staging directory. It publishes the synced directory before a target can start. Retried commands rebuild unpublished work or finish marking published work ready. The initial entries record the original topic IDs, retained offset bases, producer retry state and group commits. The controller puts new producer IDs above the singleton's reserved range. The target bootstrap voters all receive the same committed prefix before startup. See the [migration and recovery procedure](../README.md#migrate-single-node-data-to-a-cluster).

## Memory and overload

Admission is measured in bytes and object counts. Charge storage read buffers, request frames, response write buffers, decompression workspace, Raft in-flight data and snapshot chunks through explicit budgets. Hold permits until the last owner releases the bytes. Stop reading from a connection before buffering a frame whose reservation cannot be obtained. Bound decoded array lengths, per-connection work, waiters, group members, producer states, file descriptors and outstanding I/O. Limit CPU and disk work independently. Reserve control bandwidth and memory for heartbeats, metadata and group operations. Bound time spent waiting for memory or writing to a slow socket.

The initial sizing targets are 64 MiB single-node idle RSS and 128 MiB cluster-node idle RSS. Measure empty idle and idle after 100 partitions, 10 groups and 100 client connections. Exercise 256 MiB single-node and 512 MiB per cluster node cgroups under sustained load. These figures include kernel-accounted memory for the load tests. The in-process buffer budget is smaller and set independently because the cgroup also charges page cache, socket memory and allocator overhead. Report process RSS and heap alongside cgroup peak, anonymous memory, file cache and socket buffers. Tune I/O batches, limits and allocator settings on observed profiles. Baseline I/O uses bounded workers and ordinary file operations. Consider mmap, sendfile and io_uring only for a measured bottleneck. [Linux memory accounting](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files)

## Deployment on Linux and MacBook

Docker Desktop runs the Linux container on macOS. The multi-platform image provides the ARM variant for Apple silicon and the x86-64 variant for Intel. Compose leaves platform selection to Docker. Kafka clients can run on macOS or in Compose. Use separate host and Docker-network Kafka listeners with distinct advertised endpoints. In the host profile brokers advertise loopback with unique published ports per broker. In the container profile they advertise Compose service names. Internal mTLS endpoints remain on the Compose network. Persist broker data in a named volume per node. Mount certificate material read-only. Build an image that runs as a non-root user with a writable volume and no required privileged container capabilities.

The Mac acceptance test starts the singleton and the three-node Compose profile, connects a Java client from the host and a client from the Compose network, writes and consumes, then recreates a container with its volume and verifies records and committed offsets. Check one-broker termination and leader refresh in the cluster profile. Run this test on Apple silicon and verify the Intel path before promising it as tested. Linux provides the comparative RSS and performance baseline. Separate Linux hosts and controlled disk faults validate production durability. MacBook smoke tests cover the local workflow and share one physical failure domain. [Docker Desktop on Mac](https://docs.docker.com/desktop/setup/install/mac-install/), [multi-platform images](https://docs.docker.com/build/building/multi-platform/), [Docker volumes](https://docs.docker.com/engine/storage/volumes/)

## Roadmap and verification gates

| Iteration | Deliverable and gate |
|---|---|
| 1. Storage | Versioned format, durable singleton append, offset and time lookup, restart and retention. Test torn writes, segment creation and file-sync ordering. Prototype the Raft disk adapter before freezing the format. |
| 2. Kafka data path | Network framing, version negotiation, Admin topic creation, produce, fetch and idempotence. Run wire fixtures plus Java client tests with stock producer defaults. Exercise every codec with bounded decompression and large-batch progress. |
| 3. Groups | Classic joins, rebalances, commits and expiry. Run Java and Spring Kafka tests against Rusta, then pin and run the chosen Go, Python and Rust clients. Verify offsets and producer retries across restart. |
| 4. Cluster | Durable Raft replication, mTLS, group coordinator failover, learner promotion, balancing and conversion. Exercise split network, leader stalls, lost responses, power-loss simulation, interrupted snapshots and interrupted conversion. Check the acknowledged prefix and verify the minority cannot authorize new reads or writes. |
| 5. Packaging | Non-root multi-platform GHCR image, Compose, English README and Java/Spring examples. Verify both images on Linux, exercise Compose on macOS, and verify recreation keeps volumes and advertised endpoints reachable. CI runs rustfmt, Clippy, unit and integration suites. |
| 6. Release validation | Reproducible comparison with Apache Kafka, Redpanda, Apache Iggy and NATS JetStream. Pin image digests and settings. Publish RF1 and RF3 stable-write profiles, identifying ACK copy counts, hardware, filesystem and sync settings. Exercise message sizes, partition counts and connection counts. Measure RSS, cgroup peak, CPU, throughput, p50/p95/p99, disk I/O, launch, recovery, replication and adding a node. Publish Linux release numbers only after correctness and single-node idle RSS gates pass. |

Validate the disk adapter first. Durable commit across log rotation, a bounded snapshot install, and preserving the retry window after owner failover are the highest-risk integration points. Throughput and tail-latency acceptance thresholds will be set from the first comparable measurements.
