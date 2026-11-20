# Kafka API contract for Rusta 0.1

Both broker modes advertise the ranges below. The registry includes the classic-group and Admin versions required by librdkafka 2.12.1 as well as the flexible formats used by Kafka Java Client 4.1.2. [Kafka 4.1.2 message schemas](https://github.com/apache/kafka/tree/4.1.2/clients/src/main/resources/common/message), [librdkafka 2.12.1 request implementation](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_request.c)

Cluster topics default to RF3 when at least three brokers are configured and support an explicit replication factor up to the node count. Cluster topics default to unlimited retention. A configured `retention.ms` or `retention.bytes` advances the Kafka-visible start after a durable Raft commit. Replicas reclaim segments after saving a snapshot of the removed prefix. Cluster writes require `acks=all`. Full cluster Fetch requests support bounded long polling and use a fresh read barrier on each attempt to return data. Fetch below the retained start returns `OFFSET_OUT_OF_RANGE`. Group requests target the shard leader returned by `FindCoordinator`. In a batched OffsetFetch, each group addressed to another coordinator gets its own retryable error. See [distributed classic consumer groups](cluster-groups.md).

| Key | API | Inclusive versions | First release behavior |
|---:|---|---:|---|
| 0 | Produce | 9–11 | Kafka record-batch v2, keys, values, headers, CreateTime, five codec modes, optional weaker ACKs, producer sequence validation and deduplication |
| 1 | Fetch | 12–13 | Consumer fetch by name in v12 or topic ID in v13. Full requests with no server-side incremental fetch sessions, committed-prefix visibility and bounded long polling |
| 2 | ListOffsets | 6–7 | Earliest, latest and timestamp lookup on the retained range |
| 3 | Metadata | 9–12 | Bootstrap and router endpoints, topics, IDs, partition leaders and leader epochs. Honor topic-ID requests where defined. Report assigned replicas and observed in-sync replicas, with [quorum acknowledgement modes](operations.md#page-cache-acknowledgements) defined separately |
| 8 | OffsetCommit | 8–9 | Managed classic generation checks and manual assignment. Durable commits in the group partition |
| 9 | OffsetFetch | 6–9 | Per-group and batched supported forms. Return committed offsets and metadata |
| 10 | FindCoordinator | 2–4 | Classic group coordinators, including batched lookup in v4 |
| 11 | JoinGroup | 5–9 | Dynamic classic membership, join handshake and rebalance generations |
| 12 | Heartbeat | 3–4 | Dynamic member liveness and generation fencing |
| 13 | LeaveGroup | 1–5 | Dynamic departure, including batched member identities and per-member errors |
| 14 | SyncGroup | 3–5 | Client-assigned classic generations and assignment distribution |
| 15 | DescribeGroups | 5–6 | Classic group state, protocol, members and assignments |
| 16 | ListGroups | 3–5 | Classic groups with the state and type filters applicable to the request version |
| 18 | ApiVersions | 0–4 | Correct request and response header handling, feature discovery and unsupported-version fallback |
| 19 | CreateTopics | 4–7 | Explicit creation, requested partition count and replication factor for supported deployment profiles, supported topic configs and validate-only |
| 20 | DeleteTopics | 1–6 | Durable metadata removal with retry-safe local cleanup, including requests by ID where defined |
| 22 | InitProducerId | 2–4 | Nontransactional producer IDs and epochs, including the epoch refresh fields where defined |
| 23 | OffsetForLeaderEpoch | 2–4 | Epoch-end offsets on the retained log for leader-change recovery |
| 32 | DescribeConfigs | 1–4 | Read supported topic configs and their effective sources |
| 44 | IncrementalAlterConfigs | 1 | Topic retention updates, including validation and the supported set/delete operations |

Basic AdminClient topic listing and describe-by-name, and cluster discovery via bootstrap brokers, use `Metadata` or the Java fallback to it. Test those fallbacks, along with the classic group describe and offset flows. `CreatePartitions`, `DescribeTopicPartitions`, `DescribeCluster`, transaction APIs, SASL APIs and the newer consumer group protocol remain outside the planned advertisement. Manual topic creation is the default. The Rusta policy supports `retention.ms` and `retention.bytes`; any Rusta-specific option uses a distinct name. Kafka ISR fields describe replica state. Rusta does not expose `min.insync.replicas` as an all-ISR policy switch. [KafkaAdminClient 4.1.2](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java), [Spring KafkaAdmin](https://docs.spring.io/spring-kafka/reference/kafka/configuring-topics.html)

## Topic administration

`IncrementalAlterConfigs` supports SET and DELETE on `retention.ms` and `retention.bytes`. A resource with an unsupported key, operation or value is rejected as a unit. Validation leaves settings unchanged. Deleting an override restores the deployment default. When a cluster first enables time retention on a topic with legacy batches lacking an append timestamp, it uses the committed activation time as the fallback for those batches.

`DescribeConfigs` serves the broker's locally applied topic configuration. A follower can briefly lag a change committed by the controller. The response reports the effective settings, their sources and requested synonyms. `cleanup.policy` and `rusta.allow.weak.acks` are read-only through this API.

A successful delete commits metadata removal and completes cleanup on the handling broker. Each cluster member removes its local partition files on apply or recovery. Topic IDs remain unique when a name is reused. A repeat against an absent topic returns `UNKNOWN_TOPIC_OR_PARTITION` by name or `UNKNOWN_TOPIC_ID` by ID. Use the ID to retry against the original topic if the name might have been reused. Existing group offsets are keyed by topic ID and expire through normal group retention. A delete that intersects an active replica relocation can time out and be retried after the move.

Partition expansion requires distinct controller state for new partition IDs, initial replica placement and activation. Replica relocation preserves the existing partition count and IDs. Java smoke verifies that `CreatePartitions` returns an unsupported-version outcome and publishes no change.

## Verified clients

| Client | Tested listener | Verified flows |
| --- | --- | --- |
| Kafka Java Client 4.1.2 | Host and Compose | Create/list/describe/delete, config SET/DELETE and validate-only, Produce/Fetch, exact duplicate batch replay, classic groups, committed offsets, leader failover and restart |
| Spring Kafka 4.0.8 | Host | Topic creation and listener consumption, coordinator failover and resuming from committed offsets |
| franz-go 1.20.7 with kmsg 1.12.0 | Host | Create/list/describe, idempotent Produce and Fetch across partition leader failure, classic group commit and resume |
| confluent-kafka Python 2.12.2 with librdkafka 2.12.1 | Host | Create/list/describe/delete and DescribeConfigs, idempotent Produce and Fetch across partition leader failure, classic group commit and resume |
| rust-rdkafka 0.38.0 with librdkafka 2.12.1 | Compose | Create/list/describe/delete and DescribeConfigs, idempotent Produce and Fetch across partition leader failure, classic group commit and resume |

The compatibility smoke keeps the producer alive across the loss of a partition leader, checks both delivered offsets, and uses a new consumer session to check recovery from the committed offset. A wire probe tests split TCP writes, ApiVersions v0–4, unknown flexible tags, the version-zero fallback for a future version, and rejection of unadvertised and oversized requests. The broker accepts the four-zero-byte null topic array produced by librdkafka 2.12.1 for all-topic Metadata. [Client fixtures](../tests/compat/), [Java smoke fixtures](../tests/java/)

## Wire and semantic cases

- Exercise ApiVersions bootstrap and fallback, flexible headers and tagged fields, correlation IDs, fragmented TCP frames, disconnect after a submitted write, out-of-order RPC completion and protocol-appropriate rejections. A known schema with an unsupported feature receives a suitable error. When a request cannot be decoded or answered safely, close the connection instead of constructing an incompatible response. Validate frame size before allocation and bound decoded collections and expanded batches.
- Return accurate errors for stale leaders or coordinators, fenced generations, unsupported static membership, invalid offsets, mismatched topic IDs, unsupported configs and non-empty transactional IDs. Never report success for an operation outside the implemented contract. `ApiVersions` is generated from the actual handler registry. Its higher-version negotiation fallback uses the special version-zero response and response header.
- Idempotent producer success must use the original offset on replay of the accepted batch. Test sequence wraparound, epoch changes, five concurrent batches, failover between durable commit and response, and reconstructing retry state after retention.
- For full Fetch with session ID 0, test Java, franz-go and librdkafka negotiation and retry behavior. Validate `fetch.min.bytes`, deadlines, partition caps, the first eligible batch progress rule and high watermark behavior. Treat transactional reading as outside the supported application scenarios. Ordinary batches are visible only below the durable high watermark.
- Spring tests use a real `NewTopic` bean and a listener against Rusta. Test produce/consume, restart with a committed offset and group reassignment with two consumer instances. An existing topic that Spring wants to expand returns the unsupported operation outcome.
- The wire codec candidate is `kafka-protocol` with a pinned release or revision. Validate its schema coverage against 4.1.2 and its allocation behavior under bounded inputs before using it on the broker endpoint. Rusta handles record validation and broker semantics explicitly. [Rust Kafka protocol codec](https://github.com/kafka-protocol-rs/kafka-protocol-rs), [Kafka Java Fetch behavior](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/FetchSessionHandler.java)
