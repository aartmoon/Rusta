# Kafka API contract for Rusta 0.1

**Status:** Stage 2 design proposal. The ranges below are the exact planned advertisement for the first compatible release. The running broker advertises a range only when its handlers and declared semantics are implemented and tested. The primary client target is `kafka-clients` 4.1.2 with Spring Kafka 4.0.x. Also pin and verify franz-go, confluent-kafka and rust-rdkafka before claiming their compatibility. [Kafka 4.1 protocol](https://kafka.apache.org/41/design/protocol/), [Kafka 4.1.2 message schemas](https://github.com/apache/kafka/tree/4.1.2/clients/src/main/resources/common/message), [Spring compatibility](https://spring.io/projects/spring-kafka/)

The single-node data path currently advertises `ApiVersions`, `Metadata`, `CreateTopics`, `Produce`, `Fetch`, `ListOffsets`, `InitProducerId` and `OffsetForLeaderEpoch`. Consumers can assign partitions directly. Group APIs and the remaining AdminClient operations in the table are part of the release plan.

| Key | API | Inclusive versions | First release behavior |
|---:|---|---:|---|
| 0 | Produce | 9–11 | Kafka record-batch v2, keys, values, headers, CreateTime, five codec modes, optional weaker ACKs, producer sequence validation and deduplication |
| 1 | Fetch | 12–13 | Consumer fetch by name in v12 or topic ID in v13. Full requests with no server-side incremental fetch sessions, committed-prefix visibility and bounded long polling |
| 2 | ListOffsets | 6–7 | Earliest, latest and timestamp lookup on the retained range |
| 3 | Metadata | 9–12 | Bootstrap and router endpoints, topics, IDs, partition leaders and leader epochs. Honor topic-ID requests where defined. Report assigned replicas and observed in-sync replicas, with durable-majority acknowledgement defined separately |
| 8 | OffsetCommit | 8–9 | Managed classic generation checks and manual assignment. Durable commits in the group partition |
| 9 | OffsetFetch | 6–9 | Per-group and batched supported forms. Return committed offsets and metadata |
| 10 | FindCoordinator | 3–4 | Classic group coordinators, including batched lookup in v4 |
| 11 | JoinGroup | 6–9 | Dynamic classic membership, join handshake and rebalance generations |
| 12 | Heartbeat | 4 | Dynamic member liveness and generation fencing |
| 13 | LeaveGroup | 4–5 | Dynamic departure, including batched member identities and per-member errors |
| 14 | SyncGroup | 4–5 | Client-assigned classic generations and assignment distribution |
| 15 | DescribeGroups | 5–6 | Classic group state, protocol, members and assignments |
| 16 | ListGroups | 3–5 | Classic groups with the state and type filters applicable to the request version |
| 18 | ApiVersions | 0–4 | Correct request and response header handling, feature discovery and unsupported-version fallback |
| 19 | CreateTopics | 5–7 | Explicit creation, requested partition count and replication factor for supported deployment profiles, supported topic configs and validate-only |
| 20 | DeleteTopics | 4–6 | Durable metadata removal with retry-safe local cleanup, including requests by ID where defined |
| 22 | InitProducerId | 2–4 | Nontransactional producer IDs and epochs, including the epoch refresh fields where defined |
| 23 | OffsetForLeaderEpoch | 4 | Epoch-end offsets on the retained log for leader-change recovery |
| 32 | DescribeConfigs | 4 | Read supported topic configs and their effective sources |
| 44 | IncrementalAlterConfigs | 1 | Topic retention updates, including validation and the supported set/delete operations |

Basic AdminClient topic listing and describe-by-name, and cluster discovery via bootstrap brokers, use `Metadata` or the Java fallback to it. Test those fallbacks, along with the classic group describe and offset flows. `CreatePartitions`, `DescribeTopicPartitions`, `DescribeCluster`, transaction APIs, SASL APIs and the newer consumer group protocol remain outside the planned advertisement. Manual topic creation is the default. The Rusta policy supports `retention.ms` and `retention.bytes`; any Rusta-specific option uses a distinct name. Kafka ISR fields describe replica state. Rusta does not expose `min.insync.replicas` as an all-ISR policy switch. [KafkaAdminClient 4.1.2](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java), [Spring KafkaAdmin](https://docs.spring.io/spring-kafka/reference/kafka/configuring-topics.html)

## Wire and semantic cases

- Exercise ApiVersions bootstrap and fallback, flexible headers and tagged fields, correlation IDs, fragmented TCP frames, disconnect after a submitted write, out-of-order RPC completion and protocol-appropriate rejections. A known schema with an unsupported feature receives a suitable error. When a request cannot be decoded or answered safely, close the connection instead of constructing an incompatible response. Validate frame size before allocation and bound decoded collections and expanded batches.
- Return accurate errors for stale leaders or coordinators, fenced generations, unsupported static membership, invalid offsets, mismatched topic IDs, unsupported configs and non-empty transactional IDs. Never report success for an operation outside the implemented contract. `ApiVersions` is generated from the actual handler registry. Its higher-version negotiation fallback uses the special version-zero response and response header.
- Idempotent producer success must use the original offset on replay of the accepted batch. Test sequence wraparound, epoch changes, five concurrent batches, failover between durable commit and response, and reconstructing retry state after retention.
- For full Fetch with session ID 0, test Java, franz-go and librdkafka negotiation and retry behavior. Validate `fetch.min.bytes`, deadlines, partition caps, the first eligible batch progress rule and high watermark behavior. Treat transactional reading as outside the supported application scenarios. Ordinary batches are visible only below the durable high watermark.
- Spring tests use a real `NewTopic` bean and a listener against Rusta. Test produce/consume, restart with a committed offset and group reassignment with two consumer instances. An existing topic that Spring wants to expand returns the unsupported operation outcome.
- The wire codec candidate is `kafka-protocol` with a pinned release or revision. Validate its schema coverage against 4.1.2 and its allocation behavior under bounded inputs before using it on the broker endpoint. Rusta handles record validation and broker semantics explicitly. [Rust Kafka protocol codec](https://github.com/kafka-protocol-rs/kafka-protocol-rs), [Kafka Java Fetch behavior](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/FetchSessionHandler.java)
