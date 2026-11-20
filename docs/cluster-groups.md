# Distributed classic consumer groups

The controller records one placement for four internal Raft shards using the
committed broker roster. Each shard starts with up to three voters, with
placement rotated across the roster. Decommission moves these replicas with
the existing learner and promotion workflow, preserving RF. Existing clusters
enable the shards with a coordinated broker restart on their existing volumes.

The shard number is FNV-1a 64-bit over the UTF-8 bytes of the group ID, modulo
four. Shard identities are reserved for internal use and absent from Kafka
topic metadata. Changing the mapping requires a migration.

## Requests and leadership

1. A bootstrap broker answers FindCoordinator with the reported shard leader
   and the advertised endpoint for the receiving listener.
2. JoinGroup registers a transient member. The leader persists the next
   generation before replying with the member list and selected protocol.
3. SyncGroup records the stable generation and assignments before replying to
   the leader and the other waiting members.
4. Heartbeat extends the member deadline at the current coordinator. The
   coordinator checks leadership via a quorum barrier before accepting it.
5. OffsetCommit validates the generation and offsets, proposes a shard event
   and acknowledges only after durable majority and local apply.
6. OffsetFetch uses Safe ReadIndex, waits for local apply and checks that the
   serving coordinator is still in the same term.

Shard journal events carry a format version and include generation changes,
stable assignments, commits, empty transitions and expiration. Deadlines and
pending joins are local to the current leader. A leadership change drops
sessions and fences old work. Clients rediscover the coordinator and rejoin
with a new generation. The new leader replays committed offsets before serving
requests. Shards retain the Raft log until the cluster snapshot work in
iteration 2.

## Acceptance

- A Java Kafka Client 4.1.2 consumer uses subscribe, commits, and resumes from
  its saved position on three brokers. Spring Kafka 4.0.8 uses two consumers
  and recovers assignment after coordinator loss.
- A commit interrupted before replication to a durable majority leaves the
  previous offset after failover. A commit fixed by the majority before the
  response is lost remains visible after failover. A confirmed commit remains
  visible after failover and a full restart, until superseded or expired.
- An isolated minority cannot confirm commits or authorize new reads. A retry
  may observe a commit whose response was lost. Rejoining increments the
  generation and stale managed commits fail. At-least-once grants applications
  the possibility of redelivery after failover.
- Exercise LeaveGroup, group Admin, both OffsetFetch layouts and membership
  relocation. Advertise tested group API versions only.
