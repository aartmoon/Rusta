use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use protobuf::Message as ProtoMessage;
use raft::{
    Config, RawNode, SnapshotStatus, StateRole,
    eraftpb::{ConfChange, ConfChangeType, EntryType, Message, MessageType},
};
use rusta_kafka::records::Batch;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore, oneshot};
use uuid::Uuid;

use crate::{
    catalog::{MAX_PARTITIONS, TopicConfig, remove_topic_data},
    groups::{self, Completion, Group, Groups},
    partition::MAX_BATCH,
    protocol::Error,
};

use super::{
    Peer, Settings, ack_profile,
    disk::{Disk, decode_change, encode_batch, encode_retention, replace},
    producers,
    transport::{Packet, Queued, Senders},
};

#[path = "migration.rs"]
pub mod migration;
#[path = "recovery.rs"]
mod recovery;
#[path = "snapshots.rs"]
mod snapshots;
use recovery::RecoveryOffer;
use snapshots::{ControlState, GroupState};

type Reply<T> = oneshot::Receiver<Result<T, Error>>;
type GroupOperation<T> = (T, Option<Reply<i64>>);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupId {
    pub topic: Uuid,
    pub partition: u32,
}

pub const CONTROL: GroupId = GroupId {
    topic: Uuid::nil(),
    partition: 0,
};

const GROUP_SHARDS: usize = 4;
const MAX_PENDING_BATCHES: usize = 4;
const MAX_UNCOMMITTED_BYTES: usize = 2 * MAX_BATCH;
const PAGE_CACHE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

fn group_shard(index: usize) -> GroupId {
    GroupId {
        topic: Uuid::nil(),
        partition: index as u32 + 1,
    }
}

pub fn coordinator(id: &str) -> GroupId {
    let hash = id.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    group_shard((hash % GROUP_SHARDS as u64) as usize)
}

fn is_group_shard(key: GroupId) -> bool {
    key.topic.is_nil() && (1..=GROUP_SHARDS as u32).contains(&key.partition)
}

#[derive(Deserialize, Serialize)]
enum ShardRecord {
    V1(Vec<groups::Event>),
}

struct GroupShard {
    committed: HashMap<String, Group>,
    active: Option<(u64, Groups)>,
    completions: Vec<Completion>,
}

const MAX_CLUSTER_PARTITIONS: usize = 128;
const MAX_READ_BARRIERS: usize = 16;

const RELOCATION_RATE: u64 = 8 * 1024 * 1024;
const RELOCATION_BURST: u64 = 3 * 1024 * 1024;

struct RelocationBudget {
    bytes: u64,
    updated: Instant,
}

impl RelocationBudget {
    fn admit(&mut self, bytes: u64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.updated).as_micros();
        let refill = (elapsed * u128::from(RELOCATION_RATE) / 1_000_000)
            .min(u128::from(RELOCATION_BURST)) as u64;
        self.bytes = self.bytes.saturating_add(refill).min(RELOCATION_BURST);
        self.updated = now;
        if self.bytes < bytes {
            return false;
        }
        self.bytes -= bytes;
        true
    }
}

struct DrainContext<'a> {
    outbound: &'a Senders,
    peers: &'a BTreeMap<u64, Peer>,
    send_bytes: &'a Arc<Semaphore>,
    data_available: &'a Notify,
    group_progress: &'a Notify,
    local_id: u64,
    group_retention_ms: u64,
}

struct Drained {
    events: Vec<Event>,
    control_snapshot: Option<snapshots::ControlState>,
    completions: Vec<(Pending, Option<Instant>)>,
    local_leader: bool,
    term: u64,
}

pub struct Engine {
    pub data_available: Arc<Notify>,
    pub group_progress: Arc<Notify>,
    pub settings: Settings,
    pub topics: BTreeMap<String, TopicConfig>,
    pub names: HashMap<Uuid, String>,
    pub nodes: BTreeMap<GroupId, Node>,
    outbound: Arc<Senders>,
    send_bytes: Arc<Semaphore>,
    replicas: HashMap<GroupId, Vec<u64>>,
    origins: HashMap<GroupId, Vec<u64>>,
    imported_offsets: HashMap<GroupId, i64>,
    imported_producer_floor: i64,
    retired: BTreeSet<u64>,
    relocation: Option<RelocationPlan>,
    last_rebalance: Option<Uuid>,
    drain_requests: BTreeSet<u64>,
    move_ready: bool,
    relocation_budget: RelocationBudget,

    leaders: HashMap<GroupId, (u64, i32, Instant)>,
    last_report: Instant,
    join_requests: BTreeMap<u64, Instant>,
    next_barrier: Arc<AtomicU64>,
    next_producer: u64,
    reserved_producer: u64,
    data: PathBuf,
    group_retention_ms: u64,
    producer_retention_ms: u64,
    peer_readiness: HashMap<u64, BTreeMap<u64, [u8; 32]>>,
    compaction_cursor: Option<GroupId>,
    internal_snapshot_interval: u64,
    rebuilding_catalog: bool,
    pub(super) closing: bool,
}

pub struct Node {
    raw: RawNode<Disk>,
    pending: VecDeque<Pending>,
    reads: HashMap<Vec<u8>, Read>,
    committed_term: u64,
    group: Option<GroupShard>,
    fetch_leadership: (u64, u64),
    snapshot_deadlines: HashMap<u64, Instant>,
    cache_flushed_at: Option<Instant>,
    recovering: bool,
    recovery_offer: Option<RecoveryOffer>,
    recovery_requests: HashMap<u64, (Uuid, u64)>,
}

struct Pending {
    index: u64,
    term: u64,
    reply: oneshot::Sender<Result<i64, Error>>,
    offset: i64,
    batch: Option<Batch>,
    bytes: usize,
    proposed_at: Option<Instant>,
    persisted_at: Option<Instant>,
}

struct Read {
    term: u64,
    authorized: Option<u64>,
    reply: oneshot::Sender<Result<(), Error>>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Movement {
    pub key: GroupId,
    pub from: u64,
    pub to: u64,
}

#[derive(Deserialize, Serialize)]
struct RelocationPlan {
    #[serde(default)]
    id: Uuid,
    node: Option<u64>,
    movements: Vec<Movement>,
    next: usize,
}

impl RelocationPlan {
    fn active(&self) -> Option<Movement> {
        self.movements.get(self.next).copied()
    }
}

#[derive(Deserialize, Serialize)]
enum Event {
    Topic(TopicConfig),
    Drain(RelocationPlan),
    Rebalance(RelocationPlan),
    Rebalanced(Uuid),
    Moved(Movement),
    Drained(u64),
    GroupShards([Vec<u64>; GROUP_SHARDS]),
    PlacedTopic {
        config: TopicConfig,
        replicas: Vec<Vec<u64>>,
    },
    ImportedTopic {
        config: TopicConfig,
        replicas: Vec<Vec<u64>>,
        starts: Vec<i64>,
    },
    ImportedProducers(i64),
    AlterTopic(TopicConfig),
    DeleteTopic {
        name: String,
        id: Uuid,
    },
}

impl Engine {
    pub fn open(settings: Settings, root: &Path, outbound: Senders) -> Result<Self> {
        fs::create_dir_all(root)?;
        let data = root.join("raft");
        fs::create_dir_all(&data)?;
        let control = Node::open(
            &settings,
            &data.join("control"),
            settings.bootstrap_voters.clone(),
            CONTROL,
            0,
        )?;
        let membership = &control.raw.store().membership;
        ensure!(
            membership
                .voters
                .iter()
                .chain(&membership.learners)
                .all(|id| settings.peers.contains_key(id)),
            "peer configuration is missing a committed cluster member"
        );
        let counter = read_counter(root)?;
        let mut engine = Self {
            data_available: Arc::new(Notify::new()),
            group_progress: Arc::new(Notify::new()),
            settings,
            topics: BTreeMap::new(),
            names: HashMap::new(),
            nodes: BTreeMap::new(),
            outbound: Arc::new(outbound),
            send_bytes: Arc::new(Semaphore::new(32 * 1024 * 1024)),
            replicas: HashMap::new(),
            origins: HashMap::new(),
            imported_offsets: HashMap::new(),
            imported_producer_floor: 0,
            retired: BTreeSet::new(),
            relocation: None,
            last_rebalance: None,
            drain_requests: BTreeSet::new(),
            move_ready: false,
            relocation_budget: RelocationBudget {
                bytes: RELOCATION_BURST,
                updated: Instant::now(),
            },

            leaders: HashMap::new(),
            last_report: Instant::now(),
            join_requests: BTreeMap::new(),
            next_barrier: Arc::new(AtomicU64::new(0)),
            closing: false,
            next_producer: counter + 1,
            reserved_producer: counter,
            data,
            group_retention_ms: groups::retention_ms()?,
            producer_retention_ms: producers::retention_ms()?,
            peer_readiness: HashMap::new(),
            compaction_cursor: None,
            internal_snapshot_interval: snapshots::interval()?,
            rebuilding_catalog: true,
        };
        if let Some(state) = control.raw.store().internal_state::<ControlState>()? {
            engine.restore_control(state)?;
        }
        let log = &control.raw.store().log;
        let mut index = log.start().next_index;
        let end = log.committed().next_index;
        while index < end {
            index = log.visit_entries(index, end, MAX_BATCH as u64, |_, payload| {
                if !payload.is_empty()
                    && decode_change(payload)
                        .map_err(std::io::Error::other)?
                        .is_none()
                {
                    let event = serde_json::from_slice(payload)?;
                    engine.apply(event).map_err(std::io::Error::other)?;
                }
                Ok(())
            })?;
        }
        engine.rebuilding_catalog = false;
        engine.open_current_replicas()?;
        engine.open_destination()?;
        engine.remove_deleted_topic_data()?;
        engine.nodes.insert(CONTROL, control);
        Ok(engine)
    }

    pub fn node_id(&self) -> u64 {
        self.settings.id
    }

    pub fn topic_key(&self, name: &str, partition: i32) -> Result<GroupId, Error> {
        let config = self.topics.get(name).ok_or(Error::UnknownTopic)?;
        if !(0..config.partitions as i32).contains(&partition) {
            return Err(Error::UnknownTopic);
        }
        Ok(GroupId {
            topic: config.id,
            partition: partition as u32,
        })
    }

    pub fn partitions(&self) -> usize {
        self.topics.values().map(|topic| topic.partitions).sum()
    }

    fn apply(&mut self, event: Event) -> Result<()> {
        let (config, placements, starts) = match event {
            Event::AlterTopic(config) => {
                let old = self
                    .topics
                    .get_mut(&config.name)
                    .context("altered topic missing")?;
                ensure!(
                    old.id == config.id && old.partitions == config.partitions,
                    "altered topic identity changed"
                );
                *old = config;
                return Ok(());
            }
            Event::DeleteTopic { name, id } => {
                let topic = self.topics.remove(&name).context("deleted topic missing")?;
                ensure!(topic.id == id, "deleted topic identity mismatch");
                self.names.remove(&id);
                for partition in 0..topic.partitions {
                    let key = GroupId {
                        topic: id,
                        partition: partition as u32,
                    };
                    self.nodes.remove(&key);
                    self.replicas.remove(&key);
                    self.origins.remove(&key);
                    self.imported_offsets.remove(&key);
                    self.leaders.remove(&key);
                    if !self.rebuilding_catalog {
                        remove_topic_data(
                            &self.data,
                            &self.data.join(format!("{id}-{partition}")),
                        )?;
                    }
                }
                self.data_available.notify_waiters();
                return Ok(());
            }
            Event::Topic(config) => {
                let members = self.settings.bootstrap_voters.clone();
                let placements = vec![members; config.partitions];
                (config, placements, None)
            }
            Event::PlacedTopic { config, replicas } => (config, replicas, None),
            Event::ImportedTopic {
                config,
                replicas,
                starts,
            } => {
                ensure!(
                    starts.len() == config.partitions && starts.iter().all(|&offset| offset >= 0),
                    "invalid migration offset bases"
                );
                (config, replicas, Some(starts))
            }
            Event::ImportedProducers(high) => {
                ensure!(
                    self.imported_producer_floor == 0 && high > 0 && high <= i64::MAX - (4096 << 8),
                    "invalid imported producer ID ceiling"
                );
                self.imported_producer_floor = high;
                self.reserved_producer = self.reserved_producer.max((high as u64) >> 8);
                self.next_producer = self.next_producer.max(self.reserved_producer + 1);
                return Ok(());
            }
            Event::GroupShards(placements) => {
                ensure!(
                    self.replicas(group_shard(0)).is_empty(),
                    "duplicate group shard placement"
                );
                for (index, voters) in placements.into_iter().enumerate() {
                    self.register(group_shard(index), voters, 0)?;
                }
                return Ok(());
            }
            Event::Drain(plan) => {
                ensure!(
                    plan.node.is_some_and(|id| !self.retired.contains(&id)),
                    "invalid drain plan"
                );
                self.start_relocation(plan)?;
                return Ok(());
            }
            Event::Rebalance(plan) => {
                ensure!(
                    !plan.id.is_nil() && plan.node.is_none(),
                    "invalid rebalance plan"
                );
                self.start_relocation(plan)?;
                return Ok(());
            }
            Event::Rebalanced(id) => {
                ensure!(
                    self.relocation
                        .as_ref()
                        .is_some_and(|plan| plan.node.is_none()
                            && plan.id == id
                            && plan.active().is_none()),
                    "unexpected rebalance completion"
                );
                self.last_rebalance = Some(id);
                self.relocation = None;
                return Ok(());
            }
            Event::Moved(movement) => {
                let plan = self
                    .relocation
                    .as_mut()
                    .context("placement update without a relocation plan")?;
                ensure!(plan.active() == Some(movement), "unexpected completed move");
                let replicas = self
                    .replicas
                    .get_mut(&movement.key)
                    .context("missing partition assignment")?;
                let source = replicas
                    .iter_mut()
                    .find(|id| **id == movement.from)
                    .context("missing source replica")?;
                *source = movement.to;
                if self.settings.id == movement.from
                    && let Some(mut node) = self.nodes.remove(&movement.key)
                {
                    node.raw.mut_store().log.flush_page_cache()?;
                }
                if self
                    .leaders
                    .get(&movement.key)
                    .is_some_and(|entry| entry.0 == movement.from)
                {
                    self.leaders.remove(&movement.key);
                }
                plan.next += 1;
                self.move_ready = false;
                self.open_destination()?;
                return Ok(());
            }
            Event::Drained(id) => {
                ensure!(
                    self.relocation
                        .as_ref()
                        .is_some_and(|p| p.node == Some(id) && p.active().is_none()),
                    "unexpected drain completion"
                );
                self.retired.insert(id);
                self.relocation = None;
                self.drain_requests.remove(&id);
                self.join_requests.remove(&id);
                return Ok(());
            }
        };
        ensure!(
            self.partitions() + config.partitions <= MAX_CLUSTER_PARTITIONS.min(MAX_PARTITIONS),
            "cluster partition limit exceeded"
        );
        ensure!(
            !self.topics.contains_key(&config.name) && !self.names.contains_key(&config.id),
            "duplicate committed topic"
        );
        ensure!(
            placements.len() == config.partitions,
            "invalid partition assignment count"
        );
        for (partition, voters) in placements.into_iter().enumerate() {
            let key = GroupId {
                topic: config.id,
                partition: partition as u32,
            };
            self.register(
                key,
                voters,
                starts.as_ref().map_or(0, |starts| starts[partition]),
            )?;
        }
        self.names.insert(config.id, config.name.clone());
        self.topics.insert(config.name.clone(), config);
        Ok(())
    }

    fn remove_deleted_topic_data(&self) -> Result<()> {
        for entry in fs::read_dir(&self.data)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some((topic, partition)) = name.to_str().and_then(|name| name.rsplit_once('-'))
                && let Ok(id) = Uuid::parse_str(topic)
                && !id.is_nil()
                && partition.parse::<u32>().is_ok()
                && !self.names.contains_key(&id)
            {
                remove_topic_data(&self.data, &entry.path())?;
            }
        }
        Ok(())
    }

    fn register(&mut self, key: GroupId, voters: Vec<u64>, initial_offset: i64) -> Result<()> {
        let unique: BTreeSet<_> = voters.iter().collect();
        ensure!(
            !self.replicas.contains_key(&key)
                && !voters.is_empty()
                && unique.len() == voters.len()
                && voters.iter().all(|id| self.settings.peers.contains_key(id)),
            "invalid replica assignment"
        );
        if !self.rebuilding_catalog && voters.contains(&self.settings.id) {
            let path = self.data.join(format!("{}-{}", key.topic, key.partition));
            self.nodes.insert(
                key,
                Node::open(&self.settings, &path, voters.clone(), key, initial_offset)?,
            );
        }
        if initial_offset != 0 {
            self.imported_offsets.insert(key, initial_offset);
        }
        self.origins.insert(key, voters.clone());
        self.replicas.insert(key, voters);
        Ok(())
    }

    fn open_current_replicas(&mut self) -> Result<()> {
        for (&key, voters) in &self.replicas {
            if voters.contains(&self.settings.id) {
                let path = self.data.join(format!("{}-{}", key.topic, key.partition));
                let initial_offset = self.imported_offsets.get(&key).copied().unwrap_or(0);
                self.nodes.insert(
                    key,
                    Node::open(
                        &self.settings,
                        &path,
                        self.origins[&key].clone(),
                        key,
                        initial_offset,
                    )?,
                );
            }
        }
        Ok(())
    }

    fn bootstrap_groups(&mut self) -> Result<()> {
        if !self.replicas(group_shard(0)).is_empty() || self.relocation.is_some() {
            return Ok(());
        }
        let mut voters = self.voters().to_vec();
        voters.sort_unstable();
        let node = self.nodes.get_mut(&CONTROL).expect("control group");
        if !node.admitting() || voters.is_empty() {
            return Ok(());
        }
        let factor = voters.len().min(3);
        let placements = std::array::from_fn(|shard| {
            (0..factor)
                .map(|replica| voters[(shard + replica) % voters.len()])
                .collect()
        });
        node.propose(serde_json::to_vec(&Event::GroupShards(placements))?, 0)?;
        self.drain(CONTROL)
    }

    fn start_relocation(&mut self, plan: RelocationPlan) -> Result<()> {
        ensure!(
            self.relocation.is_none() && plan.next == 0,
            "overlapping relocation plans"
        );
        self.relocation = Some(plan);
        self.move_ready = false;
        self.open_destination()
    }

    fn open_destination(&mut self) -> Result<()> {
        if self.rebuilding_catalog {
            return Ok(());
        }
        let Some(movement) = self.relocation.as_ref().and_then(RelocationPlan::active) else {
            return Ok(());
        };
        if self.settings.id == movement.to && !self.nodes.contains_key(&movement.key) {
            let key = movement.key;
            let path = self.data.join(format!("{}-{}", key.topic, key.partition));
            let origins = self
                .origins
                .get(&key)
                .context("missing partition bootstrap voters")?;
            self.nodes.insert(
                key,
                Node::open(
                    &self.settings,
                    &path,
                    origins.clone(),
                    key,
                    self.imported_offsets.get(&key).copied().unwrap_or(0),
                )?,
            );
        }
        Ok(())
    }

    pub fn placement_count(&self) -> usize {
        self.voters()
            .iter()
            .filter(|id| {
                self.relocation
                    .as_ref()
                    .is_none_or(|plan| plan.node != Some(**id))
            })
            .count()
    }

    pub fn receive_drain(&mut self, id: u64) -> Result<()> {
        if self.voters().contains(&id) && !self.retired.contains(&id) {
            self.drain_requests.insert(id);
        }
        Ok(())
    }

    pub fn receive_move(&mut self, reporter: u64, plan_id: Uuid, movement: Movement) -> Result<()> {
        if self
            .relocation
            .as_ref()
            .is_some_and(|plan| plan.id == plan_id && plan.active() == Some(movement))
            && reporter != movement.from
            && (reporter == movement.to || self.replicas(movement.key).contains(&reporter))
        {
            self.move_ready = true;
        }
        Ok(())
    }

    fn plan_drain(&self, from: u64) -> Option<RelocationPlan> {
        let mut available: Vec<_> = self
            .voters()
            .iter()
            .copied()
            .filter(|id| *id != from)
            .collect();
        if available.is_empty() {
            return None;
        }
        available.sort_unstable();
        let mut movements = Vec::new();
        let assignments: BTreeMap<_, _> = self.replicas.iter().collect();
        for (&key, replicas) in assignments {
            if replicas.contains(&from) {
                let to = available
                    .iter()
                    .copied()
                    .find(|id| !replicas.contains(id))?;
                movements.push(Movement { key, from, to });
                // Spread successive moves across the available brokers.
                available.rotate_left(1);
            }
        }
        Some(RelocationPlan {
            id: Uuid::new_v4(),
            node: Some(from),
            movements,
            next: 0,
        })
    }

    pub fn rebalance(&mut self, id: Uuid) -> Result<Result<RebalanceResult, String>> {
        let control = &self.nodes[&CONTROL];
        if !control.local_leader() {
            return Ok(Err(format!(
                "send rebalance to controller {}",
                control.leader()
            )));
        }
        if id.is_nil() {
            return Ok(Err("rebalance requires a nonzero request ID".into()));
        }
        if self.last_rebalance == Some(id) {
            return Ok(Ok(RebalanceResult::Completed));
        }
        if self.relocation.as_ref().is_some_and(|plan| plan.id == id) {
            return Ok(Ok(RebalanceResult::Active));
        }
        if self.relocation.is_some() || !self.drain_requests.is_empty() {
            return Ok(Err("another relocation is in progress or queued".into()));
        }
        if !control.admitting() {
            return Ok(Err("controller is busy, retry the same request ID".into()));
        }
        let movements = balance_movements(self.voters(), &self.replicas);
        if movements.is_empty() {
            return Ok(Ok(RebalanceResult::Balanced));
        }
        let record = Event::Rebalance(RelocationPlan {
            id,
            node: None,
            movements,
            next: 0,
        });
        let encoded = serde_json::to_vec(&record)?;
        if encoded.len() > MAX_BATCH {
            return Ok(Err(
                "the rebalance plan exceeds the controller entry limit".into()
            ));
        }
        let reply = self
            .nodes
            .get_mut(&CONTROL)
            .expect("controller group")
            .propose(encoded, 0)?;
        self.drain(CONTROL)?;
        Ok(Ok(RebalanceResult::Pending(reply)))
    }

    pub fn relocation_status(&self) -> serde_json::Value {
        let active = self.relocation.as_ref().map(|plan| {
            let movement = plan.active().map(|movement| {
                let (leader, _) = self.leader_view(movement.key);
                let node = self.nodes.get(&movement.key);
                let phase = if self.move_ready {
                    "committing_placement"
                } else if let Some(node) = node {
                    let members = &node.raw.store().membership;
                    if !members.voters.contains(&movement.from)
                        && members.voters.contains(&movement.to)
                    {
                        "committing_placement"
                    } else if members.voters.contains(&movement.to) {
                        "removing_source"
                    } else if members.learners.contains(&movement.to) {
                        "catching_up"
                    } else {
                        "adding_learner"
                    }
                } else {
                    "awaiting_replica_report"
                };
                serde_json::json!({
                    "topic": self.names.get(&movement.key.topic),
                    "topic_id": movement.key.topic,
                    "partition": movement.key.partition,
                    "from": movement.from,
                    "to": movement.to,
                    "leader": leader,
                    "partition_kind": if is_group_shard(movement.key) { "group_shard" } else { "topic" },
                    "phase": phase
                })
            });
            serde_json::json!({
                "id": plan.id,
                "kind": if plan.node.is_some() { "decommission" } else { "rebalance" },
                "retiring_broker": plan.node,
                "completed": plan.next,
                "total": plan.movements.len(),
                "movement": movement
            })
        });
        serde_json::json!({
            "broker": self.settings.id,
            "controller": self.nodes[&CONTROL].leader(),
            "active": active,
            "last_rebalance": self.last_rebalance,
        })
    }

    pub fn transfer_leadership(
        &mut self,
        topic: &str,
        partition: i32,
        target: u64,
    ) -> Result<Result<GroupId, String>> {
        let Ok(key) = self.topic_key(topic, partition) else {
            return Ok(Err("unknown topic or partition".into()));
        };
        let local = self.settings.id;
        let (leader, _) = self.leader_view(key);
        if leader != local {
            return Ok(Err(format!("send transfer to partition leader {leader}")));
        }
        if self
            .relocation
            .as_ref()
            .and_then(RelocationPlan::active)
            .is_some_and(|move_| move_.key == key)
        {
            return Ok(Err("partition is relocating".into()));
        }
        let node = self.nodes.get_mut(&key).expect("local partition leader");
        if target == local {
            return Ok(Ok(key));
        }
        if !node.raw.store().membership.voters.contains(&target) {
            return Ok(Err("target is not a voting replica".into()));
        }
        if !node.admitting() || !node.caught_up(target) {
            return Ok(Err(
                "target or leader is not ready, retry the transfer".into()
            ));
        }
        node.raw.transfer_leader(target);
        self.drain(key)?;
        Ok(Ok(key))
    }

    fn propose_control(&mut self, event: Event) -> Result<()> {
        let node = self.nodes.get_mut(&CONTROL).expect("controller group");
        node.raw.propose(Vec::new(), serde_json::to_vec(&event)?)?;
        self.drain(CONTROL)
    }

    fn drive_relocation(&mut self) -> Result<()> {
        let active = self.relocation.as_ref().and_then(RelocationPlan::active);
        if let Some(movement) = active {
            self.drive_partition_move(movement)?;
        }
        let control = &self.nodes[&CONTROL];
        if !control.admitting() {
            return Ok(());
        }
        if let Some(plan) = &self.relocation {
            if let Some(movement) = active {
                if self.move_ready {
                    self.propose_control(Event::Moved(movement))?;
                }
            } else if let Some(from) = plan.node {
                if self.voters().contains(&from) {
                    self.finish_removal(CONTROL, from)?;
                } else {
                    self.propose_control(Event::Drained(from))?;
                }
            } else {
                self.propose_control(Event::Rebalanced(plan.id))?;
            }
        } else if let Some(plan) = self
            .drain_requests
            .iter()
            .find_map(|&id| self.plan_drain(id))
        {
            self.propose_control(Event::Drain(plan))?;
        }
        Ok(())
    }

    fn drive_partition_move(&mut self, movement: Movement) -> Result<()> {
        let Some(node) = self.nodes.get(&movement.key) else {
            return Ok(());
        };
        if !node.local_leader() || node.committed_term != node.raw.raft.term {
            return Ok(());
        }
        let membership = &node.raw.store().membership;
        if !membership.voters.contains(&movement.from) && membership.voters.contains(&movement.to) {
            let id = self.relocation.as_ref().expect("active relocation").id;
            self.receive_move(self.settings.id, id, movement)?;
            for queue in self.outbound.values() {
                Queued::send(queue, &self.send_bytes, Packet::Moved(id, movement));
            }
        } else if !node.admitting() {
            return Ok(());
        } else if membership.voters.contains(&movement.to) {
            if self.settings.id == movement.to || node.caught_up(movement.to) {
                self.finish_removal(movement.key, movement.from)?;
            }
        } else if membership.learners.contains(&movement.to) {
            if node.caught_up(movement.to) {
                self.propose_membership(movement.key, movement.to, ConfChangeType::AddNode)?;
            }
        } else {
            self.propose_membership(movement.key, movement.to, ConfChangeType::AddLearnerNode)?;
        }
        Ok(())
    }

    fn finish_removal(&mut self, key: GroupId, from: u64) -> Result<()> {
        let node = self.nodes.get_mut(&key).expect("existing group");
        // Keep a caught-up majority of the surviving configuration before removing the source.
        let voters = &node.raw.store().membership.voters;
        let need = (voters.len() - 1) / 2 + 1;
        let survivors = voters
            .iter()
            .copied()
            .filter(|&id| id != from && (id == self.settings.id || node.caught_up(id)))
            .collect::<Vec<_>>();
        if survivors.len() < need {
            return Ok(());
        }
        if from == self.settings.id {
            node.raw.transfer_leader(survivors[0]);
            self.drain(key)
        } else {
            self.propose_membership(key, from, ConfChangeType::RemoveNode)
        }
    }

    fn propose_membership(&mut self, key: GroupId, id: u64, kind: ConfChangeType) -> Result<()> {
        let mut change = ConfChange {
            node_id: id,
            ..ConfChange::default()
        };
        change.set_change_type(kind);
        self.nodes
            .get_mut(&key)
            .expect("existing group")
            .raw
            .propose_conf_change(Vec::new(), change)?;
        self.drain(key)
    }

    pub fn replicas(&self, key: GroupId) -> &[u64] {
        self.replicas.get(&key).map_or(&[], Vec::as_slice)
    }

    pub fn leader_view(&self, key: GroupId) -> (u64, i32) {
        if let Some(node) = self.nodes.get(&key) {
            return (node.leader(), node.epoch());
        }
        self.leaders
            .get(&key)
            .map_or((0, 0), |&(leader, epoch, seen)| {
                if seen.elapsed() < Duration::from_secs(2) {
                    (leader, epoch)
                } else {
                    (0, epoch)
                }
            })
    }

    pub fn receive_leaders(&mut self, source: u64, entries: Vec<(GroupId, i32)>) -> Result<()> {
        let now = Instant::now();
        for (key, epoch) in entries {
            if epoch <= 0 || self.nodes.contains_key(&key) || !self.replicas(key).contains(&source)
            {
                continue;
            }
            if self.leaders.get(&key).is_none_or(|&(leader, previous, _)| {
                previous < epoch || (previous == epoch && leader == source)
            }) {
                self.leaders.insert(key, (source, epoch, now));
            }
        }
        Ok(())
    }

    pub fn cap(&self) -> usize {
        MAX_CLUSTER_PARTITIONS
    }

    pub fn create(
        &mut self,
        config: TopicConfig,
        factor: usize,
    ) -> Result<Result<Reply<i64>, Error>> {
        if self.partitions() + config.partitions > self.cap() {
            return Ok(Err(Error::InvalidPartitions));
        }
        let mut voters: Vec<_> = self
            .voters()
            .iter()
            .copied()
            .filter(|id| {
                self.relocation
                    .as_ref()
                    .is_none_or(|plan| plan.node != Some(*id))
            })
            .collect();
        voters.sort_unstable();
        if !(1..=voters.len()).contains(&factor) {
            return Ok(Err(Error::InvalidReplicationFactor));
        }
        let start = self.partitions();
        let placements = (0..config.partitions)
            .map(|partition| {
                (0..factor)
                    .map(|replica| voters[((start + partition) * factor + replica) % voters.len()])
                    .collect()
            })
            .collect();
        let node = self.nodes.get_mut(&CONTROL).expect("control group");
        if node.leader() != self.settings.id {
            return Ok(Err(Error::NotController));
        }
        if self.topics.contains_key(&config.name) {
            return Ok(Err(Error::TopicExists));
        }
        if !node.admitting() {
            return Ok(Err(Error::RequestTimedOut));
        }
        let reply = node.propose(
            serde_json::to_vec(&Event::PlacedTopic {
                config,
                replicas: placements,
            })?,
            0,
        )?;
        self.drain(CONTROL)?;
        Ok(Ok(reply))
    }

    pub fn controller(&self) -> bool {
        self.nodes[&CONTROL].local_leader()
    }

    pub fn delete_topic(&mut self, config: TopicConfig) -> Result<Result<Reply<i64>, Error>> {
        if self.relocation.as_ref().is_some_and(|plan| {
            plan.movements
                .iter()
                .any(|movement| movement.key.topic == config.id)
        }) {
            return Ok(Err(Error::RequestTimedOut));
        }
        self.topic_change(Event::DeleteTopic {
            name: config.name,
            id: config.id,
        })
    }

    pub fn alter_topic(&mut self, mut config: TopicConfig) -> Result<Result<Reply<i64>, Error>> {
        if config.legacy_retention_since_ms.is_none()
            && config.retention_ms.is_some_and(|value| value >= 0)
        {
            config.legacy_retention_since_ms = Some(groups::now_ms().max(1) as i64);
        }
        self.topic_change(Event::AlterTopic(config))
    }

    fn topic_change(&mut self, event: Event) -> Result<Result<Reply<i64>, Error>> {
        let node = self.nodes.get_mut(&CONTROL).expect("control group");
        if !node.local_leader() {
            return Ok(Err(Error::NotController));
        }
        if !node.admitting() {
            return Ok(Err(Error::RequestTimedOut));
        }
        let reply = node.propose(serde_json::to_vec(&event)?, 0)?;
        self.drain(CONTROL)?;
        Ok(Ok(reply))
    }

    fn stage_produce_batch(
        &mut self,
        key: GroupId,
        inputs: impl IntoIterator<Item = ProduceBatch>,
    ) -> Result<Vec<Result<Write, Error>>> {
        inputs
            .into_iter()
            .map(|input| self.propose_data(key, input))
            .collect()
    }

    pub(super) fn produce_batch(
        &mut self,
        key: GroupId,
        inputs: impl IntoIterator<Item = ProduceBatch>,
    ) -> Result<Vec<Result<Write, Error>>> {
        let writes = self.stage_produce_batch(key, inputs)?;
        if writes
            .iter()
            .any(|write| matches!(write, Ok(Write::Pending(_))))
        {
            self.drain(key)?;
        }
        Ok(writes)
    }

    #[cfg(test)]
    fn produce(&mut self, key: GroupId, input: ProduceBatch) -> Result<Result<Write, Error>> {
        Ok(self
            .produce_batch(key, [input])?
            .pop()
            .expect("one Produce result"))
    }

    fn propose_data(&mut self, key: GroupId, input: ProduceBatch) -> Result<Result<Write, Error>> {
        // Give a ready membership change the admission slot between batches.
        if let Some(movement) = self.relocation.as_ref().and_then(RelocationPlan::active)
            && movement.key == key
            && self
                .nodes
                .get(&key)
                .is_some_and(|node| node.raw.store().membership.voters.contains(&movement.from))
        {
            self.drive_partition_move(movement)?;
        }
        let Some(node) = self.nodes.get_mut(&key) else {
            return Ok(Err(Error::NotLeader));
        };
        let result = Self::propose_on(
            node,
            input,
            self.settings.id,
            self.imported_producer_floor,
            &self.settings.peers,
            &self.next_barrier,
        )?;
        if matches!(result, Ok(Write::Existing(..))) {
            self.drain(key)?;
        }
        Ok(result)
    }

    fn propose_on(
        node: &mut Node,
        input: ProduceBatch,
        local_id: u64,
        imported_producer_floor: i64,
        peers: &BTreeMap<u64, Peer>,
        barriers: &AtomicU64,
    ) -> Result<Result<Write, Error>> {
        if node.leader() != local_id {
            return Ok(Err(Error::NotLeader));
        }
        if input.records.len() > MAX_BATCH {
            return Ok(Err(Error::MessageTooLarge));
        }
        if !node.admitting_data(input.records.len() + 9) {
            return Ok(Err(Error::RequestTimedOut));
        }
        let batch = match input.checked {
            Ok(batch) => batch,
            Err(error) => return Ok(Err(error)),
        };
        let records = input.records;
        if batch.producer > 0 {
            let broker = (batch.producer as u64) & 255;
            if batch.producer > imported_producer_floor
                && (batch.producer < 256 || !peers.contains_key(&broker))
            {
                return Ok(Err(Error::UnknownProducerId));
            }
            if batch.epoch != 0 {
                return Ok(Err(Error::InvalidProducerEpoch));
            }
            let staged = node
                .pending
                .iter()
                .filter_map(|pending| pending.batch.map(|batch| (batch, pending.offset)));
            match node.raw.store().producers.check_staged(batch, staged) {
                Ok(None) => {}
                Ok(Some(offset)) => {
                    return Self::stage_read(node, local_id, barriers).map(|authorized| {
                        authorized.map(|barrier| Write::Existing(offset, barrier))
                    });
                }
                Err(error) => return Ok(Err(error)),
            }
        }
        let offset = node.pending.back().map_or_else(
            || node.log().end().next_offset,
            |pending| {
                pending.offset + i64::from(pending.batch.expect("pending data batch").records)
            },
        );
        if offset.checked_add(i64::from(batch.records)).is_none() {
            return Ok(Err(Error::Storage));
        }
        let epoch = i32::try_from(node.raw.raft.term).context("Kafka leader epoch exhausted")?;
        let mut bytes = BytesMut::with_capacity(records.len());
        bytes.put_i64(offset);
        bytes.extend_from_slice(&records[8..12]);
        bytes.put_i32(epoch);
        bytes.extend_from_slice(&records[16..]);
        let timestamp = groups::now_ms().max(1) as i64;
        let reply = node.propose_entry(encode_batch(timestamp, &bytes), offset, Some(batch))?;
        Ok(Ok(Write::Pending(reply)))
    }

    pub fn read(&mut self, key: GroupId) -> Result<Result<Reply<()>, Error>> {
        let Some(node) = self.nodes.get_mut(&key) else {
            return Ok(Err(Error::NotLeader));
        };
        let result = Self::stage_read(node, self.settings.id, &self.next_barrier)?;
        if result.is_ok() {
            self.drain(key)?;
        }
        Ok(result)
    }

    fn stage_read(
        node: &mut Node,
        local_id: u64,
        barriers: &AtomicU64,
    ) -> Result<Result<Reply<()>, Error>> {
        if node.leader() != local_id {
            return Ok(Err(Error::NotLeader));
        }
        if node.committed_term != node.raw.raft.term || node.reads.len() == MAX_READ_BARRIERS {
            return Ok(Err(Error::RequestTimedOut));
        }
        let mut last = barriers.load(Ordering::Relaxed);
        let token = loop {
            let next = last
                .checked_add(1)
                .context("read barrier counter exhausted")?;
            match barriers.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break next.to_be_bytes().to_vec(),
                Err(current) => last = current,
            }
        };
        let (reply, receiver) = oneshot::channel();
        node.reads.insert(
            token.clone(),
            Read {
                term: node.raw.raft.term,
                authorized: None,
                reply,
            },
        );
        node.raw.read_index(token);
        Ok(Ok(receiver))
    }

    pub fn local_group_shards(&self) -> BTreeSet<GroupId> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.group.is_some() && node.local_leader())
            .map(|(key, _)| *key)
            .collect()
    }

    pub fn group_barrier(&mut self, key: GroupId) -> Result<Result<(u64, Reply<()>), Error>> {
        let Some(node) = self.nodes.get(&key) else {
            return Ok(Err(self.coordinator_error(key)));
        };
        if !node.local_leader() {
            return Ok(Err(self.coordinator_error(key)));
        }
        let term = node.raw.raft.term;
        Ok(self
            .read(key)?
            .map(|reply| (term, reply))
            .map_err(group_error))
    }

    fn coordinator_error(&self, key: GroupId) -> Error {
        if self.leader_view(key).0 == 0 {
            Error::CoordinatorNotAvailable
        } else {
            Error::NotCoordinator
        }
    }

    pub fn group_entries(&self, key: GroupId, term: u64) -> Result<&HashMap<String, Group>, Error> {
        let node = self
            .nodes
            .get(&key)
            .ok_or_else(|| self.coordinator_error(key))?;
        let shard = node.group.as_ref().expect("group shard");
        if node.local_leader() && node.raw.raft.term == term {
            return Ok(&shard.committed);
        }
        Err(Error::NotCoordinator)
    }

    pub fn active_groups(&self, key: GroupId, term: u64) -> Result<&Groups, Error> {
        self.group_entries(key, term)?;
        let node = &self.nodes[&key];
        if !node.admitting() {
            return Err(Error::CoordinatorLoadInProgress);
        }
        Ok(&node
            .group
            .as_ref()
            .expect("group shard")
            .active
            .as_ref()
            .ok_or(Error::CoordinatorLoadInProgress)?
            .1)
    }

    pub fn with_group<T>(
        &mut self,
        key: GroupId,
        term: u64,
        work: impl FnOnce(&mut Groups, &BTreeMap<String, TopicConfig>) -> T,
    ) -> Result<Result<GroupOperation<T>, Error>> {
        if let Err(error) = self.active_groups(key, term) {
            return Ok(Err(error));
        }
        let node = self.nodes.get_mut(&key).expect("active group shard");
        let shard = node.group.as_mut().expect("group shard");
        let groups = &mut shard.active.as_mut().expect("active group state").1;
        let value = work(groups, &self.topics);
        let (events, completions) = groups.take_events();
        let reply = if events.is_empty() {
            ensure!(completions.is_empty(), "group completion without a record");
            None
        } else {
            shard.completions = completions;
            let bytes = serde_json::to_vec(&ShardRecord::V1(events))?;
            ensure!(
                bytes.len() <= MAX_BATCH,
                "group record exceeds Raft entry limit"
            );
            let pending = node.propose(bytes, 0)?;
            self.drain(key)?;
            Some(pending)
        };
        Ok(Ok((value, reply)))
    }

    pub fn allocate_producer(&mut self) -> Result<i64> {
        if self.next_producer > self.reserved_producer {
            let next = self
                .reserved_producer
                .checked_add(4096)
                .context("producer ID exhausted")?;
            ensure!(next <= i64::MAX as u64 >> 8, "producer ID exhausted");
            let bytes = encode_counter(next);
            replace(
                self.data.parent().expect("cluster data parent"),
                "producer-counter",
                &bytes,
            )?;
            self.reserved_producer = next;
        }
        let producer = ((self.next_producer << 8) | self.settings.id) as i64;
        self.next_producer += 1;
        Ok(producer)
    }

    pub fn voters(&self) -> &[u64] {
        &self.nodes[&CONTROL].raw.store().membership.voters
    }

    pub fn check_peer(&self, id: u64, peer: &Peer) -> Result<()> {
        ensure!(!self.retired.contains(&id), "node ID has been retired");
        if let Some(existing) = self.settings.peers.get(&id) {
            ensure!(
                existing.spec == peer.spec,
                "node ID already has different endpoints"
            );
        } else {
            ensure!(
                !self
                    .settings
                    .peers
                    .values()
                    .any(|existing| existing.raft == peer.raft),
                "Raft address already belongs to a configured node"
            );
        }
        Ok(())
    }

    pub fn add_peer(&mut self, id: u64, peer: Peer, sender: tokio::sync::mpsc::Sender<Queued>) {
        Arc::make_mut(&mut self.outbound).insert(id, sender);
        self.settings.peers.insert(id, peer);
    }

    pub fn peer_inventory(&self) -> serde_json::Value {
        let control = &self.nodes[&CONTROL];
        let membership = &control.raw.store().membership;
        serde_json::json!({
            "cluster_id": self.settings.cluster,
            "node_id": self.settings.id,
            "bootstrap_voters": self.settings.bootstrap_voters,
            "configured_peers": self.settings.peers.keys().copied().collect::<Vec<_>>(),
            "controller": control.raw.raft.leader_id,
            "voters": membership.voters,
            "learners": membership.learners,
            "retired": self.retired,
        })
    }

    pub fn receive_join(&mut self, source: u64) -> Result<()> {
        if self.settings.peers.contains_key(&source)
            && !self.voters().contains(&source)
            && !self.retired.contains(&source)
            && self
                .relocation
                .as_ref()
                .is_none_or(|plan| plan.node != Some(source))
        {
            self.join_requests.insert(source, Instant::now());
        }
        Ok(())
    }

    pub fn receive_peer_ready(&mut self, source: u64, ready: BTreeMap<u64, [u8; 32]>) {
        if !self.retired.contains(&source) {
            self.peer_readiness.insert(source, ready);
        }
    }

    fn advance_membership(&mut self) -> Result<()> {
        self.join_requests.retain(|id, since| {
            since.elapsed() < Duration::from_secs(2)
                && !self.retired.contains(id)
                && self
                    .relocation
                    .as_ref()
                    .is_none_or(|plan| plan.node != Some(*id))
        });
        let node = &self.nodes[&CONTROL];
        if !node.admitting() {
            return Ok(());
        }
        let membership = &node.raw.store().membership;
        let committed = node.log().committed().next_index - 1;
        let promotion = membership.learners.iter().copied().find(|id| {
            self.join_requests.contains_key(id)
                && node
                    .raw
                    .raft
                    .prs()
                    .get(*id)
                    .is_some_and(|p| p.matched >= committed && p.recent_active)
        });
        let change =
            if let Some(id) = promotion {
                Some((id, ConfChangeType::AddNode))
            } else {
                self.join_requests
                    .keys()
                    .copied()
                    .find(|id| {
                        !membership.voters.contains(id)
                            && !membership.learners.contains(id)
                            && self
                                .settings
                                .staged_peers
                                .get(id)
                                .is_none_or(|fingerprint| {
                                    membership.voters.iter().chain(&membership.learners).all(
                                        |peer| {
                                            *peer == self.settings.id
                                                || self
                                                    .peer_readiness
                                                    .get(peer)
                                                    .and_then(|ids| ids.get(id))
                                                    == Some(fingerprint)
                                        },
                                    )
                                })
                    })
                    .map(|id| (id, ConfChangeType::AddLearnerNode))
            };
        if let Some((id, kind)) = change {
            let mut change = ConfChange {
                node_id: id,
                ..ConfChange::default()
            };
            change.set_change_type(kind);
            self.nodes
                .get_mut(&CONTROL)
                .expect("control group")
                .raw
                .propose_conf_change(Vec::new(), change)?;
            self.drain(CONTROL)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn tick(&mut self) -> Result<()> {
        self.tick_groups(false).map(|_| ())
    }

    pub(super) fn tick_groups(&mut self, independent: bool) -> Result<Vec<GroupId>> {
        let mut data = Vec::new();
        let keys: Vec<_> = self.nodes.keys().copied().collect();
        for key in keys {
            if independent && self.independent_partition(key) {
                data.push(key);
                continue;
            }
            if let Some(node) = self.nodes.get_mut(&key) {
                Self::tick_node(node);
                self.drain(key)?;
                if is_group_shard(key) {
                    let term = self.nodes[&key].raw.raft.term;
                    let _ = self.with_group(key, term, |groups, _| groups.maintain(true))?;
                }
            }
        }
        if self.settings.page_cache {
            // Spread the timed flushes so a burst of dirty partitions does
            // not force every partition to wait behind one long flush loop.
            let now = Instant::now();
            if let Some(node) = self
                .nodes
                .values_mut()
                .filter(|node| {
                    node.cache_flushed_at
                        .is_some_and(|last| now.duration_since(last) >= PAGE_CACHE_FLUSH_INTERVAL)
                        && node.raw.store().log.needs_page_cache_flush()
                })
                .min_by_key(|node| node.cache_flushed_at)
            {
                node.raw.mut_store().log.flush_page_cache()?;
                node.cache_flushed_at = Some(Instant::now());
            }
        }
        if self.last_report.elapsed() >= Duration::from_millis(250) {
            self.last_report = Instant::now();
            self.drive_cache_recovery()?;
            let requested = self
                .data
                .parent()
                .expect("broker data root")
                .join("decommission")
                .is_file();
            if requested {
                self.receive_drain(self.settings.id)?;
                for queue in self.outbound.values() {
                    Queued::send(queue, &self.send_bytes, Packet::Drain);
                }
            } else if !self.voters().contains(&self.settings.id)
                && !self.retired.contains(&self.settings.id)
            {
                for queue in self.outbound.values() {
                    Queued::send(queue, &self.send_bytes, Packet::Join);
                }
            }
            self.bootstrap_groups()?;
            self.drive_relocation()?;
            self.advance_retention()?;
            self.retire_producers()?;
            self.compact_retention()?;
            self.compact_internal()?;
            if !self.settings.staged_peers.is_empty() {
                let mut report = BytesMut::new();
                for (&id, fingerprint) in &self.settings.staged_peers {
                    report.put_u8(id as u8);
                    report.extend_from_slice(fingerprint);
                }
                let report = report.freeze();
                for queue in self.outbound.values() {
                    Queued::send(queue, &self.send_bytes, Packet::PeerReady(report.clone()));
                }
            }
            if self.relocation.is_none() {
                self.advance_membership()?;
            }
            let mut report = BytesMut::new();
            for (&key, node) in &self.nodes {
                if key != CONTROL
                    && self.replicas(key).len() < self.settings.peers.len()
                    && node.local_leader()
                    && node.committed_term == node.raw.raft.term
                {
                    report.extend_from_slice(key.topic.as_bytes());
                    report.put_u32(key.partition);
                    report.put_i32(node.epoch());
                }
            }
            if !report.is_empty() {
                let report = report.freeze();
                for queue in self.outbound.values() {
                    Queued::send(queue, &self.send_bytes, Packet::Leaders(report.clone()));
                }
            }
        }
        Ok(data)
    }

    fn tick_node(node: &mut Node) {
        let now = Instant::now();
        node.snapshot_deadlines.retain(|&peer, deadline| {
            if now >= *deadline {
                node.raw.report_snapshot(peer, SnapshotStatus::Failure);
                false
            } else {
                true
            }
        });
        if !node.recovering {
            node.raw.tick();
        }
    }

    pub(super) fn flush_page_cache(&mut self) -> Result<()> {
        for node in self.nodes.values_mut() {
            node.raw.mut_store().log.flush_page_cache()?;
        }
        Ok(())
    }

    fn advance_retention(&mut self) -> Result<()> {
        let time = groups::now_ms() as i64;
        let mut proposals = Vec::new();
        for (&key, node) in &self.nodes {
            if !node.admitting() {
                continue;
            }
            let Some(config) = self
                .names
                .get(&key.topic)
                .and_then(|name| self.topics.get(name))
            else {
                continue;
            };
            let keep_ms = config.retention_ms.filter(|&limit| limit >= 0);
            let max_bytes = config
                .retention_bytes
                .and_then(|limit| u64::try_from(limit).ok());
            if keep_ms.is_none() && max_bytes.is_none() {
                continue;
            }
            let current = node.raw.store().retained;
            let next = node.log().retention_boundary_with_legacy_time(
                current,
                rusta_log::Retention {
                    keep_since_ms: keep_ms.map(|limit| time.saturating_sub(limit)),
                    max_bytes,
                },
                config.legacy_retention_since_ms,
            )?;
            if next.next_offset > current.next_offset {
                proposals.push((key, next));
            }
        }
        for (key, start) in proposals {
            self.nodes
                .get_mut(&key)
                .expect("retained partition")
                .propose(encode_retention(start), start.next_offset)?;
            self.drain(key)?;
        }
        Ok(())
    }

    fn compact_retention(&mut self) -> Result<()> {
        let eligible = |node: &&Node| {
            node.group.is_none()
                && node
                    .log()
                    .first_compactable_segment(node.raw.store().retained.next_index)
                    .is_some()
        };
        let next = self
            .nodes
            .iter()
            .filter(|(key, node)| **key != CONTROL && eligible(node))
            .find(|(key, _)| self.compaction_cursor.is_none_or(|cursor| **key > cursor))
            .or_else(|| {
                self.nodes
                    .iter()
                    .find(|(key, node)| **key != CONTROL && eligible(node))
            })
            .map(|(&key, _)| key);
        if let Some(key) = next {
            self.compaction_cursor = Some(key);
            self.nodes
                .get_mut(&key)
                .expect("compaction candidate")
                .raw
                .mut_store()
                .compact_retention()?;
        }
        Ok(())
    }

    fn retire_producers(&mut self) -> Result<()> {
        let now = groups::now_ms();
        let mut proposals = Vec::new();
        for (&key, node) in &self.nodes {
            if key != CONTROL
                && node.group.is_none()
                && node.admitting()
                && let Some(record) = node
                    .raw
                    .store()
                    .producers
                    .maintenance_record(now, self.producer_retention_ms)
            {
                proposals.push((key, record));
            }
        }
        for (key, record) in proposals {
            self.nodes
                .get_mut(&key)
                .expect("existing partition")
                .propose(record, 0)?;
            self.drain(key)?;
        }
        Ok(())
    }

    pub fn receive(&mut self, key: GroupId, message: Message) -> Result<()> {
        if self.step(key, message)? {
            self.drain(key)?;
            self.recovery_progress(key);
        }
        Ok(())
    }

    fn recovery_progress(&mut self, key: GroupId) {
        if let Some(node) = self.nodes.get_mut(&key) {
            node.check_recovery_progress();
        }
    }

    fn step(&mut self, key: GroupId, message: Message) -> Result<bool> {
        if self.retired.contains(&message.from) {
            return Ok(false);
        }
        if let Some(node) = self.nodes.get_mut(&key) {
            if node.recovering && !node.accepts_recovery_append(&message) {
                return Ok(false);
            }
            if message.get_msg_type() == MessageType::MsgSnapshot {
                let snapshot = message.get_snapshot();
                let checked = node
                    .raw
                    .store()
                    .validate_snapshot(snapshot, self.settings.id);
                let metadata = snapshot.get_metadata();
                let members = metadata.get_conf_state();
                if checked.is_err()
                    || metadata.term > message.term
                    || members
                        .voters
                        .iter()
                        .chain(&members.learners)
                        .any(|id| !self.settings.peers.contains_key(id))
                {
                    eprintln!("ignored invalid partition snapshot from {}", message.from);
                    return Ok(false);
                }
                if metadata.index >= node.log().committed().next_index {
                    let checked = if key == CONTROL {
                        snapshots::control_state(snapshot.get_data())
                            .and_then(|state| state.validate(&self.settings))
                    } else if node.group.is_some() {
                        snapshots::group_state(snapshot.get_data()).map(|_| ())
                    } else {
                        Ok(())
                    };
                    if let Err(error) = checked {
                        eprintln!(
                            "ignored invalid internal snapshot from {}: {error:#}",
                            message.from
                        );
                        return Ok(false);
                    }
                }
            }
            // A transport response can be obsolete after a leadership change.
            let _ = node.raw.step(message);
            return Ok(true);
        }
        Ok(false)
    }

    fn drain(&mut self, key: GroupId) -> Result<()> {
        let destination = self
            .relocation
            .as_ref()
            .and_then(RelocationPlan::active)
            .filter(|movement| movement.key == key)
            .map(|movement| movement.to);
        let context = DrainContext {
            outbound: &self.outbound,
            peers: &self.settings.peers,
            send_bytes: &self.send_bytes,
            data_available: &self.data_available,
            group_progress: &self.group_progress,
            local_id: self.settings.id,
            group_retention_ms: self.group_retention_ms,
        };
        let node = self.nodes.get_mut(&key).expect("existing group");
        let drained = Self::drain_ready(
            node,
            key,
            context,
            destination.map(|peer| (peer, &mut self.relocation_budget)),
        )?;
        self.finish_drain(key, drained)
    }

    fn drain_ready(
        node: &mut Node,
        key: GroupId,
        context: DrainContext<'_>,
        mut relocation: Option<(u64, &mut RelocationBudget)>,
    ) -> Result<Drained> {
        let previous_commit = node.log().committed();
        let mut events = Vec::new();
        let mut completions = Vec::new();
        let mut completed_term = None;
        let mut control_snapshot = None;
        while node.raw.has_ready() {
            let mut ready = node.raw.ready();
            let mut messages = ready.take_messages();
            if !key.topic.is_nil()
                && relocation.is_none()
                && node.committed_term == node.raw.raft.term
                && ready.snapshot().is_empty()
                && !ready.entries().is_empty()
            {
                // Raft allows these leader appends to overlap local persistence.
                // Persisted messages and client replies still wait for the disk.
                let early = messages
                    .iter()
                    .take_while(|message| {
                        message.get_msg_type() == MessageType::MsgAppend
                            && !message.entries.is_empty()
                            && message
                                .entries
                                .iter()
                                .all(|entry| entry.get_entry_type() == EntryType::EntryNormal)
                            && context.outbound.contains_key(&message.to)
                    })
                    .count();
                for message in messages.drain(..early) {
                    let queue = &context.outbound[&message.to];
                    Queued::send(queue, context.send_bytes, Packet::Raft(key, message));
                }
            }
            let snapshot = (!ready.snapshot().is_empty()).then_some(ready.snapshot());
            let snapshot_index = if let Some(snapshot) = snapshot {
                node.raw.mut_store().persist(ready.hs(), &[], None)?;
                node.raw
                    .mut_store()
                    .install_snapshot(snapshot, context.local_id)?;
                if key == CONTROL {
                    control_snapshot = Some(snapshots::control_state(snapshot.get_data())?);
                } else if let Some(shard) = &mut node.group {
                    shard.committed = snapshots::group_state(snapshot.get_data())?;
                    shard.active = None;
                }
                node.committed_term = snapshot.get_metadata().term;
                Some(snapshot.get_metadata().index)
            } else {
                None
            };
            let committed = ready
                .committed_entries()
                .last()
                .map(|entry| entry.index + 1);
            let profiling = ack_profile::enabled() && !key.topic.is_nil();
            let ready_commit_at = (profiling && committed.is_some()).then(Instant::now);
            node.raw
                .mut_store()
                .persist(ready.hs(), ready.entries(), committed)?;
            if profiling && let Some(first) = ready.entries().first() {
                let last = ready.entries().last().expect("nonempty Ready entries");
                let now = Instant::now();
                for pending in node
                    .pending
                    .iter_mut()
                    .rev()
                    .take_while(|p| p.index >= first.index)
                {
                    if pending.index <= last.index && pending.proposed_at.is_some() {
                        pending.persisted_at = Some(now);
                    }
                }
            }
            let mut committed_entries = ready.take_committed_entries();
            let messages = messages.into_iter().chain(ready.take_persisted_messages());
            let states = ready.take_read_states();
            let mut light = node.raw.advance_append(ready);
            let light_committed = light.take_committed_entries();
            let light_commit_at = (profiling && !light_committed.is_empty()).then(Instant::now);
            committed_entries.extend(light_committed);
            if let Some(index) = snapshot_index {
                node.raw.advance_apply_to(index);
            }

            if let Some(last) = committed_entries.last() {
                node.raw.mut_store().log.commit_through(last.index + 1)?;
                node.committed_term = last.term;
            }
            for entry in committed_entries {
                if entry.get_entry_type() == EntryType::EntryConfChange {
                    let change = ConfChange::parse_from_bytes(&entry.data)?;
                    ensure!(
                        context.peers.contains_key(&change.node_id),
                        "peer configuration is missing a committed cluster member"
                    );
                    let membership = node.raw.apply_conf_change(&change)?;
                    node.raw.mut_store().membership = membership;
                } else if !entry.data.is_empty() {
                    if key == CONTROL {
                        events.push(serde_json::from_slice(&entry.data)?);
                    } else if let Some(shard) = &mut node.group {
                        apply_shard_record(&mut shard.committed, &entry.data)?;
                    } else {
                        node.raw.mut_store().apply_data(entry.index, &entry.data)?;
                    }
                }
                if node
                    .pending
                    .front()
                    .is_some_and(|p| p.term == entry.term && p.index == entry.index)
                {
                    let pending = node.pending.pop_front().expect("matching pending entry");
                    completed_term = Some(pending.term);
                    let commit_at = if committed.is_some_and(|end| entry.index < end) {
                        ready_commit_at
                    } else {
                        light_commit_at
                    };
                    completions.push((pending, commit_at));
                }
                node.raw.advance_apply_to(entry.index);
            }
            node.raw.mut_store().checkpoint_if_due();
            for state in states {
                if let Some(read) = node.reads.get_mut(&state.request_ctx) {
                    read.authorized = Some(state.index);
                }
            }
            for message in messages.chain(light.take_messages()) {
                let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                if snapshot {
                    // If the route drops this frame, expire Raft's paused progress and retry.
                    node.snapshot_deadlines
                        .insert(message.to, Instant::now() + Duration::from_secs(5));
                }
                if let Some((peer, budget)) = relocation.as_mut()
                    && *peer == message.to
                    && node.raw.store().membership.learners.contains(&message.to)
                    && message.get_msg_type() == MessageType::MsgAppend
                    && !message.entries.is_empty()
                    && !budget.admit(u64::from(message.compute_size()))
                {
                    continue; // Raft retransmits append entries after the next heartbeat.
                }
                if let Some(queue) = context.outbound.get(&message.to) {
                    Queued::send(queue, context.send_bytes, Packet::Raft(key, message));
                }
            }
        }
        let local_leader = node.raw.raft.state == StateRole::Leader;
        let term = node.raw.raft.term;
        if node
            .pending
            .front()
            .is_some_and(|pending| !local_leader || pending.term != term)
        {
            for pending in node.pending.drain(..) {
                let _ = pending.reply.send(Err(Error::NotLeader));
            }
        }
        if let Some(shard) = &mut node.group {
            let previous_term = shard.active.as_ref().map(|(term, _)| *term);
            let active = local_leader && node.committed_term == term;
            if completed_term.is_some() || !active {
                for complete in shard.completions.drain(..) {
                    complete(completed_term == Some(term) && active);
                }
            }
            if !active {
                shard.active = None;
            } else if shard
                .active
                .as_ref()
                .is_none_or(|(epoch, _)| *epoch != term)
            {
                shard.active = Some((
                    term,
                    Groups::replicated(GROUP_SHARDS, context.group_retention_ms, &shard.committed),
                ));
            }
            if previous_term != shard.active.as_ref().map(|(term, _)| *term)
                || node.log().committed() != previous_commit
            {
                context.group_progress.notify_waiters();
            }
        }
        let next = node.raw.store().log.committed().next_index;
        node.reads.retain(|_, read| {
            if read.reply.is_closed() {
                return false;
            }
            let result = if !local_leader || read.term != term {
                Some(Err(Error::NotLeader))
            } else if read.authorized.is_some_and(|index| index < next) {
                Some(Ok(()))
            } else {
                None
            };
            if let Some(result) = result {
                // retain provides a mutable reference, so replace the sender before consuming it.
                let (unused, _) = oneshot::channel();
                let _ = std::mem::replace(&mut read.reply, unused).send(result);
                return false;
            }
            true
        });
        if key != CONTROL && node.group.is_none() {
            let leadership = (node.leader(), term);
            if (local_leader && node.log().committed() != previous_commit)
                || leadership != node.fetch_leadership
            {
                context.data_available.notify_waiters();
            }
            node.fetch_leadership = leadership;
        }
        Ok(Drained {
            events,
            control_snapshot,
            completions,
            local_leader,
            term,
        })
    }

    fn finish_drain(&mut self, key: GroupId, drained: Drained) -> Result<()> {
        let Drained {
            events,
            control_snapshot,
            completions,
            local_leader,
            term,
        } = drained;
        let installed_control_snapshot = control_snapshot.is_some();
        if let Some(state) = control_snapshot {
            self.restore_control(state)?;
        }
        for event in events {
            self.apply(event)?;
        }
        if installed_control_snapshot {
            self.remove_deleted_topic_data()?;
        }
        Self::finish_replies(key, completions, local_leader, term);
        Ok(())
    }

    fn finish_replies(
        key: GroupId,
        completions: Vec<(Pending, Option<Instant>)>,
        local_leader: bool,
        term: u64,
    ) {
        for (pending, commit_at) in completions {
            if let (Some(proposed), Some(committed)) = (pending.proposed_at, commit_at) {
                ack_profile::record("proposal_to_commit", committed.duration_since(proposed));
                if let Some(persisted) = pending.persisted_at {
                    ack_profile::record(
                        "local_persist_to_commit",
                        committed.saturating_duration_since(persisted),
                    );
                }
                ack_profile::finish("commit_to_reply", Some(committed));
            }
            let result = if is_group_shard(key) && (!local_leader || pending.term != term) {
                Err(Error::NotCoordinator)
            } else {
                Ok(pending.offset)
            };
            let _ = pending.reply.send(result);
        }
    }
}

pub(super) struct PartitionWork {
    key: GroupId,
    pub(super) node: Node,
    peers: BTreeMap<u64, Peer>,
    outbound: Arc<Senders>,
    retired: BTreeSet<u64>,
    data_available: Arc<Notify>,
    group_progress: Arc<Notify>,
    send_bytes: Arc<Semaphore>,
    next_barrier: Arc<AtomicU64>,
    local_id: u64,
    producer_floor: i64,
    group_retention_ms: u64,
    drained: Option<Drained>,
}

impl Engine {
    fn independent_partition(&self, key: GroupId) -> bool {
        !key.topic.is_nil()
            && !self.settings.page_cache
            && self
                .relocation
                .as_ref()
                .and_then(RelocationPlan::active)
                .is_none_or(|movement| movement.key != key)
    }

    pub(super) fn lease_partition(&mut self, key: GroupId) -> Option<PartitionWork> {
        if !self.independent_partition(key) {
            return None;
        }
        let node = self.nodes.remove(&key)?;
        Some(PartitionWork {
            key,
            node,
            peers: self.settings.peers.clone(),
            outbound: self.outbound.clone(),
            retired: self.retired.clone(),
            data_available: self.data_available.clone(),
            group_progress: self.group_progress.clone(),
            send_bytes: self.send_bytes.clone(),
            next_barrier: self.next_barrier.clone(),
            local_id: self.settings.id,
            producer_floor: self.imported_producer_floor,
            group_retention_ms: self.group_retention_ms,
            drained: None,
        })
    }

    pub(super) fn restore_partition(&mut self, work: PartitionWork) {
        assert!(
            self.nodes.insert(work.key, work.node).is_none(),
            "leased partition replaced while active"
        );
        if let Some(drained) = work.drained {
            Self::finish_replies(
                work.key,
                drained.completions,
                drained.local_leader,
                drained.term,
            );
        }
    }

    pub(super) fn tick_partition(&mut self, key: GroupId) -> Result<()> {
        if let Some(node) = self.nodes.get_mut(&key) {
            Self::tick_node(node);
            self.drain(key)?;
        }
        Ok(())
    }
}

impl PartitionWork {
    pub(super) fn produce(
        &mut self,
        inputs: Vec<ProduceBatch>,
    ) -> Result<Vec<Result<Write, Error>>> {
        let results = inputs
            .into_iter()
            .map(|input| {
                Engine::propose_on(
                    &mut self.node,
                    input,
                    self.local_id,
                    self.producer_floor,
                    &self.peers,
                    &self.next_barrier,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if results.iter().any(Result::is_ok) {
            self.drain()?;
        }
        Ok(results)
    }

    pub(super) fn read(&mut self) -> Result<Result<Reply<()>, Error>> {
        let result = Engine::stage_read(&mut self.node, self.local_id, &self.next_barrier)?;
        if result.is_ok() {
            self.drain()?;
        }
        Ok(result)
    }

    pub(super) fn receive(&mut self, message: Message) -> Result<()> {
        if self.retired.contains(&message.from) {
            return Ok(());
        }
        // Snapshots retain the topology and recovery validation of Engine::receive.
        ensure!(
            message.get_msg_type() != MessageType::MsgSnapshot,
            "snapshot routed to detached worker"
        );
        if !self.node.recovering || self.node.accepts_recovery_append(&message) {
            let _ = self.node.raw.step(message);
            self.drain()?;
            self.node.check_recovery_progress();
        }
        Ok(())
    }

    pub(super) fn tick(&mut self) -> Result<()> {
        Engine::tick_node(&mut self.node);
        self.drain()
    }

    fn drain(&mut self) -> Result<()> {
        let context = DrainContext {
            outbound: &self.outbound,
            peers: &self.peers,
            send_bytes: &self.send_bytes,
            data_available: &self.data_available,
            group_progress: &self.group_progress,
            local_id: self.local_id,
            group_retention_ms: self.group_retention_ms,
        };
        let drained = Engine::drain_ready(&mut self.node, self.key, context, None)?;
        ensure!(
            drained.events.is_empty() && drained.control_snapshot.is_none(),
            "control event on a data partition"
        );
        self.drained = Some(drained);
        Ok(())
    }
}

fn balance_movements(voters: &[u64], replicas: &HashMap<GroupId, Vec<u64>>) -> Vec<Movement> {
    let mut assignment = replicas
        .iter()
        .map(|(&key, replicas)| (key, replicas.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut loads = voters
        .iter()
        .map(|&id| (id, 0_usize))
        .collect::<BTreeMap<_, _>>();
    for replicas in assignment.values() {
        for id in replicas {
            if let Some(load) = loads.get_mut(id) {
                *load += 1;
            }
        }
    }
    let mut movements = Vec::new();
    while let (Some((&from, &highest)), Some((&to, &lowest))) = (
        loads
            .iter()
            .max_by_key(|&(&id, &load)| (load, std::cmp::Reverse(id))),
        loads.iter().min_by_key(|&(&id, &load)| (load, id)),
    ) {
        if highest <= lowest + 1 {
            break;
        }
        let (&key, replicas) = assignment
            .iter_mut()
            .find(|(_, replicas)| replicas.contains(&from) && !replicas.contains(&to))
            .expect("higher replica count implies an eligible partition");
        *replicas
            .iter_mut()
            .find(|replica| **replica == from)
            .expect("existing source") = to;
        *loads.get_mut(&from).expect("member load") -= 1;
        *loads.get_mut(&to).expect("member load") += 1;
        movements.push(Movement { key, from, to });
    }
    movements
}

fn apply_shard_record(groups: &mut HashMap<String, Group>, bytes: &[u8]) -> Result<()> {
    let ShardRecord::V1(events) = serde_json::from_slice(bytes)?;
    for event in events {
        groups::apply_event(groups, event)?;
    }
    Ok(())
}

pub fn group_error(error: Error) -> Error {
    match error {
        Error::NotLeader => Error::NotCoordinator,
        Error::RequestTimedOut => Error::CoordinatorLoadInProgress,
        error => error,
    }
}

pub enum RebalanceResult {
    Pending(Reply<i64>),
    Active,
    Completed,
    Balanced,
}

pub enum Write {
    Existing(i64, Reply<()>),
    Pending(Reply<i64>),
}

pub(super) struct ProduceBatch {
    records: Bytes,
    checked: Result<Batch, Error>,
}

impl ProduceBatch {
    pub(super) fn inspect(records: Bytes) -> Self {
        let checked = if records.len() > MAX_BATCH {
            Err(Error::MessageTooLarge)
        } else {
            Batch::parse(&records)
                .map_err(|_| Error::CorruptMessage)
                .and_then(|batch| {
                    if !batch.compression_is_supported() {
                        return Err(Error::UnsupportedCompression);
                    }
                    batch
                        .scan(&records, i64::MAX)
                        .map_err(|_| Error::InvalidRecord)?;
                    Ok(batch)
                })
        };
        Self { records, checked }
    }
}

impl Node {
    fn open(
        settings: &Settings,
        path: &Path,
        voters: Vec<u64>,
        key: GroupId,
        initial_offset: i64,
    ) -> Result<Self> {
        let cache = settings.page_cache && !key.topic.is_nil() && voters.len() >= 3;
        let recovering = cache && settings.cache_recovery.is_some();
        let mut disk = Disk::open_replica(
            path,
            voters,
            key == CONTROL || is_group_shard(key),
            initial_offset,
            cache.then_some(recovering),
        )?;
        if cache && !recovering {
            disk.log.enable_page_cache();
        }
        let cache_flushed_at = cache.then(Instant::now);
        let group = if is_group_shard(key) {
            let mut committed = disk
                .internal_state::<GroupState>()?
                .map(GroupState::restore)
                .transpose()?
                .unwrap_or_default();
            let mut next = disk.log.start().next_index;
            let end = disk.log.committed().next_index;
            while next < end {
                next = disk
                    .log
                    .visit_entries(next, end, MAX_BATCH as u64, |_, bytes| {
                        if !bytes.is_empty() && bytes[0] != 255 {
                            apply_shard_record(&mut committed, bytes)
                                .map_err(std::io::Error::other)?;
                        }
                        Ok(())
                    })?;
            }
            Some(GroupShard {
                committed,
                active: None,
                completions: Vec::new(),
            })
        } else {
            None
        };
        let end = disk.log.committed().next_index;
        let committed_term = disk.log.term(end - 1)?;
        let config = Config {
            id: settings.id,
            applied: end - 1,
            election_tick: 12,
            heartbeat_tick: 2,
            pre_vote: true,
            check_quorum: true,
            // Durable data followers can learn the new commit from their next append or
            // heartbeat. This lets them publish append and commit in one manifest sync.
            skip_bcast_commit: !cache && !key.topic.is_nil(),
            max_size_per_msg: MAX_BATCH as u64 + 4096,
            max_inflight_msgs: 4,
            max_committed_size_per_ready: 2 * MAX_BATCH as u64,
            max_uncommitted_size: MAX_UNCOMMITTED_BYTES as u64,
            ..Config::default()
        };
        config.validate()?;
        let raw = RawNode::new(
            &config,
            disk,
            &slog::Logger::root(slog::Discard, slog::o!()),
        )?;
        Ok(Self {
            raw,
            pending: VecDeque::new(),
            reads: HashMap::new(),
            committed_term,
            group,
            fetch_leadership: (0, 0),
            snapshot_deadlines: HashMap::new(),
            cache_flushed_at,
            recovering,
            recovery_offer: None,
            recovery_requests: HashMap::new(),
        })
    }

    pub fn leader(&self) -> u64 {
        self.raw.raft.leader_id
    }

    pub fn epoch(&self) -> i32 {
        i32::try_from(self.raw.raft.term).unwrap_or(i32::MAX)
    }

    pub fn local_leader(&self) -> bool {
        self.raw.raft.state == StateRole::Leader
    }

    pub fn log(&self) -> &rusta_log::PartitionLog {
        &self.raw.store().log
    }

    pub fn log_start(&self) -> i64 {
        self.raw.store().retained.next_offset
    }

    pub fn compacted_data_epoch(&self) -> Option<i32> {
        self.raw.store().compacted_data_epoch
    }

    pub fn in_sync(&self) -> Vec<u64> {
        if !self.local_leader() {
            return Vec::new();
        }
        let committed = self.log().committed().next_index - 1;
        self.raw
            .raft
            .prs()
            .iter()
            .filter(|(_, state)| state.matched >= committed && state.recent_active)
            .map(|(id, _)| *id)
            .collect()
    }

    fn caught_up(&self, id: u64) -> bool {
        let committed = self.log().committed().next_index - 1;
        self.raw
            .raft
            .prs()
            .get(id)
            .is_some_and(|p| p.matched >= committed && p.recent_active)
    }

    fn admitting(&self) -> bool {
        self.local_leader()
            && self.raw.raft.lead_transferee.is_none()
            && self.committed_term == self.raw.raft.term
            && self.pending.is_empty()
            && self.log().end() == self.log().committed()
    }

    fn admitting_data(&self, bytes: usize) -> bool {
        self.local_leader()
            && self.raw.raft.lead_transferee.is_none()
            && self.committed_term == self.raw.raft.term
            && self.pending.len() < MAX_PENDING_BATCHES
            && self.pending.iter().all(|pending| pending.batch.is_some())
            && self.raw.raft.raft_log.last_index() + 1
                == self.log().committed().next_index + self.pending.len() as u64
            && self
                .pending
                .iter()
                .map(|pending| pending.bytes)
                .sum::<usize>()
                + bytes
                <= MAX_UNCOMMITTED_BYTES
    }

    fn propose(&mut self, bytes: Vec<u8>, offset: i64) -> Result<Reply<i64>> {
        self.propose_entry(bytes, offset, None)
    }

    fn propose_entry(
        &mut self,
        bytes: Vec<u8>,
        offset: i64,
        batch: Option<Batch>,
    ) -> Result<Reply<i64>> {
        let (reply, receiver) = oneshot::channel();
        let term = self.raw.raft.term;
        let length = bytes.len();
        let proposed_at = batch.is_some().then(ack_profile::start).flatten();
        self.raw.propose(Vec::new(), bytes)?;
        let index = self.raw.raft.raft_log.last_index();
        self.pending.push_back(Pending {
            index,
            term,
            reply,
            offset,
            batch,
            bytes: length,
            proposed_at,
            persisted_at: None,
        });
        Ok(receiver)
    }
}

fn encode_counter(next: u64) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[..8].copy_from_slice(&next.to_be_bytes());
    let checksum = crc32c::crc32c(&bytes[..8]);
    bytes[8..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn read_counter(root: &Path) -> Result<u64> {
    match fs::read(root.join("producer-counter")) {
        Ok(bytes) => {
            ensure!(bytes.len() == 12, "invalid producer ID counter");
            let next = u64::from_be_bytes(bytes[..8].try_into()?);
            ensure!(
                next <= i64::MAX as u64 >> 8 && bytes == encode_counter(next),
                "invalid producer ID counter"
            );
            Ok(next)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups::{Key, Offset};
    use tokio::sync::oneshot::error::TryRecvError;

    const GROUP: &str = "fault-test";

    #[test]
    fn partition_pipelines_ordered_batches_without_early_ack() -> Result<()> {
        let mut cluster = TestCluster::open()?;
        cluster.leader()?;
        let key = GroupId {
            topic: Uuid::new_v4(),
            partition: 0,
        };
        let controller = cluster
            .brokers
            .iter_mut()
            .flatten()
            .find(|broker| broker.controller())
            .expect("controller elected");
        controller
            .create(
                TopicConfig {
                    name: "pipeline".into(),
                    id: key.topic,
                    partitions: 1,
                    allow_weak_acks: false,
                    retention_ms: None,
                    retention_bytes: None,
                    legacy_retention_since_ms: None,
                },
                3,
            )?
            .expect("topic proposed");
        cluster.pump()?;
        let mut leader = None;
        for _ in 0..150 {
            for broker in cluster.brokers.iter_mut().flatten() {
                broker.tick()?;
            }
            cluster.pump()?;
            leader = cluster.brokers.iter().position(|broker| {
                broker
                    .as_ref()
                    .unwrap()
                    .nodes
                    .get(&key)
                    .is_some_and(Node::admitting)
            });
            if leader.is_some() {
                break;
            }
        }
        let leader = leader.expect("partition leader elected");
        let make_batch = |sequence: i32| {
            let mut batch = vec![0u8; 61];
            batch.extend_from_slice(&[12, 0, 0, 0, 1, 1, 0]);
            let length = batch.len() as i32 - 12;
            batch[8..12].copy_from_slice(&length.to_be_bytes());
            batch[16] = 2;
            batch[43..51].copy_from_slice(&257_i64.to_be_bytes());
            batch[53..57].copy_from_slice(&sequence.to_be_bytes());
            batch[57..61].copy_from_slice(&1_i32.to_be_bytes());
            let crc = crc32c::crc32c(&batch[21..]);
            batch[17..21].copy_from_slice(&crc.to_be_bytes());
            ProduceBatch::inspect(Bytes::from(batch))
        };
        let broker = cluster.brokers[leader].as_mut().unwrap();
        let mut replies = Vec::new();
        for write in broker.produce_batch(key, [make_batch(0), make_batch(1)])? {
            let Write::Pending(mut reply) = write.unwrap() else {
                panic!("grouped Produce batch must be proposed");
            };
            assert!(matches!(reply.try_recv(), Err(TryRecvError::Empty)));
            replies.push(reply);
        }
        assert_eq!(broker.nodes[&key].log().durable().next_offset, 2);
        assert!(matches!(
            broker.produce(key, make_batch(0))?,
            Err(Error::RequestTimedOut)
        ));
        for sequence in 2..MAX_PENDING_BATCHES as i32 {
            let Write::Pending(mut reply) = broker.produce(key, make_batch(sequence))?.unwrap()
            else {
                panic!("new batch must be proposed");
            };
            assert!(matches!(reply.try_recv(), Err(TryRecvError::Empty)));
            replies.push(reply);
        }
        assert!(matches!(
            broker.produce(key, make_batch(MAX_PENDING_BATCHES as i32))?,
            Err(Error::RequestTimedOut)
        ));
        assert_eq!(broker.nodes[&key].log().committed().next_offset, 0);
        cluster.pump()?;
        for (offset, reply) in replies.iter_mut().enumerate() {
            assert_eq!(reply.try_recv()?.unwrap(), offset as i64);
        }
        let end = MAX_PENDING_BATCHES as i64;
        assert!(cluster.brokers.iter().enumerate().any(|(index, broker)| {
            index != leader
                && broker.as_ref().unwrap().nodes[&key]
                    .log()
                    .durable()
                    .next_offset
                    == end
        }));
        for _ in 0..4 {
            for broker in cluster.brokers.iter_mut().flatten() {
                broker.tick()?;
            }
            cluster.pump()?;
        }
        for broker in cluster.brokers.iter().flatten() {
            assert_eq!(broker.nodes[&key].log().committed().next_offset, end);
        }
        let broker = cluster.brokers[leader].as_mut().unwrap();
        let Write::Existing(offset, mut barrier) = broker.produce(key, make_batch(0))?.unwrap()
        else {
            panic!("committed retry must reuse the original offset");
        };
        cluster.pump()?;
        assert_eq!(offset, 0);
        barrier.try_recv()?.unwrap();

        let broker = cluster.brokers[leader].as_mut().unwrap();
        let mut unconfirmed = Vec::new();
        for sequence in MAX_PENDING_BATCHES..MAX_PENDING_BATCHES + 2 {
            let Write::Pending(reply) = broker.produce(key, make_batch(sequence as i32))?.unwrap()
            else {
                panic!("new batch must be proposed");
            };
            unconfirmed.push(reply);
        }
        let mut heartbeat = Message {
            from: ((leader + 1) % 3 + 1) as u64,
            to: broker.settings.id,
            term: broker.nodes[&key].raw.raft.term + 1,
            ..Message::default()
        };
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        broker.receive(key, heartbeat)?;
        for reply in &mut unconfirmed {
            assert!(matches!(reply.try_recv()?, Err(Error::NotLeader)));
        }
        assert_eq!(
            broker.nodes[&key].log().committed().next_offset,
            MAX_PENDING_BATCHES as i64
        );
        Ok(())
    }

    type Outbound = HashMap<u64, tokio::sync::mpsc::Receiver<Queued>>;

    struct TestCluster {
        brokers: Vec<Option<Engine>>,
        routes: Vec<Outbound>,
        _root: tempfile::TempDir,
    }

    impl TestCluster {
        fn open() -> Result<Self> {
            let root = tempfile::tempdir()?;
            let cluster = Uuid::new_v4();
            let peers = (1..=3)
                .map(|id| {
                    let endpoint = super::super::endpoint("localhost:9092").unwrap();
                    (
                        id,
                        super::super::Peer {
                            spec: String::new(),
                            raft: String::new(),
                            dns: String::new(),
                            host: endpoint.clone(),
                            internal: endpoint,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let mut brokers = Vec::new();
            let mut routes = Vec::new();
            for id in 1..=3 {
                let settings = Settings {
                    id,
                    cluster,
                    peers: peers.clone(),
                    bootstrap_voters: vec![1, 2, 3],
                    page_cache: false,
                    cache_recovery: None,
                    staged_peers: BTreeMap::new(),
                    bind: "127.0.0.1:0".parse()?,
                    certificates: PathBuf::new(),
                };
                let (senders, receivers) = settings
                    .peers
                    .keys()
                    .copied()
                    .filter(|&peer| peer != id)
                    .map(|peer| {
                        let (sender, receiver) = tokio::sync::mpsc::channel(16);
                        ((peer, sender), (peer, receiver))
                    })
                    .unzip();
                brokers.push(Some(Engine::open(
                    settings,
                    &root.path().join(id.to_string()),
                    senders,
                )?));
                routes.push(receivers);
            }
            Ok(Self {
                _root: root,
                brokers,
                routes,
            })
        }

        fn pump(&mut self) -> Result<()> {
            self.pump_until(|_| false)
        }

        fn pump_until(&mut self, stop: impl Fn(&Self) -> bool) -> Result<()> {
            while !stop(self) {
                let mut packets = Vec::new();
                for (source, routes) in self.routes.iter_mut().enumerate() {
                    for (&dest, receiver) in routes {
                        while let Ok(queued) = receiver.try_recv() {
                            if self.brokers[source].is_some()
                                && let Packet::Raft(key, message) = queued.into_packet()
                            {
                                packets.push((dest as usize - 1, key, message));
                            }
                        }
                    }
                }
                if packets.is_empty() {
                    return Ok(());
                }
                for (dest, key, message) in packets {
                    if let Some(broker) = &mut self.brokers[dest] {
                        broker.receive(key, message)?;
                    }
                    if stop(self) {
                        return Ok(());
                    }
                }
            }
            Ok(())
        }

        fn leader(&mut self) -> Result<usize> {
            for _ in 0..150 {
                for broker in self.brokers.iter_mut().flatten() {
                    broker.tick()?;
                    broker.bootstrap_groups()?;
                }
                self.pump()?;
                if let Some(index) = self.brokers.iter().position(|broker| {
                    broker
                        .as_ref()
                        .and_then(|broker| broker.nodes.get(&coordinator(GROUP)))
                        .is_some_and(Node::admitting)
                }) {
                    return Ok(index);
                }
            }
            anyhow::bail!("no elected coordinator");
        }

        fn write(&mut self, leader: usize, partition: Key, value: i64) -> Result<Reply<i64>> {
            let broker = self.brokers[leader].as_mut().unwrap();
            let key = coordinator(GROUP);
            let term = broker.nodes[&key].raw.raft.term;
            let operation = broker.with_group(key, term, |groups, _| {
                groups.commit(
                    GROUP,
                    vec![(
                        partition,
                        Offset {
                            value,
                            leader_epoch: -1,
                            metadata: None,
                        },
                    )],
                )
            })?;
            let (outcome, pending) = operation.expect("coordinator admitted commit");
            outcome.expect("offset validated");
            Ok(pending.expect("commit was proposed"))
        }

        fn read(&mut self, leader: usize, partition: Key) -> Result<i64> {
            let key = coordinator(GROUP);
            let (term, mut barrier) = self.brokers[leader]
                .as_mut()
                .unwrap()
                .group_barrier(key)?
                .expect("leader read barrier");
            self.pump()?;
            barrier.try_recv()?.expect("quorum authorized the read");
            let groups = self.brokers[leader]
                .as_ref()
                .unwrap()
                .group_entries(key, term)
                .expect("coordinator term is current");
            Ok(groups[GROUP].offsets[&partition].value)
        }
    }

    #[test]
    fn old_controller_drain_records_remain_readable() -> Result<()> {
        let record =
            serde_json::from_str::<Event>(r#"{"Drain":{"node":2,"movements":[],"next":0}}"#)?;
        let Event::Drain(plan) = record else {
            anyhow::bail!("expected a drain plan");
        };
        assert_eq!(plan.node, Some(2));
        assert!(plan.id.is_nil());
        assert!(plan.active().is_none());
        Ok(())
    }

    #[test]
    fn replica_balancing_preserves_rf_and_converges() {
        let topic = Uuid::new_v4();
        let mut replicas = (0..8)
            .map(|partition| (GroupId { topic, partition }, vec![1, 2, 3]))
            .collect::<HashMap<_, _>>();
        replicas.insert(group_shard(0), vec![1, 2, 3]);
        let roster = [1, 2, 3, 4];
        let plan = balance_movements(&roster, &replicas);
        assert_eq!(plan, balance_movements(&roster, &replicas));
        assert!(!plan.is_empty());
        for movement in plan {
            let voters = replicas.get_mut(&movement.key).unwrap();
            assert!(!voters.contains(&movement.to));
            *voters.iter_mut().find(|id| **id == movement.from).unwrap() = movement.to;
        }
        assert!(replicas.values().all(|ids| ids.len() == 3));
        assert!(balance_movements(&roster, &replicas).is_empty());
        assert!(replicas.values().any(|ids| ids.contains(&4)));
    }

    #[test]
    fn group_shard_mapping_is_stable() {
        for (id, shard) in [("a", 0), ("b", 1), ("c", 2), ("d", 3), ("группа", 1)] {
            assert_eq!(coordinator(id), group_shard(shard));
        }
    }

    #[test]
    fn coordinator_failure_before_durable_majority_preserves_previous_offset() -> Result<()> {
        let mut cluster = TestCluster::open()?;
        let leader = cluster.leader()?;
        let partition = Key {
            topic: Uuid::new_v4(),
            partition: 0,
        };
        let mut confirmed = cluster.write(leader, partition, 4)?;
        cluster.pump()?;
        confirmed.try_recv()?.expect("confirmed offset");
        assert_eq!(cluster.read(leader, partition)?, 4);

        // The entry is durable on the leader. Withhold all replication, then
        // crash that leader. Its queued messages cannot reach the survivors.
        let mut unconfirmed = cluster.write(leader, partition, 8)?;
        let log = cluster.brokers[leader].as_ref().unwrap().nodes[&coordinator(GROUP)].log();
        assert!(log.durable().next_index > log.committed().next_index);
        assert!(matches!(unconfirmed.try_recv(), Err(TryRecvError::Empty)));
        drop(cluster.brokers[leader].take());
        let next = cluster.leader()?;
        assert_eq!(cluster.read(next, partition)?, 4);
        Ok(())
    }

    #[test]
    fn coordinator_failure_after_durable_majority_preserves_offset_with_lost_response() -> Result<()>
    {
        let mut cluster = TestCluster::open()?;
        let leader = cluster.leader()?;
        let partition = Key {
            topic: Uuid::new_v4(),
            partition: 0,
        };
        let response = cluster.write(leader, partition, 9)?;
        // Simulate the requester disconnecting while the commit is in flight.
        // Stop delivery as soon as the leader commits. Followers may have
        // persisted the entry without receiving the updated commit index.
        drop(response);
        let key = coordinator(GROUP);
        cluster.pump_until(|cluster| {
            cluster.brokers[leader].as_ref().unwrap().nodes[&key]
                .pending
                .is_empty()
        })?;
        let node = &cluster.brokers[leader].as_ref().unwrap().nodes[&key];
        let next_index = node.log().committed().next_index;
        assert_eq!(
            node.group.as_ref().unwrap().committed[GROUP].offsets[&partition].value,
            9
        );
        assert!(cluster.brokers.iter().enumerate().any(|(index, broker)| {
            let log = broker.as_ref().unwrap().nodes[&key].log();
            index != leader
                && log.durable().next_index >= next_index
                && log.committed().next_index < next_index
        }));
        drop(cluster.brokers[leader].take());
        let next = cluster.leader()?;
        assert_eq!(cluster.read(next, partition)?, 9);
        Ok(())
    }
}
