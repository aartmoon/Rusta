use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use protobuf::Message as ProtoMessage;
use raft::{
    Config, RawNode, StateRole, Storage,
    eraftpb::{ConfChange, ConfChangeType, EntryType, Message},
};
use rusta_kafka::records::Batch;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, oneshot};
use uuid::Uuid;

use crate::{
    catalog::{MAX_PARTITIONS, TopicConfig},
    partition::{History, MAX_BATCH, MAX_PRODUCER_STATES},
    protocol::Error,
};

use super::{
    Settings,
    disk::{Disk, decode_change, replace},
    transport::{Packet, Queued, Senders},
};

type Reply<T> = oneshot::Receiver<Result<T, Error>>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupId {
    pub topic: Uuid,
    pub partition: u32,
}

pub const CONTROL: GroupId = GroupId {
    topic: Uuid::nil(),
    partition: 0,
};

const MAX_CLUSTER_PARTITIONS: usize = 128;
const MAX_READ_BARRIERS: usize = 16;

pub struct Engine {
    pub settings: Settings,
    pub topics: BTreeMap<String, TopicConfig>,
    pub names: HashMap<Uuid, String>,
    pub nodes: BTreeMap<GroupId, Node>,
    outbound: Senders,
    send_bytes: Arc<Semaphore>,
    replicas: HashMap<GroupId, Vec<u64>>,
    origins: HashMap<GroupId, Vec<u64>>,
    retired: BTreeSet<u64>,
    drain: Option<DrainPlan>,
    drain_requests: BTreeSet<u64>,
    move_ready: bool,

    leaders: HashMap<GroupId, (u64, i32, Instant)>,
    last_report: Instant,
    join_requests: BTreeMap<u64, Instant>,
    next_barrier: u64,
    next_producer: u64,
    reserved_producer: u64,
    data: PathBuf,
}

pub struct Node {
    raw: RawNode<Disk>,
    pending: Option<Pending>,
    reads: HashMap<Vec<u8>, Read>,
    histories: HashMap<i64, History>,
    committed_term: u64,
}

struct Pending {
    index: u64,
    term: u64,
    reply: oneshot::Sender<Result<i64, Error>>,
    offset: i64,
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
struct DrainPlan {
    node: u64,
    movements: Vec<Movement>,
    next: usize,
}

impl DrainPlan {
    fn active(&self) -> Option<Movement> {
        self.movements.get(self.next).copied()
    }
}

#[derive(Deserialize, Serialize)]
enum Event {
    Topic(TopicConfig),
    Drain(DrainPlan),
    Moved(Movement),
    Drained(u64),
    PlacedTopic {
        config: TopicConfig,
        replicas: Vec<Vec<u64>>,
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
            true,
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
            settings,
            topics: BTreeMap::new(),
            names: HashMap::new(),
            nodes: BTreeMap::new(),
            outbound,
            send_bytes: Arc::new(Semaphore::new(32 * 1024 * 1024)),
            replicas: HashMap::new(),
            origins: HashMap::new(),
            retired: BTreeSet::new(),
            drain: None,
            drain_requests: BTreeSet::new(),
            move_ready: false,

            leaders: HashMap::new(),
            last_report: Instant::now(),
            join_requests: BTreeMap::new(),
            next_barrier: 0,
            next_producer: counter + 1,
            reserved_producer: counter,
            data,
        };
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
        let (config, placements) = match event {
            Event::Topic(config) => {
                let members = self.settings.bootstrap_voters.clone();
                let placements = vec![members; config.partitions];
                (config, placements)
            }
            Event::PlacedTopic { config, replicas } => (config, replicas),
            Event::Drain(plan) => {
                ensure!(
                    self.drain.is_none() && !self.retired.contains(&plan.node),
                    "overlapping drain plans"
                );
                self.drain = Some(plan);
                self.open_destination()?;
                return Ok(());
            }
            Event::Moved(movement) => {
                let plan = self
                    .drain
                    .as_mut()
                    .context("placement update without a drain")?;
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
                if self.settings.id == movement.from {
                    self.nodes.remove(&movement.key);
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
                    self.drain
                        .as_ref()
                        .is_some_and(|p| p.node == id && p.active().is_none()),
                    "unexpected drain completion"
                );
                self.retired.insert(id);
                self.drain = None;
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
            let unique: std::collections::HashSet<_> = voters.iter().collect();
            ensure!(
                !voters.is_empty()
                    && unique.len() == voters.len()
                    && voters.iter().all(|id| self.settings.peers.contains_key(id)),
                "invalid replica assignment"
            );
            let group = GroupId {
                topic: config.id,
                partition: partition as u32,
            };
            if voters.contains(&self.settings.id) {
                let path = self.data.join(format!("{}-{partition}", config.id));
                self.nodes.insert(
                    group,
                    Node::open(&self.settings, &path, voters.clone(), false)?,
                );
            }
            self.origins.insert(group, voters.clone());
            self.replicas.insert(group, voters);
        }
        self.names.insert(config.id, config.name.clone());
        self.topics.insert(config.name.clone(), config);
        Ok(())
    }

    fn open_destination(&mut self) -> Result<()> {
        let Some(movement) = self.drain.as_ref().and_then(DrainPlan::active) else {
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
                Node::open(&self.settings, &path, origins.clone(), false)?,
            );
        }
        Ok(())
    }

    pub fn placement_count(&self) -> usize {
        self.voters()
            .iter()
            .filter(|id| self.drain.as_ref().is_none_or(|plan| plan.node != **id))
            .count()
    }

    pub fn receive_drain(&mut self, id: u64) -> Result<()> {
        if self.voters().contains(&id) && !self.retired.contains(&id) {
            self.drain_requests.insert(id);
        }
        Ok(())
    }

    pub fn receive_move(&mut self, reporter: u64, movement: Movement) -> Result<()> {
        if self.drain.as_ref().and_then(DrainPlan::active) == Some(movement)
            && reporter != movement.from
            && (reporter == movement.to || self.replicas(movement.key).contains(&reporter))
        {
            self.move_ready = true;
        }
        Ok(())
    }

    fn plan_drain(&self, from: u64) -> Option<DrainPlan> {
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
        Some(DrainPlan {
            node: from,
            movements,
            next: 0,
        })
    }

    fn propose_control(&mut self, event: Event) -> Result<()> {
        let node = self.nodes.get_mut(&CONTROL).expect("controller group");
        node.raw.propose(Vec::new(), serde_json::to_vec(&event)?)?;
        self.drain(CONTROL)
    }

    fn drive_drain(&mut self) -> Result<()> {
        let active = self.drain.as_ref().and_then(DrainPlan::active);
        if let Some(movement) = active {
            self.drive_partition_move(movement)?;
        }
        let control = &self.nodes[&CONTROL];
        if !control.admitting() {
            return Ok(());
        }
        if let Some(plan) = &self.drain {
            if let Some(movement) = active {
                if self.move_ready {
                    self.propose_control(Event::Moved(movement))?;
                }
            } else if !self.voters().contains(&plan.node) {
                self.propose_control(Event::Drained(plan.node))?;
            } else {
                self.finish_removal(CONTROL, plan.node)?;
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
            self.receive_move(self.settings.id, movement)?;
            for queue in self.outbound.values() {
                Queued::send(queue, &self.send_bytes, Packet::Moved(movement));
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
            .filter(|id| self.drain.as_ref().is_none_or(|plan| plan.node != *id))
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

    pub fn produce(&mut self, key: GroupId, records: Bytes) -> Result<Result<Write, Error>> {
        // Give a ready membership change the admission slot between batches.
        if let Some(movement) = self.drain.as_ref().and_then(DrainPlan::active)
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
        if node.leader() != self.settings.id {
            return Ok(Err(Error::NotLeader));
        }
        if !node.admitting() {
            return Ok(Err(Error::RequestTimedOut));
        }
        if records.len() > MAX_BATCH {
            return Ok(Err(Error::MessageTooLarge));
        }
        let batch = match Batch::parse(&records) {
            Ok(batch) => batch,
            Err(_) => return Ok(Err(Error::CorruptMessage)),
        };
        if !batch.compression_is_supported() {
            return Ok(Err(Error::UnsupportedCompression));
        }
        if batch.scan(&records, i64::MAX).is_err() {
            return Ok(Err(Error::InvalidRecord));
        }
        if batch.producer > 0 {
            let broker = (batch.producer as u64) & 255;
            if batch.producer < 256 || !self.settings.peers.contains_key(&broker) {
                return Ok(Err(Error::UnknownProducerId));
            }
            if batch.epoch != 0 {
                return Ok(Err(Error::InvalidProducerEpoch));
            }
            match node.histories.get(&batch.producer) {
                Some(history) if batch.sequence != history.next_sequence => {
                    let Some(offset) = history.find(batch) else {
                        return Ok(Err(Error::OutOfOrderSequence));
                    };
                    return self.read(key).map(|authorized| {
                        authorized.map(|barrier| Write::Existing(offset, barrier))
                    });
                }
                None if batch.sequence != 0 => return Ok(Err(Error::UnknownProducerId)),
                None if node.histories.len() >= MAX_PRODUCER_STATES => {
                    return Ok(Err(Error::PolicyViolation));
                }
                _ => {}
            }
        }
        let offset = node.raw.store().log.end().next_offset;
        if offset.checked_add(i64::from(batch.records)).is_none() {
            return Ok(Err(Error::Storage));
        }
        let epoch = i32::try_from(node.raw.raft.term).context("Kafka leader epoch exhausted")?;
        let mut bytes = BytesMut::with_capacity(records.len());
        bytes.put_i64(offset);
        bytes.extend_from_slice(&records[8..12]);
        bytes.put_i32(epoch);
        bytes.extend_from_slice(&records[16..]);
        let reply = node.propose(bytes.to_vec(), offset)?;
        self.drain(key)?;
        Ok(Ok(Write::Pending(reply)))
    }

    pub fn read(&mut self, key: GroupId) -> Result<Result<Reply<()>, Error>> {
        let Some(node) = self.nodes.get_mut(&key) else {
            return Ok(Err(Error::NotLeader));
        };
        if node.leader() != self.settings.id {
            return Ok(Err(Error::NotLeader));
        }
        if node.committed_term != node.raw.raft.term || node.reads.len() == MAX_READ_BARRIERS {
            return Ok(Err(Error::RequestTimedOut));
        }
        self.next_barrier = self
            .next_barrier
            .checked_add(1)
            .context("read barrier counter exhausted")?;
        let token = self.next_barrier.to_be_bytes().to_vec();
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
        self.drain(key)?;
        Ok(Ok(receiver))
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

    pub fn receive_join(&mut self, source: u64) -> Result<()> {
        if self.settings.peers.contains_key(&source)
            && !self.voters().contains(&source)
            && !self.retired.contains(&source)
            && self.drain.as_ref().is_none_or(|plan| plan.node != source)
        {
            self.join_requests.insert(source, Instant::now());
        }
        Ok(())
    }

    fn advance_membership(&mut self) -> Result<()> {
        self.join_requests.retain(|id, since| {
            since.elapsed() < Duration::from_secs(2)
                && !self.retired.contains(id)
                && self.drain.as_ref().is_none_or(|plan| plan.node != *id)
        });
        let node = self.nodes.get_mut(&CONTROL).expect("control group");
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
        let change = if let Some(id) = promotion {
            Some((id, ConfChangeType::AddNode))
        } else {
            self.join_requests
                .keys()
                .copied()
                .find(|id| !membership.voters.contains(id) && !membership.learners.contains(id))
                .map(|id| (id, ConfChangeType::AddLearnerNode))
        };
        if let Some((id, kind)) = change {
            let mut change = ConfChange {
                node_id: id,
                ..ConfChange::default()
            };
            change.set_change_type(kind);
            node.raw.propose_conf_change(Vec::new(), change)?;
            self.drain(CONTROL)?;
        }
        Ok(())
    }

    pub fn tick(&mut self) -> Result<()> {
        let keys: Vec<_> = self.nodes.keys().copied().collect();
        for key in keys {
            if let Some(node) = self.nodes.get_mut(&key) {
                node.raw.tick();
                self.drain(key)?;
            }
        }
        if self.last_report.elapsed() >= Duration::from_millis(250) {
            self.last_report = Instant::now();
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
            self.drive_drain()?;
            if self.drain.is_none() {
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
        Ok(())
    }

    pub fn receive(&mut self, key: GroupId, message: Message) -> Result<()> {
        if self.retired.contains(&message.from) {
            return Ok(());
        }
        if let Some(node) = self.nodes.get_mut(&key) {
            // A transport response can be obsolete after a leadership change.
            let _ = node.raw.step(message);
            self.drain(key)?;
        }
        Ok(())
    }

    fn drain(&mut self, key: GroupId) -> Result<()> {
        let node = self.nodes.get_mut(&key).expect("existing group");
        let mut events = Vec::new();
        let mut completions = Vec::new();
        while node.raw.has_ready() {
            let mut ready = node.raw.ready();
            ensure!(ready.snapshot().is_empty(), "unexpected Raft snapshot");
            node.raw.mut_store().persist(ready.hs(), ready.entries())?;
            let mut committed = ready.take_committed_entries();
            let messages = ready
                .take_messages()
                .into_iter()
                .chain(ready.take_persisted_messages());
            let states = ready.take_read_states();
            let mut light = node.raw.advance_append(ready);
            committed.extend(light.take_committed_entries());

            if let Some(last) = committed.last() {
                node.raw.mut_store().log.commit_through(last.index + 1)?;
                node.committed_term = last.term;
            }
            for entry in committed {
                if entry.get_entry_type() == EntryType::EntryConfChange {
                    let change = ConfChange::parse_from_bytes(&entry.data)?;
                    ensure!(
                        self.settings.peers.contains_key(&change.node_id),
                        "peer configuration is missing a committed cluster member"
                    );
                    let membership = node.raw.apply_conf_change(&change)?;
                    node.raw.mut_store().membership = membership;
                } else if !entry.data.is_empty() {
                    if key == CONTROL {
                        events.push(serde_json::from_slice(&entry.data)?);
                    } else {
                        node.note_batch(&entry.data)?;
                    }
                }
                if node
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.term == entry.term && p.index == entry.index)
                {
                    let pending = node.pending.take().expect("matching pending entry");
                    completions.push(pending);
                }
                node.raw.advance_apply_to(entry.index);
            }
            for state in states {
                if let Some(read) = node.reads.get_mut(&state.request_ctx) {
                    read.authorized = Some(state.index);
                }
            }
            for message in messages.chain(light.take_messages()) {
                if let Some(queue) = self.outbound.get(&message.to) {
                    Queued::send(queue, &self.send_bytes, Packet::Raft(key, message));
                }
            }
        }
        let local_leader = node.raw.raft.state == StateRole::Leader;
        let term = node.raw.raft.term;
        if node
            .pending
            .as_ref()
            .is_some_and(|pending| !local_leader || pending.term != term)
        {
            let pending = node.pending.take().expect("old pending entry");
            let _ = pending.reply.send(Err(Error::NotLeader));
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
        for event in events {
            self.apply(event)?;
        }
        for pending in completions {
            let _ = pending.reply.send(Ok(pending.offset));
        }
        Ok(())
    }
}

pub enum Write {
    Existing(i64, Reply<()>),
    Pending(Reply<i64>),
}

impl Node {
    fn open(settings: &Settings, path: &Path, voters: Vec<u64>, control: bool) -> Result<Self> {
        let mut histories = HashMap::<i64, History>::new();
        let disk = Disk::open(path, voters, control, |entry, payload| {
            let batch = Batch::parse(payload).map_err(std::io::Error::other)?;
            if batch.producer > 0 {
                if !histories.contains_key(&batch.producer)
                    && histories.len() >= MAX_PRODUCER_STATES
                {
                    return Err(std::io::Error::other(
                        "partition producer state exceeds limit",
                    ));
                }
                histories
                    .entry(batch.producer)
                    .or_default()
                    .add(batch, entry.position.next_offset);
            }
            Ok(())
        })?;
        let end = disk.log.committed().next_index;
        let committed_term = disk.log.term(end - 1)?;
        let config = Config {
            id: settings.id,
            applied: end - 1,
            election_tick: 12,
            heartbeat_tick: 2,
            pre_vote: true,
            check_quorum: true,
            max_size_per_msg: MAX_BATCH as u64 + 4096,
            max_inflight_msgs: 4,
            max_committed_size_per_ready: 2 * MAX_BATCH as u64,
            max_uncommitted_size: 2 * MAX_BATCH as u64,
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
            pending: None,
            reads: HashMap::new(),
            histories,
            committed_term,
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
            && self.pending.is_none()
            && self.log().end() == self.log().committed()
    }

    fn propose(&mut self, bytes: Vec<u8>, offset: i64) -> Result<Reply<i64>> {
        let (reply, receiver) = oneshot::channel();
        let index = self.raw.store().last_index()? + 1;
        let term = self.raw.raft.term;
        self.raw.propose(Vec::new(), bytes)?;
        self.pending = Some(Pending {
            index,
            term,
            reply,
            offset,
        });
        Ok(receiver)
    }

    fn note_batch(&mut self, bytes: &[u8]) -> Result<()> {
        let batch = Batch::parse(bytes)?;
        if batch.producer > 0 {
            ensure!(
                self.histories.contains_key(&batch.producer)
                    || self.histories.len() < MAX_PRODUCER_STATES,
                "partition producer state exceeds limit"
            );
            let offset = i64::from_be_bytes(bytes[..8].try_into()?);
            self.histories
                .entry(batch.producer)
                .or_default()
                .add(batch, offset);
        }
        Ok(())
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
