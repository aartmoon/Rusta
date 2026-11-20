use super::*;
use crate::cluster::checkpoint;
use crate::groups::{Key, Offset, OffsetBudget, valid_id};

pub(super) fn interval() -> Result<u64> {
    match std::env::var("RUSTA_CLUSTER_SNAPSHOT_INTERVAL") {
        Ok(value) => {
            let value = value.parse()?;
            ensure!(value > 0, "internal snapshot interval must be positive");
            Ok(value)
        }
        Err(std::env::VarError::NotPresent) => Ok(4096),
        Err(error) => Err(error.into()),
    }
}

#[derive(Deserialize, Serialize)]
pub(super) struct GroupState(Vec<StoredGroup>);

#[derive(Deserialize, Serialize)]
struct StoredGroup {
    name: String,
    generation: i32,
    empty_since: Option<u64>,
    offsets: Vec<(Key, Offset)>,
}

impl GroupState {
    fn capture(groups: &HashMap<String, Group>) -> Self {
        Self(
            groups
                .iter()
                .map(|(name, group)| StoredGroup {
                    name: name.clone(),
                    generation: group.generation,
                    empty_since: group.empty_since,
                    offsets: group
                        .offsets
                        .iter()
                        .map(|(key, offset)| (*key, offset.clone()))
                        .collect(),
                })
                .collect(),
        )
    }

    pub(super) fn restore(self) -> Result<HashMap<String, Group>> {
        ensure!(
            self.0.len() <= groups::MAX_GROUPS / GROUP_SHARDS,
            "group snapshot exceeds shard capacity"
        );
        let mut groups = HashMap::new();
        for stored in self.0 {
            ensure!(
                valid_id(&stored.name) && stored.generation >= 0,
                "invalid snapshot group"
            );
            let mut group = Group::default();
            group.generation = stored.generation;
            group.empty_since = stored.empty_since;
            let mut budget = OffsetBudget::new(&group.offsets);
            let mut offsets = HashMap::new();
            for (key, offset) in stored.offsets {
                budget
                    .admit(key, &offset)
                    .map_err(|error| anyhow::anyhow!("invalid snapshot offset: {error:?}"))?;
                offsets.insert(key, offset);
            }
            group.offsets = offsets;
            ensure!(
                groups.insert(stored.name, group).is_none(),
                "duplicate snapshot group"
            );
        }
        Ok(groups)
    }
}

pub(super) fn group_state(bytes: &[u8]) -> Result<HashMap<String, Group>> {
    let (_, state) = checkpoint::snapshot_parts(bytes, true)?;
    checkpoint::decode_state::<GroupState>(state)?.restore()
}

#[derive(Deserialize, Serialize)]
pub(super) struct ControlState {
    topics: Vec<TopicConfig>,
    assignments: Vec<Assignment>,
    retired: BTreeSet<u64>,
    last_rebalance: Option<Uuid>,
    imported_producer_floor: i64,
}

#[derive(Deserialize, Serialize)]
struct Assignment {
    key: GroupId,
    start_offset: i64,
    origin: Vec<u64>,
    current: Vec<u64>,
}

impl ControlState {
    pub(super) fn validate(&self, settings: &Settings) -> Result<()> {
        let mut names = BTreeSet::new();
        let mut topics = HashMap::new();
        let mut total = 0;
        for topic in &self.topics {
            ensure!(
                (1..=MAX_CLUSTER_PARTITIONS).contains(&topic.partitions)
                    && !topic.id.is_nil()
                    && names.insert(&topic.name)
                    && topics.insert(topic.id, topic.partitions).is_none(),
                "invalid snapshot topic"
            );
            total += topic.partitions;
        }
        ensure!(
            total <= MAX_CLUSTER_PARTITIONS
                && self.assignments.len() <= total + GROUP_SHARDS
                && self
                    .retired
                    .iter()
                    .all(|id| settings.peers.contains_key(id))
                && (0..=i64::MAX - (4096 << 8)).contains(&self.imported_producer_floor),
            "invalid controller snapshot metadata"
        );
        let mut keys = BTreeSet::new();
        for assignment in &self.assignments {
            let key = assignment.key;
            ensure!(
                keys.insert(key)
                    && (is_group_shard(key) && assignment.start_offset == 0
                        || topics
                            .get(&key.topic)
                            .is_some_and(|&n| (key.partition as usize) < n)
                            && assignment.start_offset >= 0)
                    && assignment.origin.len() == assignment.current.len()
                    && !assignment
                        .current
                        .iter()
                        .any(|id| self.retired.contains(id)),
                "invalid snapshot placement"
            );
            for ids in [&assignment.origin, &assignment.current] {
                ensure!(
                    !ids.is_empty()
                        && ids.iter().all(|id| settings.peers.contains_key(id))
                        && ids.iter().collect::<BTreeSet<_>>().len() == ids.len(),
                    "invalid controller snapshot replicas"
                );
            }
        }
        let shards = keys.iter().filter(|&&key| is_group_shard(key)).count();
        ensure!(
            (shards == 0 || shards == GROUP_SHARDS) && keys.len() == total + shards,
            "incomplete snapshot placements"
        );
        Ok(())
    }
}

pub(super) fn control_state(bytes: &[u8]) -> Result<ControlState> {
    let (_, state) = checkpoint::snapshot_parts(bytes, true)?;
    checkpoint::decode_state(state)
}

impl Engine {
    pub(super) fn compact_internal(&mut self) -> Result<()> {
        let interval = self.internal_snapshot_interval;
        if self.relocation.is_none()
            && self.nodes[&CONTROL]
                .raw
                .store()
                .internal_compaction_due(interval)
        {
            return self.snapshot_control();
        }
        if let Some(node) = self
            .nodes
            .values_mut()
            .find(|node| node.group.is_some() && node.raw.store().internal_compaction_due(interval))
        {
            let shard = node.group.as_ref().expect("group shard");
            let state = GroupState::capture(&shard.committed);
            node.raw.mut_store().compact_internal(&state)?;
        }
        Ok(())
    }

    pub(super) fn snapshot_control(&mut self) -> Result<()> {
        let state = ControlState {
            topics: self.topics.values().cloned().collect(),
            assignments: self
                .replicas
                .iter()
                .map(|(&key, current)| Assignment {
                    key,
                    start_offset: self.imported_offsets.get(&key).copied().unwrap_or(0),
                    origin: self.origins[&key].clone(),
                    current: current.clone(),
                })
                .collect(),
            retired: self.retired.clone(),
            last_rebalance: self.last_rebalance,
            imported_producer_floor: self.imported_producer_floor,
        };
        self.nodes
            .get_mut(&CONTROL)
            .expect("control group")
            .raw
            .mut_store()
            .compact_internal(&state)
    }

    pub(super) fn restore_control(&mut self, state: ControlState) -> Result<()> {
        state.validate(&self.settings)?;
        self.flush_page_cache()?;
        self.nodes.retain(|key, _| *key == CONTROL);
        self.replicas.clear();
        self.origins.clear();
        self.imported_offsets.clear();
        self.retired = state.retired;
        self.relocation = None;
        self.move_ready = false;
        self.last_rebalance = state.last_rebalance;
        self.leaders.clear();
        self.drain_requests.retain(|id| !self.retired.contains(id));
        self.join_requests
            .retain(|id, _| !self.retired.contains(id));
        self.topics = state
            .topics
            .into_iter()
            .map(|topic| (topic.name.clone(), topic))
            .collect();
        self.names = self
            .topics
            .iter()
            .map(|(name, topic)| (topic.id, name.clone()))
            .collect();
        self.imported_producer_floor = 0;
        if state.imported_producer_floor != 0 {
            self.apply(Event::ImportedProducers(state.imported_producer_floor))?;
        }
        for assignment in state.assignments {
            let key = assignment.key;
            if assignment.start_offset != 0 {
                self.imported_offsets.insert(key, assignment.start_offset);
            }
            self.origins.insert(key, assignment.origin);
            self.replicas.insert(key, assignment.current);
        }
        if !self.rebuilding_catalog {
            self.open_current_replicas()?;
        }
        self.data_available.notify_waiters();
        self.group_progress.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_checkpoint_round_trips_a_large_shard() -> Result<()> {
        let mut entropy = 0x1d2e_3f40_5162_7384_u64;
        let mut metadata = || {
            (0..4096)
                .map(|_| {
                    entropy ^= entropy << 13;
                    entropy ^= entropy >> 7;
                    entropy ^= entropy << 17;
                    (b' ' + (entropy % 95) as u8) as char
                })
                .collect::<String>()
        };
        let count = groups::MAX_GROUPS / GROUP_SHARDS;
        let topic = Uuid::new_v4();
        let expected = GroupState(
            (0..count)
                .map(|group| StoredGroup {
                    name: format!("group-{group}"),
                    generation: 3,
                    empty_since: Some(227),
                    offsets: (0..30)
                        .map(|partition| {
                            (
                                Key { topic, partition },
                                Offset {
                                    value: 42,
                                    leader_epoch: 7,
                                    metadata: Some(metadata()),
                                },
                            )
                        })
                        .collect(),
                })
                .collect(),
        );
        let compressed = checkpoint::encode_state(&expected)?;
        assert!(compressed.len() > 2 * 1024 * 1024);
        let restored: GroupState = checkpoint::decode_state(&compressed)?;
        let restored = restored.restore()?;
        assert_eq!(restored.len(), count);
        for saved in expected.0 {
            let group = &restored[&saved.name];
            assert_eq!(group.generation, saved.generation);
            assert_eq!(group.empty_since, saved.empty_since);
            for (key, offset) in saved.offsets {
                assert_eq!(group.offsets[&key].metadata, offset.metadata);
                assert_eq!(group.offsets[&key].value, offset.value);
            }
        }
        Ok(())
    }
}
