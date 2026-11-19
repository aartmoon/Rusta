use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use rusta_log::{Options, PartitionLog};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::partition::Partition;

pub const MAX_PARTITIONS: usize = 512;
const PRODUCER_ID_BLOCK: i64 = 4096;
pub const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    pub name: String,
    pub id: Uuid,
    pub partitions: usize,
    pub allow_weak_acks: bool,
    #[serde(default)]
    pub retention_ms: Option<i64>,
    #[serde(default)]
    pub retention_bytes: Option<i64>,
}

pub struct Topic {
    pub config: TopicConfig,
    pub partitions: Vec<Partition>,
}

pub struct Catalog {
    pub topics: BTreeMap<String, Topic>,
    pub names_by_id: HashMap<Uuid, String>,
    pub cluster_id: Uuid,
    state: PartitionLog,
    data_dir: PathBuf,
    next_producer: i64,
    last_reserved_producer: i64,
    pub partition_count: usize,
}

#[derive(Deserialize, Serialize)]
enum Event {
    Cluster(Uuid),
    Topic(TopicConfig),
    ReserveProducers(i64),
}

impl Catalog {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root).context("create broker data directory")?;
        let mut state = PartitionLog::open(root.join("catalog"), Options::default())
            .context("open the broker catalog")?;
        let data_dir = root.join("partitions");
        fs::create_dir_all(&data_dir)?;
        File::open(root)?.sync_all()?;

        let mut cluster_id = None;
        let mut topics = BTreeMap::new();
        let mut names_by_id = HashMap::new();
        let mut partition_count = 0;
        let mut last_reserved_producer = 0;
        let mut next = state.start().next_index;
        let end = state.committed().next_index;
        while next < end {
            next = state.visit_entries(next, end, 1024 * 1024, |_, payload| {
                let event: Event = serde_json::from_slice(payload)?;
                match event {
                    Event::Cluster(id) => {
                        if cluster_id.replace(id).is_some() {
                            return Err(std::io::Error::other("duplicate cluster ID"));
                        }
                    }
                    Event::Topic(config) => {
                        partition_count += config.partitions;
                        if partition_count > MAX_PARTITIONS
                            || config.partitions == 0
                            || names_by_id.insert(config.id, config.name.clone()).is_some()
                            || topics.contains_key(&config.name)
                        {
                            return Err(std::io::Error::other("invalid topic catalog entry"));
                        }
                        let topic =
                            open_topic(&data_dir, config, false).map_err(std::io::Error::other)?;
                        topics.insert(topic.config.name.clone(), topic);
                    }
                    Event::ReserveProducers(high) => {
                        if high <= last_reserved_producer {
                            return Err(std::io::Error::other(
                                "nonmonotonic producer ID allocation",
                            ));
                        }
                        last_reserved_producer = high;
                    }
                }
                Ok(())
            })?;
        }

        let cluster_id = match cluster_id {
            Some(id) => id,
            None => {
                ensure!(end == 1, "missing cluster ID in catalog");
                let id = Uuid::new_v4();
                store(&mut state, &Event::Cluster(id))?;
                id
            }
        };
        Ok(Self {
            topics,
            names_by_id,
            cluster_id,
            state,
            data_dir,
            next_producer: last_reserved_producer + 1,
            last_reserved_producer,
            partition_count,
        })
    }

    pub fn create_topic(&mut self, config: TopicConfig) -> Result<()> {
        ensure!(
            self.partition_count + config.partitions <= MAX_PARTITIONS,
            "too many partitions"
        );
        let topic = open_topic(&self.data_dir, config.clone(), true)?;
        File::open(&self.data_dir)?.sync_all()?;
        store(&mut self.state, &Event::Topic(config))?;
        self.partition_count += topic.config.partitions;
        self.names_by_id
            .insert(topic.config.id, topic.config.name.clone());
        self.topics.insert(topic.config.name.clone(), topic);
        Ok(())
    }

    pub fn allocate_producer(&mut self) -> Result<i64> {
        if self.next_producer > self.last_reserved_producer {
            let high = self
                .last_reserved_producer
                .checked_add(PRODUCER_ID_BLOCK)
                .context("producer ID range exhausted")?;
            store(&mut self.state, &Event::ReserveProducers(high))?;
            self.last_reserved_producer = high;
        }
        let id = self.next_producer;
        self.next_producer += 1;
        Ok(id)
    }

    pub fn maintenance(&mut self, retention: bool) -> bool {
        let mut committed = false;
        for topic in self.topics.values_mut() {
            for partition in &mut topic.partitions {
                match partition.maintain(retention.then_some(&topic.config)) {
                    Ok(changed) => committed |= changed,
                    Err(error) => eprintln!("maintain {}: {error:#}", topic.config.name),
                }
            }
        }
        committed
    }

    pub fn last_reserved_producer(&self) -> i64 {
        self.last_reserved_producer
    }
}

fn store(state: &mut PartitionLog, event: &Event) -> Result<()> {
    state.append_internal(1, 0, &serde_json::to_vec(event)?)?;
    state.commit()?;
    Ok(())
}

fn open_topic(root: &Path, config: TopicConfig, create: bool) -> Result<Topic> {
    let dir = root.join(config.id.to_string());
    if create {
        fs::create_dir(&dir)?;
    }
    let partitions = (0..config.partitions)
        .map(|index| {
            let path = dir.join(index.to_string());
            if !create {
                ensure!(
                    path.join("manifest").is_file(),
                    "missing manifest for {} partition {index}",
                    config.name
                );
            }
            Partition::open(path)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Topic { config, partitions })
}
