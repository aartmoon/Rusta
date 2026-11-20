use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusta_log::{Options, PartitionLog, Retention};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Group, MAX_GROUPS, checkpoint, now_ms};
use crate::protocol::Error;

const MAX_OFFSETS: usize = 2048;
const MAX_OFFSET_BYTES: usize = 128 * 1024;
pub const MAX_METADATA: usize = 4096;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Key {
    pub topic: Uuid,
    pub partition: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Offset {
    pub value: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
}

impl Offset {
    fn charge(&self) -> usize {
        96 + self.metadata.as_ref().map_or(0, String::len)
    }
}

pub type Offsets = HashMap<Key, Offset>;

#[derive(Deserialize, Serialize)]
pub enum Event {
    Generation {
        group: String,
        id: i32,
    },
    Stable {
        group: String,
        id: i32,
        assignments: Vec<(String, Vec<u8>)>,
    },
    Commit {
        group: String,
        offsets: Vec<(Key, Offset)>,
        empty_since: Option<u64>,
    },
    Empty {
        group: String,
        since: u64,
    },
    Expire {
        group: String,
    },
}

pub struct Store {
    log: PartitionLog,
    path: PathBuf,
}

pub const JOURNAL_RETENTION: Retention = Retention {
    max_bytes: Some(16 * 1024 * 1024),
    keep_since_ms: None,
};

impl Store {
    pub fn open(path: &Path) -> Result<(Self, HashMap<String, Group>)> {
        let log = PartitionLog::open(
            path,
            Options {
                segment_bytes: 8 * 1024 * 1024,
                ..Options::default()
            },
        )
        .context("open group journal")?;
        let end = log.committed().next_index;
        let (mut start, mut groups) = checkpoint::load(path, log.start().next_index, end)
            .context("recover group checkpoint")?;
        while start < end {
            start = log.visit_entries(start, end, 1024 * 1024, |_, data| {
                let event = serde_json::from_slice(data)?;
                apply(&mut groups, event)
            })?;
        }

        let mut store = Self {
            log,
            path: path.to_owned(),
        };
        let time = now_ms();
        // Sessions are process-local. Persist the first observed empty time after a restart.
        for (id, group) in &mut groups {
            if group.empty_since.is_none() {
                store.append(Event::Empty {
                    group: id.clone(),
                    since: time,
                })?;
                group.empty_since = Some(time);
            }
        }
        Ok((store, groups))
    }

    pub fn append(&mut self, event: Event) -> Result<()> {
        let encoded = serde_json::to_vec(&event)?;
        self.log.append_internal(1, now_ms() as i64, &encoded)?;
        self.log.commit()?;
        Ok(())
    }

    pub fn compact(
        &mut self,
        groups: &HashMap<String, Group>,
        retention: Retention,
    ) -> io::Result<usize> {
        let next = self.log.committed().next_index;
        self.log
            .apply_retention_after(retention, || checkpoint::save(&self.path, next, groups))
    }
}

fn apply(groups: &mut HashMap<String, Group>, event: Event) -> io::Result<()> {
    let group = match &event {
        Event::Generation { group, .. }
        | Event::Stable { group, .. }
        | Event::Commit { group, .. }
        | Event::Empty { group, .. }
        | Event::Expire { group } => group,
    };
    if let Event::Expire { group } = event {
        groups.remove(&group);
        return Ok(());
    }
    if !groups.contains_key(group) && groups.len() >= MAX_GROUPS {
        return Err(io::Error::other("group journal exceeds group capacity"));
    }
    let state = groups.entry(group.clone()).or_default();
    match event {
        Event::Generation { id, .. } => {
            if id <= state.generation {
                return Err(io::Error::other("group journal generation moved backwards"));
            }
            state.generation = id;
            state.empty_since = None;
        }
        Event::Stable { id, .. } => {
            if id != state.generation {
                return Err(io::Error::other(
                    "stable group generation does not match its join",
                ));
            }
        }
        Event::Commit {
            offsets,
            empty_since,
            ..
        } => {
            let mut budget = OffsetBudget::new(&state.offsets);
            for (key, offset) in &offsets {
                budget
                    .admit(*key, offset)
                    .map_err(|error| io::Error::other(format!("{error:?}")))?;
            }
            state.offsets.extend(offsets);
            state.empty_since = empty_since;
        }
        Event::Empty { since, .. } => state.empty_since = Some(since),
        Event::Expire { .. } => unreachable!(),
    }
    Ok(())
}

pub struct OffsetBudget<'a> {
    existing: &'a Offsets,
    seen: HashSet<Key>,
    count: usize,
    bytes: usize,
    request_bytes: usize,
}

impl<'a> OffsetBudget<'a> {
    pub fn new(existing: &'a Offsets) -> Self {
        Self {
            existing,
            seen: HashSet::new(),
            count: existing.len(),
            bytes: existing.values().map(Offset::charge).sum(),
            request_bytes: 0,
        }
    }

    pub fn admit(&mut self, key: Key, offset: &Offset) -> Result<(), Error> {
        if offset
            .metadata
            .as_ref()
            .is_some_and(|value| value.len() > MAX_METADATA)
        {
            return Err(Error::OffsetMetadataTooLarge);
        }
        if !self.seen.insert(key) || offset.value < 0 {
            return Err(Error::InvalidRequest);
        }
        let old = self.existing.get(&key);
        let count = self.count + usize::from(old.is_none());
        let bytes = self.bytes - old.map_or(0, Offset::charge) + offset.charge();
        let request_bytes = self.request_bytes + offset.charge();
        if count > MAX_OFFSETS || bytes > MAX_OFFSET_BYTES || request_bytes > MAX_OFFSET_BYTES {
            return Err(Error::PolicyViolation);
        }
        self.count = count;
        self.bytes = bytes;
        self.request_bytes = request_bytes;
        Ok(())
    }
}
