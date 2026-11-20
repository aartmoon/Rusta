use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use protobuf::Message as ProtoMessage;
use raft::{
    GetEntriesContext, RaftState, Storage, StorageError,
    eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Snapshot},
};
use rusta_log::{Options, PartitionLog, Position};

use crate::partition::MAX_BATCH;

const STATE_MAGIC: &[u8; 8] = b"RUSTAR01";
const TIMESTAMPED_BATCH: u8 = 254;
const RETAINED_START: u8 = 253;
const MAX_READ_BYTES: u64 = 2 * 1024 * 1024;

pub struct Disk {
    pub log: PartitionLog,
    pub retained: Position,
    internal_entries: bool,
    root: PathBuf,
    pub membership: ConfState,
    election: (u64, u64),
}

impl Disk {
    pub fn open(
        root: &Path,
        peers: Vec<u64>,
        internal_entries: bool,
        mut recover_batch: impl FnMut(rusta_log::Entry, &[u8]) -> io::Result<()>,
    ) -> Result<Self> {
        let log = PartitionLog::open(
            root,
            Options {
                max_entry_bytes: MAX_BATCH,
                ..Options::default()
            },
        )?;
        let election = load_election(root, log.end().next_index > 1)?;
        let last = log.term(log.end().next_index - 1)?;
        ensure!(election.0 >= last, "persistent Raft term is below log term");
        let mut voters: BTreeSet<_> = peers.into_iter().collect();
        let mut learners = BTreeSet::new();
        let mut retained = log.start();
        let mut next = log.start().next_index;
        let end = log.committed().next_index;
        while next < end {
            next = log.visit_entries(next, end, MAX_READ_BYTES, |entry, payload| {
                if entry.record_count > 0 {
                    return recover_batch(entry, payload);
                }
                if !internal_entries && payload.first() == Some(&RETAINED_START) {
                    retained = check_retention(&log, retained, entry.position.next_index, payload)
                        .map_err(io::Error::other)?;
                } else if let Some(change) = decode_change(payload).map_err(io::Error::other)? {
                    match change.get_change_type() {
                        ConfChangeType::AddLearnerNode => {
                            if !voters.contains(&change.node_id) {
                                learners.insert(change.node_id);
                            }
                        }
                        ConfChangeType::AddNode => {
                            learners.remove(&change.node_id);
                            voters.insert(change.node_id);
                        }
                        ConfChangeType::RemoveNode => {
                            voters.remove(&change.node_id);
                            learners.remove(&change.node_id);
                        }
                    }
                }
                Ok(())
            })?;
        }
        let mut membership = ConfState::default();
        membership.set_voters(voters.into_iter().collect());
        membership.set_learners(learners.into_iter().collect());
        Ok(Self {
            log,
            retained,
            internal_entries,
            root: root.to_owned(),
            membership,
            election,
        })
    }

    pub fn persist(&mut self, election: Option<&HardState>, entries: &[Entry]) -> Result<()> {
        // Persist term and vote before entries. A crash may leave a newer term
        // with an older log, while it must never leave an entry from a future term.
        if let Some(state) = election {
            let updated = (state.term, state.vote);
            if updated != self.election {
                save_election(&self.root, updated)?;
                self.election = updated;
            }
        }
        if let Some(entry) = entries.first() {
            ensure!(
                entry.index <= self.log.end().next_index,
                "gap in Raft Ready entries"
            );
            if entry.index < self.log.end().next_index {
                self.log.truncate_suffix(entry.index)?;
            }
        }
        for entry in entries {
            ensure!(
                entry.index == self.log.end().next_index,
                "nonconsecutive Raft entries"
            );
            ensure!(
                entry.get_entry_type() == EntryType::EntryNormal
                    || entry.get_entry_type() == EntryType::EntryConfChange,
                "unsupported membership entry"
            );
            ensure!(
                entry.term <= self.election.0,
                "Raft entry from an unpersisted term"
            );
            if entry.get_entry_type() == EntryType::EntryConfChange {
                let mut data = Vec::with_capacity(entry.data.len() + 1);
                data.push(255);
                data.extend_from_slice(&entry.data);
                self.log.append_internal(entry.term, 0, &data)?;
            } else if self.internal_entries
                || entry.data.is_empty()
                || entry.data.first() == Some(&RETAINED_START)
            {
                self.log.append_internal(entry.term, 0, &entry.data)?;
            } else {
                let (timestamp, batch) = decode_batch(&entry.data)?;
                self.log.append_batch(entry.term, timestamp, batch)?;
            }
        }
        if !entries.is_empty() {
            self.log.sync()?;
        }
        Ok(())
    }
    /// Apply a committed data-partition entry, returning its batch if it carries records.
    pub fn apply_data<'a>(&mut self, index: u64, data: &'a [u8]) -> Result<Option<&'a [u8]>> {
        if data.first() == Some(&RETAINED_START) {
            self.retained = check_retention(&self.log, self.retained, index, data)?;
            return Ok(None);
        }
        Ok(Some(decode_batch(data)?.1))
    }
}

pub fn encode_batch(timestamp: i64, batch: &[u8]) -> Vec<u8> {
    if timestamp == 0 {
        return batch.to_vec();
    }
    let mut bytes = Vec::with_capacity(9 + batch.len());
    bytes.push(TIMESTAMPED_BATCH);
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(batch);
    bytes
}

fn decode_batch(data: &[u8]) -> Result<(i64, &[u8])> {
    if data.first() != Some(&TIMESTAMPED_BATCH) {
        return Ok((0, data));
    }
    ensure!(data.len() > 9, "truncated timestamped batch");
    let timestamp = i64::from_be_bytes(data[1..9].try_into()?);
    ensure!(timestamp > 0, "invalid Raft append timestamp");
    Ok((timestamp, &data[9..]))
}

pub fn encode_retention(start: Position) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(17);
    bytes.push(RETAINED_START);
    bytes.extend_from_slice(&start.next_index.to_be_bytes());
    bytes.extend_from_slice(&start.next_offset.to_be_bytes());
    bytes
}

fn check_retention(
    log: &PartitionLog,
    previous: Position,
    index: u64,
    data: &[u8],
) -> Result<Position> {
    ensure!(data.len() == 17, "invalid retained start record");
    let start = Position {
        next_index: u64::from_be_bytes(data[1..9].try_into()?),
        next_offset: i64::from_be_bytes(data[9..].try_into()?),
    };
    ensure!(
        start.next_index > previous.next_index
            && start.next_index <= index
            && start.next_offset > previous.next_offset
            && log.position_at(start.next_index)? == start,
        "invalid retained log boundary"
    );
    Ok(start)
}

pub fn decode_change(bytes: &[u8]) -> Result<Option<ConfChange>> {
    if bytes.first() == Some(&255) {
        Ok(Some(ConfChange::parse_from_bytes(&bytes[1..])?))
    } else {
        Ok(None)
    }
}

fn other(error: io::Error) -> raft::Error {
    StorageError::Other(Box::new(error)).into()
}

impl Storage for Disk {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let state = HardState {
            term: self.election.0,
            vote: self.election.1,
            commit: self.log.committed().next_index - 1,
            ..HardState::default()
        };
        Ok(RaftState::new(state, self.membership.clone()))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        limit: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if high > self.last_index()? + 1 {
            return Err(StorageError::Unavailable.into());
        }
        let mut result = Vec::new();
        self.log
            .visit_entries(
                low,
                high,
                limit.into().unwrap_or(u64::MAX).min(MAX_READ_BYTES),
                |stored, bytes| {
                    let mut entry = Entry {
                        index: stored.position.next_index,
                        term: stored.term,
                        ..Entry::default()
                    };
                    if stored.record_count == 0 && bytes.first() == Some(&255) {
                        entry.set_entry_type(EntryType::EntryConfChange);
                        entry.set_data(bytes[1..].to_vec().into());
                    } else if stored.record_count > 0 {
                        entry.set_data(encode_batch(stored.append_timestamp_ms, bytes).into());
                    } else {
                        entry.set_data(bytes.to_vec().into());
                    }
                    result.push(entry);
                    Ok(())
                },
            )
            .map_err(other)?;
        Ok(result)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index < self.first_index()? - 1 {
            return Err(StorageError::Compacted.into());
        }
        if index > self.last_index()? {
            return Err(StorageError::Unavailable.into());
        }
        self.log.term(index).map_err(other)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.log.start().next_index)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.log.end().next_index - 1)
    }

    fn snapshot(&self, _index: u64, _to: u64) -> raft::Result<Snapshot> {
        Err(StorageError::SnapshotTemporarilyUnavailable.into())
    }
}

fn load_election(root: &Path, required: bool) -> Result<(u64, u64)> {
    let path = root.join("election");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !required => return Ok((0, 0)),
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    ensure!(
        bytes.len() == 28 && &bytes[..8] == STATE_MAGIC,
        "invalid election state"
    );
    let crc = u32::from_be_bytes(bytes[24..].try_into()?);
    ensure!(
        crc32c::crc32c(&bytes[..24]) == crc,
        "election state checksum mismatch"
    );
    Ok((
        u64::from_be_bytes(bytes[8..16].try_into()?),
        u64::from_be_bytes(bytes[16..24].try_into()?),
    ))
}

pub fn replace(root: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let temporary = root.join(format!("{name}.tmp"));
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, root.join(name))?;
    File::open(root)?.sync_all()
}

fn save_election(root: &Path, election: (u64, u64)) -> Result<()> {
    let mut data = Vec::with_capacity(28);
    data.extend_from_slice(STATE_MAGIC);
    data.extend_from_slice(&election.0.to_be_bytes());
    data.extend_from_slice(&election.1.to_be_bytes());
    data.extend_from_slice(&crc32c::crc32c(&data).to_be_bytes());
    replace(root, "election", &data)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_ignores_durable_uncommitted_retention() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("partition");
        let mut disk = Disk::open(&path, vec![1, 2, 3], false, |_, _| Ok(()))?;
        let initial = disk.retained;
        let mut batch = vec![0_u8; 61];
        batch[8..12].copy_from_slice(&49_i32.to_be_bytes());
        batch[16] = 2;
        batch[57..61].copy_from_slice(&1_i32.to_be_bytes());
        let checksum = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&checksum.to_be_bytes());
        let data = encode_batch(100, &batch);
        disk.persist(
            Some(&HardState {
                term: 1,
                ..HardState::default()
            }),
            &[Entry {
                index: 1,
                term: 1,
                data: data.clone().into(),
                ..Entry::default()
            }],
        )?;
        disk.log.commit_through(2)?;
        let boundary = disk.log.committed();
        let marker = encode_retention(boundary);
        disk.persist(
            None,
            &[Entry {
                index: 2,
                term: 1,
                data: marker.clone().into(),
                ..Entry::default()
            }],
        )?;
        assert_eq!(disk.log.durable().next_index, 3);
        drop(disk);

        let mut disk = Disk::open(&path, vec![1, 2, 3], false, |_, _| Ok(()))?;
        assert_eq!(disk.retained, initial);
        assert_eq!(disk.log.committed(), boundary);
        let entries = disk.entries(1, 3, None, GetEntriesContext::empty(false))?;
        assert_eq!(entries[0].data.as_ref(), data.as_slice());
        assert_eq!(entries[1].data.as_ref(), marker.as_slice());
        disk.log.commit_through(3)?;
        assert!(disk.apply_data(2, &marker)?.is_none());
        assert_eq!(disk.retained, boundary);
        drop(disk);

        let disk = Disk::open(&path, vec![1, 2, 3], false, |_, _| Ok(()))?;
        assert_eq!(disk.retained, boundary);
        assert_eq!(disk.log.start(), initial);
        Ok(())
    }
}
