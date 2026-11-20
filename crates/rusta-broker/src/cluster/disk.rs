use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use protobuf::Message as ProtoMessage;
use raft::{
    GetEntriesContext, RaftState, Storage, StorageError,
    eraftpb::{
        ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Snapshot,
        SnapshotMetadata,
    },
};
use rusta_log::{Options, PartitionLog, Position};

use crate::partition::MAX_BATCH;

use super::checkpoint::{self, DataCheckpoint};
use super::producers::{IMPORTED_PRODUCERS, PRODUCER_CLOCK, Producers, RETIRE_PRODUCERS};

const STATE_MAGIC: &[u8; 8] = b"RUSTAR01";
const TIMESTAMPED_BATCH: u8 = 254;
const RETAINED_START: u8 = 253;
const MAX_READ_BYTES: u64 = 2 * 1024 * 1024;
const CHECKPOINT_INTERVAL: u64 = 4096;

pub struct Disk {
    pub log: PartitionLog,
    pub retained: Position,
    pub producers: Producers,
    pub compacted_data_epoch: Option<i32>,
    internal_entries: bool,
    root: PathBuf,
    pub membership: ConfState,
    election: (u64, u64),
    checkpoint_next_index: u64,
    bootstrap: Vec<u64>,
}

struct Recovered {
    retained: Position,
    producers: Producers,
    voters: BTreeSet<u64>,
    learners: BTreeSet<u64>,
    last_data_epoch: Option<i32>,
}

impl Recovered {
    fn new(log: &PartitionLog, bootstrap: &[u64], state: Option<DataCheckpoint>) -> Self {
        if let Some(state) = state {
            return Self {
                retained: state.retained(),
                producers: state.producers,
                voters: state.voters.into_iter().collect(),
                learners: state.learners.into_iter().collect(),
                last_data_epoch: state.last_data_epoch,
            };
        }
        Self {
            retained: log.start(),
            producers: Producers::default(),
            voters: bootstrap.iter().copied().collect(),
            learners: BTreeSet::new(),
            last_data_epoch: None,
        }
    }

    fn replay(
        &mut self,
        log: &PartitionLog,
        mut next: u64,
        end: u64,
        internal: bool,
    ) -> Result<()> {
        while next < end {
            next = log.visit_entries(next, end, MAX_READ_BYTES, |entry, payload| {
                if entry.record_count > 0 {
                    self.last_data_epoch = Some(batch_epoch(payload));
                    return self
                        .producers
                        .note_batch(
                            payload,
                            entry.position.next_offset,
                            entry.append_timestamp_ms,
                        )
                        .map_err(io::Error::other);
                }
                if !internal && !payload.is_empty() && payload[0] != 255 {
                    return apply_data_state(
                        log,
                        &mut self.retained,
                        &mut self.producers,
                        entry.position.next_index,
                        payload,
                    )
                    .map_err(io::Error::other);
                }
                if let Some(change) = decode_change(payload).map_err(io::Error::other)? {
                    match change.get_change_type() {
                        ConfChangeType::AddLearnerNode => {
                            if !self.voters.contains(&change.node_id) {
                                self.learners.insert(change.node_id);
                            }
                        }
                        ConfChangeType::AddNode => {
                            self.learners.remove(&change.node_id);
                            self.voters.insert(change.node_id);
                        }
                        ConfChangeType::RemoveNode => {
                            self.voters.remove(&change.node_id);
                            self.learners.remove(&change.node_id);
                        }
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn membership(&self) -> ConfState {
        ConfState {
            voters: self.voters.iter().copied().collect(),
            learners: self.learners.iter().copied().collect(),
            ..ConfState::default()
        }
    }
}

impl Disk {
    #[cfg(test)]
    pub fn open(
        root: &Path,
        peers: Vec<u64>,
        internal_entries: bool,
        initial_offset: i64,
    ) -> Result<Self> {
        Self::open_replica(root, peers, internal_entries, initial_offset, None)
    }

    pub fn open_replica(
        root: &Path,
        peers: Vec<u64>,
        internal_entries: bool,
        initial_offset: i64,
        cache: Option<bool>,
    ) -> Result<Self> {
        let options = Options {
            max_entry_bytes: MAX_BATCH,
            ..Options::default()
        };
        let mut log = match cache {
            Some(recover) => PartitionLog::open_page_cached(root, options, recover)?,
            None => PartitionLog::open(root, options)?,
        };
        if log.start().next_index == 1 {
            log.initialize_offset(initial_offset)?;
        }
        let election = load_election(root, log.end().next_index > 1)?;
        let last = log.term(log.end().next_index - 1)?;
        ensure!(election.0 >= last, "persistent Raft term is below log term");
        let prefix = load_snapshot(root, &log, internal_entries)?;
        let compacted_data_epoch = prefix.as_ref().and_then(|state| state.last_data_epoch);
        let state = if internal_entries {
            prefix
        } else {
            match DataCheckpoint::load(root, &log) {
                Ok(Some(checkpoint)) => Some(checkpoint),
                result => {
                    if let Err(error) = result {
                        eprintln!(
                            "rebuilding data checkpoint at {}: {error:#}",
                            root.display()
                        );
                    }
                    prefix
                }
            }
        };
        let checkpoint_next_index = state
            .as_ref()
            .map_or(log.start().next_index, |state| state.next_index);
        let mut recovered = Recovered::new(&log, &peers, state);
        recovered.replay(
            &log,
            checkpoint_next_index,
            log.committed().next_index,
            internal_entries,
        )?;
        if let Err(error) = prune_snapshots(root, log.start()) {
            eprintln!("clean snapshots at {}: {error}", root.display());
        }
        let membership = recovered.membership();
        Ok(Self {
            log,
            retained: recovered.retained,
            producers: recovered.producers,
            compacted_data_epoch,
            internal_entries,
            root: root.to_owned(),
            membership,
            election,
            checkpoint_next_index,
            bootstrap: peers,
        })
    }

    pub fn seed_log(path: &Path, offset: i64) -> Result<PartitionLog> {
        let mut log = PartitionLog::open(path, Options::default())?;
        ensure!(
            log.end().next_index == 1,
            "migration destination journal is not empty"
        );
        log.initialize_offset(offset)?;
        save_election(path, (1, 0))?;
        Ok(log)
    }

    pub fn persist(
        &mut self,
        election: Option<&HardState>,
        entries: &[Entry],
        committed: Option<u64>,
    ) -> Result<()> {
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
            } else if !self.internal_entries
                && entry.data.first() == Some(&RETAINED_START)
                && retention_position(&entry.data)?.next_index == entry.index
            {
                self.log
                    .append_internal_in_new_segment(entry.term, &entry.data)?;
            } else if self.internal_entries
                || entry.data.is_empty()
                || matches!(
                    entry.data.first(),
                    Some(
                        &RETAINED_START | &RETIRE_PRODUCERS | &PRODUCER_CLOCK | &IMPORTED_PRODUCERS
                    )
                )
            {
                self.log.append_internal(entry.term, 0, &entry.data)?;
            } else {
                let (timestamp, batch) = decode_batch(&entry.data)?;
                self.log.append_batch(entry.term, timestamp, batch)?;
            }
        }
        if let Some(index) = committed {
            self.log.sync_and_commit_through(index)?;
        } else if !entries.is_empty() {
            self.log.sync()?;
        }
        if entries
            .iter()
            .any(|entry| entry.get_entry_type() == EntryType::EntryConfChange)
        {
            self.log.flush_page_cache()?;
        }
        Ok(())
    }
    pub fn apply_data(&mut self, index: u64, data: &[u8]) -> Result<()> {
        apply_data_state(
            &self.log,
            &mut self.retained,
            &mut self.producers,
            index,
            data,
        )
    }

    pub fn compact_retention(&mut self) -> Result<()> {
        if self.internal_entries {
            return Ok(());
        }
        let old = self.log.start();
        let Some(base) = self.log.first_compactable_segment(self.retained.next_index) else {
            return Ok(());
        };
        let state = load_snapshot(&self.root, &self.log, false)?;
        let mut recovered = Recovered::new(&self.log, &self.bootstrap, state);
        recovered.replay(&self.log, old.next_index, base.next_index, false)?;
        let state = DataCheckpoint {
            next_index: base.next_index,
            term: self.log.term(base.next_index - 1)?,
            next_offset: base.next_offset,
            retained_index: base.next_index,
            retained_offset: base.next_offset,
            last_data_epoch: recovered.last_data_epoch,
            voters: recovered.voters.into_iter().collect(),
            learners: recovered.learners.into_iter().collect(),
            producers: recovered.producers,
        };
        replace(&self.root, &snapshot_name(base), &state.encode()?)?;
        self.log.compact_prefix(base)?;
        self.compacted_data_epoch = state.last_data_epoch;
        self.remove_previous_snapshot(old);
        Ok(())
    }

    fn remove_previous_snapshot(&self, old: Position) {
        if old.next_index > 1
            && old != self.log.start()
            && let Err(error) = fs::remove_file(self.root.join(snapshot_name(old)))
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!(
                "remove obsolete snapshot at {}: {error}",
                self.root.display()
            );
        }
    }

    pub fn compact_internal(&mut self, application: &impl serde::Serialize) -> Result<()> {
        ensure!(
            self.internal_entries,
            "internal compaction requires an internal journal"
        );
        let old = self.log.start();
        let base = self.log.committed();
        if base != self.log.end() || base.next_index <= old.next_index {
            return Ok(());
        }
        let app = checkpoint::encode_state(application)?;
        let mut checkpoint = DataCheckpoint {
            next_index: base.next_index,
            term: self.log.term(base.next_index - 1)?,
            next_offset: 0,
            retained_index: base.next_index,
            retained_offset: 0,
            last_data_epoch: None,
            voters: self.membership.voters.clone(),
            learners: self.membership.learners.clone(),
            producers: &self.producers,
        };
        checkpoint.voters.sort_unstable();
        checkpoint.learners.sort_unstable();
        let bytes = checkpoint::seal_snapshot(&checkpoint.encode()?, &app);
        replace(&self.root, &snapshot_name(base), &bytes)?;
        self.log.compact_prefix(base)?;
        self.retained = base;
        self.checkpoint_next_index = base.next_index;
        self.remove_previous_snapshot(old);
        Ok(())
    }

    pub fn internal_compaction_due(&self, interval: u64) -> bool {
        let committed = self.log.committed();
        self.internal_entries
            && committed == self.log.end()
            && committed.next_index - self.checkpoint_next_index >= interval
    }

    pub fn internal_state<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>> {
        if !self.internal_entries || self.log.start().next_index == 1 {
            return Ok(None);
        }
        let bytes = checkpoint::read(
            &self.root,
            &snapshot_name(self.log.start()),
            checkpoint::MAX_SNAPSHOT,
        )?
        .context("missing internal snapshot")?;
        let (_, application) = checkpoint::snapshot_parts(&bytes, true)?;
        Ok(Some(checkpoint::decode_state(application)?))
    }

    pub fn validate_snapshot(&self, snap: &Snapshot, local: u64) -> Result<DataCheckpoint> {
        let (metadata, _) = checkpoint::snapshot_parts(snap.get_data(), self.internal_entries)?;
        let state = DataCheckpoint::decode(metadata)?;
        let meta = snap.get_metadata();
        let members = meta.get_conf_state();
        let mut learners = state.learners.clone();
        if !state.voters.contains(&local) && !learners.contains(&local) {
            learners.push(local);
            learners.sort_unstable();
        }
        let commit = self.log.committed();
        let commit_term = self.log.term(commit.next_index - 1)?;
        ensure!(
            state.next_index > 1
                && state.next_index
                    == meta
                        .index
                        .checked_add(1)
                        .context("snapshot index overflow")?
                && state.term > 0
                && state.term == meta.term
                && state.position() == state.retained()
                && state.next_offset >= 0
                && (!self.internal_entries || state.next_offset == 0)
                && (state.next_index < commit.next_index
                    || state.next_index == commit.next_index
                        && state.position() == commit
                        && state.term == commit_term
                    || state.next_index > commit.next_index
                        && state.next_offset >= commit.next_offset
                        && state.term >= commit_term)
                && members.voters == state.voters
                && members.learners == learners
                && members.voters_outgoing.is_empty()
                && members.learners_next.is_empty()
                && !members.auto_leave,
            "invalid partition snapshot metadata"
        );
        Ok(state)
    }

    pub fn install_snapshot(&mut self, snap: &Snapshot, local: u64) -> Result<()> {
        let mut state = self.validate_snapshot(snap, local)?;
        let base = state.position();
        let old = self.log.start();
        let term = state.term;
        ensure!(
            base.next_index > self.log.committed().next_index && term <= self.election.0,
            "snapshot must extend the committed prefix in a persisted term"
        );
        let membership = snap.get_metadata().get_conf_state();
        state.learners.clone_from(&membership.learners);
        let mut bytes = state.encode()?;
        if self.internal_entries {
            let (_, application) = checkpoint::snapshot_parts(snap.get_data(), true)?;
            bytes = checkpoint::seal_snapshot(&bytes, application);
        }
        replace(&self.root, &snapshot_name(base), &bytes)?;
        self.log.install_snapshot_base(base, term)?;
        self.retained = base;
        self.producers = state.producers;
        self.compacted_data_epoch = state.last_data_epoch;
        self.membership.clone_from(membership);
        self.checkpoint_next_index = base.next_index;
        self.remove_previous_snapshot(old);
        Ok(())
    }

    pub fn checkpoint_if_due(&mut self) {
        let next = self.log.committed().next_index;
        if self.internal_entries || next - self.checkpoint_next_index < CHECKPOINT_INTERVAL {
            return;
        }
        match DataCheckpoint::save(
            &self.root,
            &self.log,
            self.retained,
            &self.membership,
            &self.producers,
        ) {
            Ok(next) => self.checkpoint_next_index = next,
            Err(error) => {
                // The committed journal remains the recovery authority.
                self.checkpoint_next_index = next;
                eprintln!("checkpoint {}: {error:#}", self.root.display());
            }
        }
    }
}

fn snapshot_name(base: Position) -> String {
    format!("snapshot-{:020}", base.next_index)
}

fn prune_snapshots(root: &Path, base: Position) -> io::Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(suffix) = name
            .to_str()
            .and_then(|name| name.strip_prefix("snapshot-"))
        else {
            continue;
        };
        let digits = suffix.strip_suffix(".tmp").unwrap_or(suffix);
        if digits.len() != 20 || !entry.file_type()?.is_file() {
            continue;
        }
        if let Ok(index) = digits.parse::<u64>()
            && (suffix != digits || index != base.next_index)
        {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        File::open(root)?.sync_all()?;
    }
    Ok(())
}

fn load_snapshot(
    root: &Path,
    log: &PartitionLog,
    internal: bool,
) -> Result<Option<DataCheckpoint>> {
    let start = log.start();
    if start.next_index == 1 {
        return Ok(None);
    }
    let bytes = checkpoint::read(root, &snapshot_name(start), checkpoint::MAX_SNAPSHOT)?
        .context("missing compacted partition snapshot")?;
    let (metadata, _) = checkpoint::snapshot_parts(&bytes, internal)?;
    let state = DataCheckpoint::decode(metadata)?;
    state.validate(log)?;
    ensure!(
        state.position() == start && state.retained() == start,
        "invalid compacted partition snapshot"
    );
    Ok(Some(state))
}

fn apply_data_state(
    log: &PartitionLog,
    retained: &mut Position,
    producers: &mut Producers,
    index: u64,
    data: &[u8],
) -> Result<()> {
    match data.first() {
        Some(&RETAINED_START) => {
            *retained = check_retention(log, *retained, index, data)?;
            Ok(())
        }
        Some(&RETIRE_PRODUCERS) => producers.retire(data),
        Some(&PRODUCER_CLOCK) => producers.start_legacy_grace(data),
        Some(&IMPORTED_PRODUCERS) => producers.import(data),
        _ => {
            let (timestamp, batch) = decode_batch(data)?;
            let (offset, _) = batch
                .split_first_chunk::<8>()
                .context("missing Kafka batch offset")?;
            producers.note_batch(batch, i64::from_be_bytes(*offset), timestamp)
        }
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

pub fn batch_epoch(batch: &[u8]) -> i32 {
    i32::from_be_bytes(batch[12..16].try_into().expect("Kafka batch epoch"))
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
    let start = retention_position(data)?;
    let physical = log.start();
    if physical.next_index > 1 && start.next_index <= physical.next_index {
        ensure!(
            start.next_offset <= physical.next_offset,
            "invalid superseded retention boundary"
        );
        return Ok(previous);
    }
    ensure!(
        start.next_index > previous.next_index
            && start.next_index <= index
            && start.next_offset > previous.next_offset
            && log.position_at(start.next_index)? == start,
        "invalid retained log boundary"
    );
    Ok(start)
}

fn retention_position(data: &[u8]) -> Result<Position> {
    ensure!(data.len() == 17, "invalid retained start record");
    Ok(Position {
        next_index: u64::from_be_bytes(data[1..9].try_into()?),
        next_offset: i64::from_be_bytes(data[9..].try_into()?),
    })
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

    fn snapshot(&self, index: u64, to: u64) -> raft::Result<Snapshot> {
        let base = self.log.start();
        if base.next_index == 1 || base.next_index - 1 < index {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        #[cfg(not(test))]
        {
            use std::time::{Duration, Instant};
            static NEXT: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);
            let now = Instant::now();
            let mut next = NEXT.lock().expect("snapshot transfer clock");
            if next.is_some_and(|deadline| deadline > now) {
                return Err(StorageError::SnapshotTemporarilyUnavailable.into());
            }
            *next = Some(now + Duration::from_millis(250));
        }
        let result = (|| -> Result<Snapshot> {
            let bytes =
                checkpoint::read(&self.root, &snapshot_name(base), checkpoint::MAX_SNAPSHOT)?
                    .context("missing Raft snapshot")?;
            let (metadata, _) = checkpoint::snapshot_parts(&bytes, self.internal_entries)?;
            let state = DataCheckpoint::decode(metadata)?;
            state.validate(&self.log)?;
            let mut membership = ConfState {
                voters: state.voters,
                learners: state.learners,
                ..ConfState::default()
            };
            if !membership.voters.contains(&to) && !membership.learners.contains(&to) {
                // A destination added after this prefix has no vote until it replays
                // its membership change from the suffix.
                membership.learners.push(to);
                membership.learners.sort_unstable();
            }
            let mut metadata = SnapshotMetadata {
                index: base.next_index - 1,
                term: state.term,
                ..SnapshotMetadata::default()
            };
            metadata.set_conf_state(membership);
            let mut snapshot = Snapshot::default();
            snapshot.set_metadata(metadata);
            snapshot.set_data(bytes.into());
            Ok(snapshot)
        })();
        result.map_err(|error| {
            eprintln!("read Raft snapshot at {}: {error:#}", self.root.display());
            StorageError::SnapshotTemporarilyUnavailable.into()
        })
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
    use crate::protocol::Error;
    use rusta_kafka::records::Batch;

    #[test]
    fn checkpoint_replays_only_committed_producer_updates_and_retirement() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("partition");
        let mut disk = Disk::open(&path, vec![1, 2, 3], false, 0)?;
        let mut batch = vec![0_u8; 61];
        batch[8..12].copy_from_slice(&49_i32.to_be_bytes());
        batch[16] = 2;
        batch[43..51].copy_from_slice(&257_i64.to_be_bytes());
        batch[57..61].copy_from_slice(&1_i32.to_be_bytes());
        let checksum = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&checksum.to_be_bytes());
        let parsed = Batch::parse(&batch)?;
        let entry = |index, bytes: Vec<u8>| Entry {
            index,
            term: 1,
            data: bytes.into(),
            ..Entry::default()
        };
        disk.persist(
            Some(&HardState {
                term: 1,
                ..HardState::default()
            }),
            &[entry(1, batch.clone())],
            Some(2),
        )?;
        disk.apply_data(1, &batch)?;
        let clock = disk.producers.maintenance_record(100, 50).unwrap();
        disk.persist(None, &[entry(2, clock.clone())], Some(3))?;
        disk.apply_data(2, &clock)?;
        DataCheckpoint::save(
            &path,
            &disk.log,
            disk.retained,
            &disk.membership,
            &disk.producers,
        )?;

        let retirement = disk.producers.maintenance_record(150, 50).unwrap();
        disk.persist(None, &[entry(3, retirement.clone())], None)?;
        drop(disk);

        let mut disk = Disk::open(&path, vec![1, 2, 3], false, 0)?;
        assert!(matches!(disk.producers.check(parsed), Ok(Some(0))));
        assert_eq!(
            disk.producers.maintenance_record(150, 50),
            Some(retirement.clone())
        );
        assert!(disk.producers.maintenance_record(149, 50).is_none());
        disk.log.commit_through(4)?;
        disk.apply_data(3, &retirement)?;
        drop(disk);

        let disk = Disk::open(&path, vec![1, 2, 3], false, 0)?;
        assert!(matches!(
            disk.producers.check(parsed),
            Err(Error::UnknownProducerId)
        ));
        let mut fresh = parsed;
        fresh.producer = 513;
        assert!(matches!(disk.producers.check(fresh), Ok(None)));
        assert!(disk.producers.maintenance_record(200, 50).is_none());
        Ok(())
    }

    #[test]
    fn recovery_ignores_durable_uncommitted_retention() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("partition");
        let mut disk = Disk::open(&path, vec![1, 2, 3], false, 0)?;
        let initial = disk.retained;
        let mut batch = vec![0_u8; 61];
        batch[8..12].copy_from_slice(&49_i32.to_be_bytes());
        batch[16] = 2;
        batch[43..57].fill(255);
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
            Some(2),
        )?;
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
            None,
        )?;
        assert_eq!(disk.log.durable().next_index, 3);
        drop(disk);

        let mut disk = Disk::open(&path, vec![1, 2, 3], false, 0)?;
        assert_eq!(disk.retained, initial);
        assert_eq!(disk.log.committed(), boundary);
        let entries = disk.entries(1, 3, None, GetEntriesContext::empty(false))?;
        assert_eq!(entries[0].data.as_ref(), data.as_slice());
        assert_eq!(entries[1].data.as_ref(), marker.as_slice());
        disk.log.commit_through(3)?;
        disk.apply_data(2, &marker)?;
        assert_eq!(disk.retained, boundary);
        drop(disk);

        let mut disk = Disk::open(&path, vec![1, 2, 3], false, 0)?;
        assert_eq!(disk.retained, boundary);
        assert_eq!(disk.log.start(), initial);
        disk.compact_retention()?;
        assert_eq!(disk.log.start(), boundary);
        assert!(!path.join("00000000000000000001.log").exists());
        let snapshot = disk.snapshot(0, 4)?;
        assert_eq!(snapshot.get_metadata().get_conf_state().learners, vec![4]);

        let replica_path = directory.path().join("replica");
        let mut replica = Disk::open(&replica_path, vec![1, 2, 3], false, 0)?;
        replica.persist(
            Some(&HardState {
                term: 1,
                ..HardState::default()
            }),
            &[],
            None,
        )?;
        let mut corrupt = snapshot.clone();
        let mut data = corrupt.get_data().to_vec();
        data[16] ^= 1;
        corrupt.set_data(data.into());
        assert!(replica.install_snapshot(&corrupt, 4).is_err());
        assert_eq!(replica.log.committed(), initial);

        let blocked = replica_path.join(format!("{}.tmp", snapshot_name(boundary)));
        fs::create_dir(&blocked)?;
        assert!(replica.install_snapshot(&snapshot, 4).is_err());
        drop(replica);
        let mut replica = Disk::open(&replica_path, vec![1, 2, 3], false, 0)?;
        assert_eq!(replica.log.committed(), initial);
        fs::remove_dir(blocked)?;
        replica.install_snapshot(&snapshot, 4)?;
        assert!(replica.install_snapshot(&snapshot, 4).is_err());
        let suffix = disk.entries(2, 3, None, GetEntriesContext::empty(false))?;
        replica.persist(None, &suffix, Some(3))?;
        replica.apply_data(2, &marker)?;
        drop(replica);
        let replica = Disk::open(&replica_path, vec![1, 2, 3], false, 0)?;
        assert_eq!(replica.log.start(), boundary);
        assert_eq!(replica.retained, boundary);
        assert_eq!(replica.membership.learners, vec![4]);
        Ok(())
    }
}
