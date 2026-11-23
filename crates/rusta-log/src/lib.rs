//! Segmented partition storage with explicit local, durable and committed positions.
#![forbid(unsafe_code)]

mod format;
mod segment;

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use format::{
    Checkpoint, HEADER_LEN, MAX_PAYLOAD, ProgressManifest, batch_fields, batch_max_timestamp,
    corrupt, invalid, write_entry,
};
pub use format::{Entry, Position};
use segment::{Cursor, MAX_SEGMENTS, Scan, Segment, list, path};

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Roll before an append would exceed this size. One large entry may occupy its own segment.
    pub segment_bytes: u64,
    /// Limit new data batches and internal entry payloads. Existing larger entries remain readable.
    pub max_entry_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            segment_bytes: 128 * 1024 * 1024,
            max_entry_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Retention {
    /// The earliest append time to keep in milliseconds since Unix epoch.
    pub keep_since_ms: Option<i64>,
    /// Target for retained on-disk log bytes. The active segment is kept unless it expires by time.
    pub max_bytes: Option<u64>,
}

// Keep a bounded suffix of the in-process Raft log for term lookups, replication,
// and consumers reading near the high watermark.
const RECENT_BYTES: usize = 256 * 1024;
const RECENT_ENTRIES: usize = 64;

#[derive(Default)]
struct RecentEntries {
    entries: VecDeque<(Entry, Vec<u8>)>,
    bytes: usize,
}

impl RecentEntries {
    fn push(&mut self, entry: Entry, payload: &[u8]) {
        if payload.len() > RECENT_BYTES {
            *self = Self::default();
            return;
        }
        while self.bytes + payload.len() > RECENT_BYTES || self.entries.len() == RECENT_ENTRIES {
            let (_, removed) = self.entries.pop_front().expect("recent entries have bytes");
            self.bytes -= removed.len();
        }
        let mut payload = payload.to_vec();
        if entry.record_count != 0 {
            payload[..8].copy_from_slice(&entry.position.next_offset.to_be_bytes());
        }
        self.bytes += payload.len();
        self.entries.push_back((entry, payload));
    }

    fn suffix(&self, index: u64) -> Option<impl Iterator<Item = &(Entry, Vec<u8>)>> {
        let first = self.entries.front()?.0.position.next_index;
        let skip = usize::try_from(index.checked_sub(first)?).ok()?;
        (skip < self.entries.len()).then(|| self.entries.iter().skip(skip))
    }
}

/// A single partition opened exclusively. Mutation methods require the caller to serialize work.
pub struct PartitionLog {
    root: PathBuf,
    options: Options,
    checkpoint: Checkpoint,
    segments: Vec<Segment>,
    writer: Option<File>,
    recent: RecentEntries,
    page_cache: bool,
    needs_flush: bool,
    stable_manifest: bool,
    progress_manifest: Option<ProgressManifest>,
    segment_directory_dirty: bool,
    sync_observer: Option<fn(&'static str, Duration)>,
    failed: bool,
    _lock: File,
}

impl PartitionLog {
    /// Open a partition. Its parent directory must already exist.
    pub fn open(root: impl AsRef<Path>, options: Options) -> io::Result<Self> {
        Self::open_inner(root.as_ref(), options, None, false)
    }

    /// Open a durable Raft data log, atomically upgrading a legacy manifest if present.
    pub fn open_raft_durable(root: impl AsRef<Path>, options: Options) -> io::Result<Self> {
        Self::open_inner(root.as_ref(), options, None, true)
    }

    /// Open a replica whose buffered progress is protected by a kernel boot fence.
    /// After kernel loss only the separately synchronized manifest is trusted.
    pub fn open_page_cached(
        root: impl AsRef<Path>,
        options: Options,
        recover_from_disk: bool,
    ) -> io::Result<Self> {
        Self::open_inner(root.as_ref(), options, Some(recover_from_disk), false)
    }

    fn open_inner(
        root: &Path,
        options: Options,
        cache: Option<bool>,
        durable_raft: bool,
    ) -> io::Result<Self> {
        if options.segment_bytes < HEADER_LEN
            || options.max_entry_bytes == 0
            || options.max_entry_bytes > MAX_PAYLOAD
        {
            return Err(invalid("invalid segment size or entry size limit"));
        }
        let root = root.canonicalize().or_else(|error| {
            if error.kind() != ErrorKind::NotFound {
                return Err(error);
            }
            let requested = root;
            match fs::create_dir(requested) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            requested.canonicalize()
        })?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        lock.try_lock().map_err(io::Error::from)?;
        if let Some(parent) = root.parent() {
            File::open(parent)?.sync_all()?;
        }

        let indexes = list(&root)?;
        let stable = cache
            .map(|_| Checkpoint::load_stable(&root))
            .transpose()
            .or_else(|error| {
                if error.kind() == ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(error)
                }
            })?;
        let progress = if durable_raft {
            ProgressManifest::open(&root)?
        } else {
            None
        };
        let checkpoint = match if let Some((_, checkpoint)) = &progress {
            Ok(*checkpoint)
        } else if let (Some(true), Some(stable)) = (cache, stable) {
            Ok(stable)
        } else {
            Checkpoint::load(&root)
        } {
            Ok(checkpoint)
                if cache == Some(true)
                    && stable.is_none()
                    && !(indexes.is_empty()
                        && checkpoint.durable == Position::initial()
                        && checkpoint.start == Position::initial()) =>
            {
                return Err(corrupt(
                    "page-cache replica has no synchronized recovery manifest",
                ));
            }
            Ok(checkpoint) => checkpoint,
            Err(error) if error.kind() == ErrorKind::NotFound && indexes.is_empty() => {
                let checkpoint = Checkpoint::initial();
                if !durable_raft {
                    checkpoint.save(&root)?;
                }
                checkpoint
            }
            Err(error) => return Err(error),
        };
        if cache == Some(true) {
            checkpoint.save(&root)?;
        }
        let segments = recover(&root, checkpoint, &indexes)?;
        let progress_manifest = match progress {
            Some((manifest, _)) => Some(manifest),
            None if durable_raft => Some(ProgressManifest::create(&root, checkpoint)?),
            None => None,
        };
        let mut log = Self {
            root,
            options,
            checkpoint,
            segments,
            writer: None,
            recent: RecentEntries::default(),
            page_cache: false,
            needs_flush: false,
            stable_manifest: cache.is_some(),
            progress_manifest,
            segment_directory_dirty: false,
            sync_observer: None,
            failed: false,
            _lock: lock,
        };
        if cache.is_some() && stable.is_none() {
            log.needs_flush = true;
            log.flush_page_cache()?;
        }
        Ok(log)
    }

    /// Observe successful data and progress-manifest syncs for diagnostics.
    pub fn observe_syncs(&mut self, observer: fn(&'static str, Duration)) {
        self.sync_observer = Some(observer);
    }

    pub fn start(&self) -> Position {
        self.checkpoint.start
    }

    /// The caller must fence any reopened volume whose kernel cache could
    /// have been lost. Normal checkpoint and retention operations stay durable.
    pub fn enable_page_cache(&mut self) {
        assert!(
            self.progress_manifest.is_none(),
            "durable Raft logs cannot use page-cache ACKs"
        );
        self.page_cache = true;
        // An earlier process on the same boot may have left data in cache.
        self.needs_flush = true;
    }

    pub fn needs_page_cache_flush(&self) -> bool {
        self.needs_flush
    }

    pub fn flush_page_cache(&mut self) -> io::Result<()> {
        self.ensure_available()?;
        if self.needs_flush {
            self.mutate(|log| log.save(log.checkpoint))?;
        }
        Ok(())
    }

    pub fn committed(&self) -> Position {
        self.checkpoint.committed
    }

    pub fn durable(&self) -> Position {
        self.checkpoint.durable
    }

    pub fn end(&self) -> Position {
        self.segments
            .last()
            .map_or(self.checkpoint.start, |segment| segment.end)
    }

    fn last_term(&self) -> u64 {
        self.segments
            .last()
            .map_or(self.checkpoint.start_term, |segment| segment.last_term)
    }

    /// Stage a Kafka v2 batch. The stored base offset is assigned by the log.
    /// The caller validates the records and sets the Kafka partition leader epoch before appending.
    pub fn append_batch(
        &mut self,
        term: u64,
        append_timestamp_ms: i64,
        batch: &[u8],
    ) -> io::Result<Entry> {
        if batch.len() > self.options.max_entry_bytes {
            return Err(invalid("batch exceeds the append payload limit"));
        }
        let (record_count, timestamp) = batch_fields(batch)?;
        if crc32c::crc32c(&batch[21..]) != u32::from_be_bytes(format::field(batch, 17)) {
            return Err(invalid("Kafka batch checksum mismatch"));
        }
        self.append(
            term,
            append_timestamp_ms,
            record_count,
            timestamp,
            batch,
            false,
        )
    }

    /// Stage an opaque consensus entry without advancing the Kafka record offset.
    pub fn append_internal(
        &mut self,
        term: u64,
        append_timestamp_ms: i64,
        payload: &[u8],
    ) -> io::Result<Entry> {
        self.append(term, append_timestamp_ms, 0, i64::MIN, payload, false)
    }

    pub fn append_internal_in_new_segment(
        &mut self,
        term: u64,
        payload: &[u8],
    ) -> io::Result<Entry> {
        self.append(term, 0, 0, i64::MIN, payload, true)
    }

    fn append(
        &mut self,
        term: u64,
        append_timestamp_ms: i64,
        record_count: u32,
        record_timestamp: i64,
        payload: &[u8],
        new_segment: bool,
    ) -> io::Result<Entry> {
        self.ensure_available()?;
        if term == 0 || term < self.last_term() || payload.len() > self.options.max_entry_bytes {
            return Err(invalid("invalid term or append payload size"));
        }
        let entry = Entry {
            position: self.end(),
            term,
            append_timestamp_ms,
            record_count,
            payload_len: payload.len() as u32,
        };
        let next = entry.position.after(record_count)?;
        let roll = new_segment
            || self.segments.last().is_none_or(|segment| {
                segment.bytes + HEADER_LEN + u64::from(entry.payload_len)
                    > self.options.segment_bytes
            });
        if roll && self.segments.len() == MAX_SEGMENTS {
            return Err(io::Error::other("partition segment limit reached"));
        }
        self.mutate(|log| {
            if roll {
                let sync_previous = log.progress_manifest.is_none() || log.end() != log.durable();
                if let Some(previous) = log.writer.take()
                    && sync_previous
                {
                    previous.sync_data()?;
                }
                log.writer = Some(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path(&log.root, entry.position.next_index))?,
                );
                log.segment_directory_dirty = log.progress_manifest.is_some();
                log.segments.push(Segment::new(entry.position));
            } else if log.writer.is_none() {
                let last = log.segments.last().expect("existing segment");
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(path(&log.root, last.start.next_index))?;
                file.seek(SeekFrom::Start(last.bytes))?;
                log.writer = Some(file);
            }
            write_entry(
                log.writer.as_mut().expect("active segment is open"),
                entry,
                payload,
            )?;
            log.segments
                .last_mut()
                .expect("active segment exists")
                .push(entry, next, record_timestamp);
            if log.progress_manifest.is_some() {
                log.recent.push(entry, payload);
            }
            Ok(entry)
        })
    }

    /// Make staged writes recoverable without making them visible to readers.
    pub fn sync(&mut self) -> io::Result<Position> {
        self.sync_boundary(None)
    }

    /// Sync and publish all local writes in one checkpoint. For single-voter use.
    pub fn commit(&mut self) -> io::Result<Position> {
        self.sync_and_commit_through(self.end().next_index)
    }

    /// Sync staged writes and publish the prefix already committed by consensus.
    pub fn sync_and_commit_through(&mut self, next_index: u64) -> io::Result<Position> {
        if !(self.committed().next_index..=self.end().next_index).contains(&next_index) {
            return Err(invalid("commit must extend the staged prefix"));
        }
        self.sync_boundary(Some(next_index))?;
        Ok(self.committed())
    }

    fn sync_boundary(&mut self, committed: Option<u64>) -> io::Result<Position> {
        self.ensure_available()?;
        let end = self.end();
        let committed = committed
            .filter(|&index| index != self.committed().next_index)
            .map(|index| self.position_at(index))
            .transpose()?
            .unwrap_or(self.committed());
        if self.durable() == end && self.committed() == committed {
            return Ok(end);
        }
        self.mutate(|log| {
            if log.durable() != end && !log.page_cache {
                let started = log.sync_observer.map(|observer| (observer, Instant::now()));
                log.writer
                    .as_ref()
                    .expect("staged writes have an open writer")
                    .sync_data()?;
                if let Some((observe, started)) = started {
                    observe("segment_sync", started.elapsed());
                }
            }
            log.save_progress(Checkpoint {
                durable: end,
                committed,
                ..log.checkpoint
            })?;
            Ok(end)
        })
    }

    /// Publish a prefix already synced locally after consensus has committed it.
    /// The boundary is the next index, exclusive of the prefix.
    pub fn commit_through(&mut self, next_index: u64) -> io::Result<Position> {
        self.ensure_available()?;
        if !(self.committed().next_index..=self.durable().next_index).contains(&next_index) {
            return Err(invalid("commit must extend the durable prefix"));
        }
        if next_index == self.committed().next_index {
            return Ok(self.committed());
        }
        let committed = self.position_at(next_index)?;
        self.mutate(|log| {
            log.save_progress(Checkpoint {
                committed,
                ..log.checkpoint
            })?;
            Ok(committed)
        })
    }

    pub fn has_durable_raft_manifest(&self) -> bool {
        self.progress_manifest.is_some()
    }

    /// Apply a data-only quorum commit to the live log. The appended entries
    /// have already been synced. Recovery can rediscover their commit through
    /// Raft if the next manifest write does not survive.
    pub fn publish_raft_commit(&mut self, next_index: u64) -> io::Result<()> {
        self.ensure_available()?;
        if !self.has_durable_raft_manifest()
            || !(self.committed().next_index..=self.durable().next_index).contains(&next_index)
        {
            return Err(invalid("Raft commit must extend the durable prefix"));
        }
        if next_index != self.committed().next_index {
            self.checkpoint.committed = self.position_at(next_index)?;
        }
        Ok(())
    }

    /// Synchronize live Raft progress before saving an application checkpoint.
    pub fn sync_raft_progress(&mut self) -> io::Result<()> {
        self.ensure_available()?;
        if self.has_durable_raft_manifest() {
            self.mutate(|log| log.save_progress(log.checkpoint))?;
        }
        Ok(())
    }

    /// Set a copied partition's offset base before its first consensus entry.
    pub fn initialize_offset(&mut self, offset: i64) -> io::Result<()> {
        self.ensure_available()?;
        let base = self.start();
        if offset < 0 || base.next_index != 1 {
            return Err(invalid("invalid initial Kafka offset"));
        }
        if base.next_offset == offset {
            return Ok(());
        }
        if base != Position::initial() || base != self.end() {
            return Err(invalid("initial offset is already in use"));
        }
        let base = Position {
            next_index: 1,
            next_offset: offset,
        };
        self.mutate(|log| {
            log.save(Checkpoint {
                start: base,
                committed: base,
                durable: base,
                start_term: 0,
            })
        })
    }

    /// Replace the log with a snapshot boundary after the caller has durably
    /// saved its state machine. The caller supplies the next index and offset.
    /// At the existing committed index this can discard only an identical prefix.
    pub fn install_snapshot_base(&mut self, base: Position, term: u64) -> io::Result<()> {
        self.ensure_available()?;
        let committed = self.committed();
        let committed_term = self.term(committed.next_index - 1)?;
        if base.next_index <= 1
            || base.next_index < committed.next_index
            || base.next_offset < committed.next_offset
            || term == 0
            || term < committed_term
            || (base.next_index == committed.next_index
                && (base != committed || term != committed_term))
        {
            return Err(invalid("snapshot would replace a committed log boundary"));
        }
        self.mutate(|log| {
            log.writer = None;
            // An empty manifest makes every old segment unreachable. Recovery
            // will remove them if a crash occurs after publication.
            let checkpoint = Checkpoint {
                start: base,
                start_term: term,
                durable: base,
                committed: base,
            };
            log.save(checkpoint)?;
            log.segments = recover(&log.root, checkpoint, &list(&log.root)?)?;
            log.recent = RecentEntries::default();
            Ok(())
        })
    }

    /// Remove a conflicting uncommitted suffix, leaving entries below the supplied index.
    pub fn truncate_suffix(&mut self, next_index: u64) -> io::Result<()> {
        self.ensure_available()?;
        if !(self.committed().next_index..=self.end().next_index).contains(&next_index) {
            return Err(invalid(
                "truncation would remove committed data or exceed the log",
            ));
        }
        if next_index == self.end().next_index {
            return Ok(());
        }
        let retained = self.position_at(next_index)?;
        self.mutate(|log| {
            log.writer = None;
            if retained.next_index < log.durable().next_index {
                log.save(Checkpoint {
                    durable: retained,
                    ..log.checkpoint
                })?;
            }
            let boundary = Checkpoint {
                durable: retained,
                ..log.checkpoint
            };
            log.segments = recover(&log.root, boundary, &list(&log.root)?)?;
            log.recent = RecentEntries::default();
            if log.durable().next_index < retained.next_index {
                // Recovery synced the truncation, but the preserved staged prefix still needs sync.
                let active = log.segments.last().expect("preserved staged entries");
                let mut writer = OpenOptions::new()
                    .write(true)
                    .open(path(&log.root, active.start.next_index))?;
                writer.seek(SeekFrom::Start(active.bytes))?;
                log.writer = Some(writer);
            }
            Ok(())
        })
    }

    /// Size of the requested committed suffix when it is present in the recent cache.
    pub fn recent_batch_bytes(&self, offset: i64) -> Option<usize> {
        let committed = self.committed();
        let first = self.recent.entries.front()?.0.position;
        if offset < first.next_offset || offset >= committed.next_offset {
            return None;
        }
        Some(
            self.recent
                .entries
                .iter()
                .take_while(|(entry, _)| entry.position.next_index < committed.next_index)
                .filter(|(entry, _)| {
                    entry.record_count > 0
                        && entry.position.next_offset + i64::from(entry.record_count) > offset
                })
                .map(|(_, payload)| payload.len())
                .sum(),
        )
    }

    /// Read the committed batch containing an offset. Pass a reusable payload buffer.
    /// Return None at the high watermark and InvalidInput outside the readable range.
    pub fn batch_at(&self, offset: i64, payload: &mut Vec<u8>) -> io::Result<Option<Entry>> {
        let mut found = None;
        self.visit_batches(offset, payload, |entry, _| {
            found = Some(entry);
            Ok(false)
        })?;
        Ok(found)
    }

    /// Stream committed batches beginning with the batch containing the offset.
    /// Return false from the visitor to stop reading. Payload memory is reused.
    pub fn visit_batches(
        &self,
        offset: i64,
        payload: &mut Vec<u8>,
        mut visit: impl FnMut(Entry, &[u8]) -> io::Result<bool>,
    ) -> io::Result<()> {
        self.ensure_available()?;
        if !(self.start().next_offset..=self.committed().next_offset).contains(&offset) {
            return Err(invalid("offset outside the committed log"));
        }
        if offset == self.committed().next_offset {
            payload.clear();
            return Ok(());
        }

        let committed = self.committed().next_index;
        if self
            .recent
            .entries
            .front()
            .is_some_and(|(entry, _)| entry.position.next_offset <= offset)
        {
            for (entry, cached) in &self.recent.entries {
                if entry.position.next_index >= committed {
                    break;
                }
                if entry.record_count == 0
                    || entry.position.next_offset + i64::from(entry.record_count) <= offset
                {
                    continue;
                }
                payload.clear();
                payload.extend_from_slice(cached);
                if !visit(*entry, payload)? {
                    break;
                }
            }
            return Ok(());
        }

        let first = self
            .segments
            .partition_point(|segment| segment.end.next_offset <= offset);
        let mut found = false;
        for segment in &self.segments[first..] {
            let mut scan =
                segment.scan(&self.root, u64::MAX, offset.max(segment.start.next_offset))?;
            while let Some(entry) = scan.next(payload)? {
                if entry.position.next_index >= self.committed().next_index {
                    return Ok(());
                }
                if entry.record_count == 0
                    || entry.position.after(entry.record_count)?.next_offset <= offset
                {
                    continue;
                }
                found = true;
                if !visit(entry, payload)? {
                    return Ok(());
                }
            }
        }
        if !found {
            return Err(corrupt("committed batch is missing"));
        }
        Ok(())
    }

    /// Find the earliest matching committed record in offset order.
    /// The decoder returns the first offset in the candidate batch with timestamp >= target.
    pub fn offset_for_timestamp(
        &self,
        target: i64,
        payload: &mut Vec<u8>,
        decode: impl FnMut(Entry, &[u8], i64) -> io::Result<Option<i64>>,
    ) -> io::Result<Option<i64>> {
        self.offset_for_timestamp_from(self.start().next_offset, target, payload, decode)
    }

    /// Find a timestamp within a Kafka-visible suffix of the committed log.
    pub fn offset_for_timestamp_from(
        &self,
        start: i64,
        target: i64,
        payload: &mut Vec<u8>,
        mut decode: impl FnMut(Entry, &[u8], i64) -> io::Result<Option<i64>>,
    ) -> io::Result<Option<i64>> {
        self.ensure_available()?;
        if !(self.start().next_offset..=self.committed().next_offset).contains(&start) {
            return Err(invalid("timestamp search starts outside the committed log"));
        }
        let first = self
            .segments
            .partition_point(|segment| segment.end.next_offset <= start);
        for segment in &self.segments[first..] {
            if segment.start.next_index >= self.committed().next_index {
                break;
            }
            if segment.max_record_timestamp < target {
                continue;
            }
            let mut scan =
                segment.scan(&self.root, u64::MAX, start.max(segment.start.next_offset))?;
            while let Some(entry) = scan.next(payload)? {
                if entry.position.next_index >= self.committed().next_index {
                    break;
                }
                if entry.record_count == 0
                    || entry.position.next_offset < start
                    || batch_max_timestamp(payload) < target
                {
                    continue;
                }
                if let Some(offset) = decode(entry, payload, target)? {
                    if (entry.position.next_offset
                        ..entry.position.after(entry.record_count)?.next_offset)
                        .contains(&offset)
                    {
                        return Ok(Some(offset));
                    }
                    return Err(invalid(
                        "timestamp decoder returned an offset outside its batch",
                    ));
                }
            }
        }
        Ok(None)
    }

    /// Check whether applying the durable uncommitted suffix needs only cached user data.
    pub fn has_cached_data_suffix(&self, max_bytes: usize) -> bool {
        let from = self.committed().next_index;
        if self.end() != self.durable() {
            return false;
        }
        if from == self.durable().next_index {
            return true;
        }
        let Some(entries) = self.recent.suffix(from) else {
            return false;
        };
        let mut bytes = 0;
        for (entry, payload) in entries {
            bytes += payload.len();
            if bytes > max_bytes || entry.record_count == 0 || entry.append_timestamp_ms == 0 {
                return false;
            }
        }
        true
    }

    pub fn has_cached_term(&self, index: u64) -> bool {
        index == self.start().next_index - 1
            || (index >= self.start().next_index && self.recent.suffix(index).is_some())
    }

    /// Read the Raft term at an index, including the boundary immediately before retained entries.
    pub fn term(&self, index: u64) -> io::Result<u64> {
        self.ensure_available()?;
        if index == self.start().next_index - 1 {
            return Ok(self.checkpoint.start_term);
        }
        let mut term = None;
        let end = index
            .checked_add(1)
            .ok_or_else(|| invalid("index overflow"))?;
        self.visit_entries(index, end, u64::MAX, |entry, _| {
            term = Some(entry.term);
            Ok(())
        })?;
        term.ok_or_else(|| corrupt("requested entry is missing"))
    }

    /// Stream local entries in the half-open Raft-index range into a consumer.
    /// The byte budget includes envelope bytes. The first entry may exceed it to allow progress.
    /// Returns the next index to request. Payload memory is reused between visits.
    pub fn visit_entries(
        &self,
        first: u64,
        end: u64,
        byte_budget: u64,
        mut visit: impl FnMut(Entry, &[u8]) -> io::Result<()>,
    ) -> io::Result<u64> {
        self.ensure_available()?;
        if first < self.start().next_index || end > self.end().next_index || first > end {
            return Err(invalid("requested indexes are outside the retained log"));
        }
        if first == end {
            return Ok(end);
        }

        let mut bytes = 0_u64;
        let mut accept = |entry: Entry, payload: &[u8]| -> io::Result<Option<u64>> {
            let index = entry.position.next_index;
            let total = bytes.saturating_add(HEADER_LEN + u64::from(entry.payload_len));
            if bytes != 0 && total > byte_budget {
                return Ok(Some(index));
            }
            visit(entry, payload)?;
            bytes = total;
            Ok((index + 1 == end || bytes >= byte_budget).then_some(index + 1))
        };
        if let Some(entries) = self.recent.suffix(first) {
            for (entry, payload) in entries {
                if let Some(next) = accept(*entry, payload)? {
                    return Ok(next);
                }
            }
            return Err(corrupt("requested entries are missing"));
        }
        let mut payload = Vec::new();
        let start = self
            .segments
            .partition_point(|segment| segment.end.next_index <= first);
        for segment in &self.segments[start..] {
            let mut scan =
                segment.scan(&self.root, first.max(segment.start.next_index), i64::MAX)?;
            while let Some(entry) = scan.next(&mut payload)? {
                let index = entry.position.next_index;
                if index < first {
                    continue;
                }
                if let Some(next) = accept(entry, &payload)? {
                    return Ok(next);
                }
            }
        }
        Err(corrupt("requested entries are missing"))
    }

    /// Select a segment boundary without changing the physical log.
    /// Raft callers must commit the selected position before using it for reads.
    pub fn retention_boundary(&self, start: Position, policy: Retention) -> io::Result<Position> {
        self.retention_boundary_with_legacy_time(start, policy, None)
    }

    pub fn retention_boundary_with_legacy_time(
        &self,
        start: Position,
        policy: Retention,
        legacy_append_ms: Option<i64>,
    ) -> io::Result<Position> {
        self.ensure_available()?;
        if self.end() != self.committed()
            || !(self.start().next_index..=self.committed().next_index).contains(&start.next_index)
        {
            return Err(invalid("retention requires a committed suffix"));
        }
        let first = self
            .segments
            .partition_point(|segment| segment.end.next_index <= start.next_index);
        let segments = &self.segments[first..];
        let mut prefix_bytes = 0;
        if let Some(segment) = segments.first()
            && segment.start.next_index < start.next_index
        {
            let mut scan = segment.scan(&self.root, start.next_index, i64::MAX)?;
            prefix_bytes = scan.byte_position();
            let mut payload = Vec::new();
            while let Some(entry) = scan.next(&mut payload)? {
                if entry.position.next_index == start.next_index {
                    break;
                }
                prefix_bytes = scan.byte_position();
            }
        }
        let mut bytes = segments.iter().map(|segment| segment.bytes).sum::<u64>() - prefix_bytes;
        let mut boundary = start;
        for (index, segment) in segments.iter().enumerate() {
            let append_ms = if segment.has_legacy_records {
                segment
                    .max_append_timestamp
                    .max(legacy_append_ms.unwrap_or(0))
            } else {
                segment.max_append_timestamp
            };
            let expired = policy
                .keep_since_ms
                .is_some_and(|cutoff| append_ms < cutoff);
            let oversized =
                index + 1 < segments.len() && policy.max_bytes.is_some_and(|limit| bytes > limit);
            if !expired && !oversized {
                break;
            }
            boundary = segment.end;
            bytes -= segment.bytes - if index == 0 { prefix_bytes } else { 0 };
        }
        Ok(boundary)
    }

    /// Apply singleton retention to a committed log and return the number of removed segments.
    pub fn apply_retention(&mut self, policy: Retention) -> io::Result<usize> {
        self.apply_retention_after(policy, || Ok(()))
    }

    /// Persist external state before a retention pass removes the source entries.
    pub fn apply_retention_after(
        &mut self,
        policy: Retention,
        before_removal: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<usize> {
        let boundary = self.retention_boundary(self.start(), policy)?;
        if boundary == self.start() {
            return Ok(0);
        }
        before_removal()?;
        self.compact_prefix(boundary)
    }

    /// Return the last whole segment that can be removed below the consensus index.
    pub fn compactable_prefix(&self, next_index: u64) -> Position {
        let next_index = next_index.min(self.committed().next_index);
        let count = self
            .segments
            .partition_point(|segment| segment.end.next_index <= next_index);
        count
            .checked_sub(1)
            .map_or(self.start(), |index| self.segments[index].end)
    }

    pub fn first_compactable_segment(&self, next_index: u64) -> Option<Position> {
        let limit = next_index.min(self.committed().next_index);
        self.segments
            .first()
            .and_then(|segment| (segment.end.next_index <= limit).then_some(segment.end))
    }

    /// Reclaim fully committed segments after the caller publishes its snapshot.
    pub fn compact_prefix(&mut self, boundary: Position) -> io::Result<usize> {
        self.ensure_available()?;
        if self.compactable_prefix(boundary.next_index) != boundary {
            return Err(invalid("compaction requires a committed segment boundary"));
        }
        let removed = self
            .segments
            .partition_point(|segment| segment.end.next_index <= boundary.next_index);
        if removed == 0 {
            return Ok(0);
        }
        self.mutate(|log| {
            let last = &log.segments[removed - 1];
            log.save(Checkpoint {
                start: last.end,
                start_term: last.last_term,
                ..log.checkpoint
            })?;
            if removed == log.segments.len() {
                log.writer = None;
            }
            for segment in log.segments.drain(..removed) {
                fs::remove_file(path(&log.root, segment.start.next_index))?;
            }
            File::open(&log.root)?.sync_all()?;
            Ok(removed)
        })
    }

    /// Resolve an exact consensus index to its Kafka position.
    pub fn position_at(&self, index: u64) -> io::Result<Position> {
        if index == self.end().next_index {
            return Ok(self.end());
        }
        let mut position = None;
        self.visit_entries(index, index + 1, u64::MAX, |entry, _| {
            position = Some(entry.position);
            Ok(())
        })?;
        position.ok_or_else(|| corrupt("requested boundary is missing"))
    }

    fn save(&mut self, checkpoint: Checkpoint) -> io::Result<()> {
        if self.needs_flush {
            if let Some(segment) = self.segments.last() {
                if let Some(writer) = &self.writer {
                    writer.sync_data()?;
                } else {
                    File::open(path(&self.root, segment.start.next_index))?.sync_all()?;
                }
            }
            self.needs_flush = false;
        }
        if let Some(manifest) = &mut self.progress_manifest {
            if self.segment_directory_dirty {
                // Publish the segment name before publishing a checkpoint that depends on it.
                File::open(&self.root)?.sync_all()?;
                self.segment_directory_dirty = false;
            }
            manifest.save(checkpoint)?;
        } else {
            checkpoint.save(&self.root)?;
        }
        if self.stable_manifest {
            checkpoint.save_stable(&self.root)?;
        }
        self.checkpoint = checkpoint;
        Ok(())
    }

    fn save_progress(&mut self, checkpoint: Checkpoint) -> io::Result<()> {
        if self.page_cache {
            checkpoint.save_buffered(&self.root)?;
            self.checkpoint = checkpoint;
            self.needs_flush = true;
            Ok(())
        } else {
            let started = self
                .sync_observer
                .map(|observer| (observer, Instant::now()));
            let stage = match (
                checkpoint.durable != self.checkpoint.durable,
                checkpoint.committed != self.checkpoint.committed,
            ) {
                (true, true) => "append_and_commit_manifest",
                (true, false) => "append_manifest",
                (false, _) => "commit_manifest",
            };
            self.save(checkpoint)?;
            if let Some((observe, started)) = started {
                observe(stage, started.elapsed());
            }
            Ok(())
        }
    }

    fn ensure_available(&self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "partition storage must be reopened after a mutation error",
            ));
        }
        Ok(())
    }

    fn mutate<T>(&mut self, operation: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<T> {
        let result = operation(self);
        self.failed = result.is_err();
        result
    }
}

fn recover(root: &Path, checkpoint: Checkpoint, indexes: &[u64]) -> io::Result<Vec<Segment>> {
    let mut position = checkpoint.start;
    let mut term = checkpoint.start_term;
    let mut segments = Vec::new();
    let mut payload = Vec::new();
    let mut cut = None;

    for &index in indexes.iter().filter(|&&index| {
        (checkpoint.start.next_index..checkpoint.durable.next_index).contains(&index)
    }) {
        if index != position.next_index {
            return Err(corrupt("gap or overlap in retained segments"));
        }
        let filename = path(root, index);
        let file_len = fs::metadata(&filename)?.len();
        let mut scan = Scan::new(&filename, Cursor { position, byte: 0 }, file_len)?;
        let mut segment = Segment::new(position);
        while position.next_index < checkpoint.durable.next_index {
            if position.next_index == checkpoint.committed.next_index
                && position != checkpoint.committed
            {
                return Err(corrupt("committed offset disagrees with the log"));
            }
            let Some(entry) = scan.next(&mut payload)? else {
                break;
            };
            if entry.term < term {
                return Err(corrupt("non-monotonic terms in the log"));
            }
            term = entry.term;
            position = entry
                .position
                .after(entry.record_count)
                .map_err(|_| corrupt("entry position overflow"))?;
            let timestamp = if entry.record_count == 0 {
                i64::MIN
            } else {
                batch_max_timestamp(&payload)
            };
            segment.push(entry, position, timestamp);
        }
        if segment.bytes == 0 {
            return Err(corrupt("empty durable segment"));
        }
        if segment.bytes < file_len {
            cut = Some((filename, segment.bytes));
        }
        segments.push(segment);
    }
    if position != checkpoint.durable
        || (position.next_index == checkpoint.committed.next_index
            && position != checkpoint.committed)
    {
        return Err(corrupt("durable positions disagree with the retained log"));
    }

    if let Some((filename, len)) = cut {
        let file = OpenOptions::new().write(true).open(filename)?;
        file.set_len(len)?;
        file.sync_data()?;
    }
    let mut deleted = false;
    for &index in indexes {
        if index < checkpoint.start.next_index || index >= checkpoint.durable.next_index {
            fs::remove_file(path(root, index))?;
            deleted = true;
        }
    }
    if deleted {
        File::open(root)?.sync_all()?;
    }
    Ok(segments)
}
