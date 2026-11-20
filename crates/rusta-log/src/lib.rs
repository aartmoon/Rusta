//! Segmented partition storage with explicit local, durable and committed positions.
#![forbid(unsafe_code)]

mod format;
mod segment;

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use format::{
    Checkpoint, HEADER_LEN, MAX_PAYLOAD, batch_fields, batch_max_timestamp, corrupt, invalid,
    write_entry,
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

/// A single partition opened exclusively. Mutation methods require the caller to serialize work.
pub struct PartitionLog {
    root: PathBuf,
    options: Options,
    checkpoint: Checkpoint,
    segments: Vec<Segment>,
    writer: Option<File>,
    failed: bool,
    _lock: File,
}

impl PartitionLog {
    /// Open a partition. Its parent directory must already exist.
    pub fn open(root: impl AsRef<Path>, options: Options) -> io::Result<Self> {
        if options.segment_bytes < HEADER_LEN
            || options.max_entry_bytes == 0
            || options.max_entry_bytes > MAX_PAYLOAD
        {
            return Err(invalid("invalid segment size or entry size limit"));
        }
        let root = root.as_ref().canonicalize().or_else(|error| {
            if error.kind() != ErrorKind::NotFound {
                return Err(error);
            }
            let requested = root.as_ref();
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
        let checkpoint = match Checkpoint::load(&root) {
            Ok(checkpoint) => checkpoint,
            Err(error) if error.kind() == ErrorKind::NotFound && indexes.is_empty() => {
                let checkpoint = Checkpoint::initial();
                checkpoint.save(&root)?;
                checkpoint
            }
            Err(error) => return Err(error),
        };
        let segments = recover(&root, checkpoint, &indexes)?;
        Ok(Self {
            root,
            options,
            checkpoint,
            segments,
            writer: None,
            failed: false,
            _lock: lock,
        })
    }

    pub fn start(&self) -> Position {
        self.checkpoint.start
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
        self.append(term, append_timestamp_ms, record_count, timestamp, batch)
    }

    /// Stage an opaque consensus entry without advancing the Kafka record offset.
    pub fn append_internal(
        &mut self,
        term: u64,
        append_timestamp_ms: i64,
        payload: &[u8],
    ) -> io::Result<Entry> {
        self.append(term, append_timestamp_ms, 0, i64::MIN, payload)
    }

    fn append(
        &mut self,
        term: u64,
        append_timestamp_ms: i64,
        record_count: u32,
        record_timestamp: i64,
        payload: &[u8],
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
        let roll = self.segments.last().is_none_or(|segment| {
            segment.bytes + HEADER_LEN + u64::from(entry.payload_len) > self.options.segment_bytes
        });
        if roll && self.segments.len() == MAX_SEGMENTS {
            return Err(io::Error::other("partition segment limit reached"));
        }
        self.mutate(|log| {
            if roll {
                if let Some(previous) = log.writer.take() {
                    previous.sync_data()?;
                }
                log.writer = Some(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path(&log.root, entry.position.next_index))?,
                );
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
            Ok(entry)
        })
    }

    /// Make staged writes recoverable without making them visible to readers.
    pub fn sync(&mut self) -> io::Result<Position> {
        self.sync_boundary(false)
    }

    /// Sync and publish all local writes in one checkpoint. For single-voter use.
    pub fn commit(&mut self) -> io::Result<Position> {
        self.sync_boundary(true)
    }

    fn sync_boundary(&mut self, commit: bool) -> io::Result<Position> {
        self.ensure_available()?;
        let end = self.end();
        if self.durable() == end && (!commit || self.committed() == end) {
            return Ok(end);
        }
        self.mutate(|log| {
            if log.durable() != end {
                log.writer
                    .as_ref()
                    .expect("staged writes have an open writer")
                    .sync_data()?;
            }
            let mut checkpoint = log.checkpoint;
            checkpoint.durable = end;
            if commit {
                checkpoint.committed = end;
            }
            log.save(checkpoint)?;
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
            log.save(Checkpoint {
                committed,
                ..log.checkpoint
            })?;
            Ok(committed)
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
                let total = bytes.saturating_add(HEADER_LEN + u64::from(entry.payload_len));
                if bytes != 0 && total > byte_budget {
                    return Ok(index);
                }
                visit(entry, &payload)?;
                bytes = total;
                if index + 1 == end || bytes >= byte_budget {
                    return Ok(index + 1);
                }
            }
        }
        Err(corrupt("requested entries are missing"))
    }

    /// Select a segment boundary without changing the physical log.
    /// Raft callers must commit the selected position before using it for reads.
    pub fn retention_boundary(&self, start: Position, policy: Retention) -> io::Result<Position> {
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
            let expired = policy
                .keep_since_ms
                .is_some_and(|cutoff| segment.max_append_timestamp < cutoff);
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
        let removed = self
            .segments
            .partition_point(|segment| segment.end.next_index <= boundary.next_index);
        if removed == 0 {
            return Ok(0);
        }
        before_removal()?;
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
        checkpoint.save(&self.root)?;
        self.checkpoint = checkpoint;
        Ok(())
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
