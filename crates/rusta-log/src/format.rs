use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, IoSlice, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crc32c::{crc32c, crc32c_append};

const MANIFEST: &str = "manifest";
const MAGIC: &[u8; 8] = b"RUSTAL01";
const CHECKPOINT_LEN: usize = 68;
// Keep the two records on separate filesystem pages, including on 64 KiB Linux kernels.
const SLOT_STRIDE: u64 = 64 * 1024;
const SLOT_LEN: usize = 8 + CHECKPOINT_LEN + 4;
pub(crate) const HEADER_LEN: u64 = 36;
pub(crate) const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub next_index: u64,
    pub next_offset: i64,
}

impl Position {
    #[must_use]
    pub(crate) fn initial() -> Self {
        Self {
            next_index: 1,
            next_offset: 0,
        }
    }

    pub(crate) fn after(self, records: u32) -> io::Result<Self> {
        Ok(Self {
            next_index: self
                .next_index
                .checked_add(1)
                .ok_or_else(|| invalid("index overflow"))?,
            next_offset: self
                .next_offset
                .checked_add(i64::from(records))
                .ok_or_else(|| invalid("offset overflow"))?,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Checkpoint {
    pub start: Position,
    pub start_term: u64,
    pub durable: Position,
    pub committed: Position,
}

impl Checkpoint {
    pub fn initial() -> Self {
        Self {
            start: Position::initial(),
            start_term: 0,
            durable: Position::initial(),
            committed: Position::initial(),
        }
    }

    pub fn load(root: &Path) -> io::Result<Self> {
        Self::load_named(root, MANIFEST)
    }

    pub fn load_stable(root: &Path) -> io::Result<Self> {
        Self::load_named(root, "manifest.stable")
    }

    fn load_named(root: &Path, name: &str) -> io::Result<Self> {
        let mut file = File::open(root.join(name))?;
        let mut bytes = [0; CHECKPOINT_LEN];
        if file.metadata()?.len() != CHECKPOINT_LEN as u64 {
            return Err(corrupt("invalid manifest size"));
        }
        file.read_exact(&mut bytes)?;
        Self::decode(&bytes)
    }

    fn decode(bytes: &[u8; CHECKPOINT_LEN]) -> io::Result<Self> {
        if &bytes[..8] != MAGIC || crc32c(&bytes[..64]) != u32::from_be_bytes(field(bytes, 64)) {
            return Err(corrupt("invalid manifest version or checksum"));
        }
        let position_at = |offset| Position {
            next_index: u64::from_be_bytes(field(bytes, offset)),
            next_offset: i64::from_be_bytes(field(bytes, offset + 8)),
        };
        let checkpoint = Self {
            start: position_at(8),
            durable: position_at(24),
            committed: position_at(40),
            start_term: u64::from_be_bytes(field(bytes, 56)),
        };
        if checkpoint.start.next_index == 0
            || checkpoint.start.next_offset < 0
            || checkpoint.start.next_index > checkpoint.committed.next_index
            || checkpoint.committed.next_index > checkpoint.durable.next_index
            || checkpoint.start.next_offset > checkpoint.committed.next_offset
            || checkpoint.committed.next_offset > checkpoint.durable.next_offset
            || (checkpoint.start.next_index == 1) != (checkpoint.start_term == 0)
        {
            return Err(corrupt("invalid manifest boundaries"));
        }
        Ok(checkpoint)
    }

    pub fn save(self, root: &Path) -> io::Result<()> {
        self.write_manifest(root, MANIFEST, true)
    }

    pub fn save_buffered(self, root: &Path) -> io::Result<()> {
        self.write_manifest(root, MANIFEST, false)
    }

    pub fn save_stable(self, root: &Path) -> io::Result<()> {
        self.write_manifest(root, "manifest.stable", true)
    }

    fn encode(self) -> [u8; CHECKPOINT_LEN] {
        let mut bytes = [0; CHECKPOINT_LEN];
        bytes[..8].copy_from_slice(MAGIC);
        for (offset, position) in [(8, self.start), (24, self.durable), (40, self.committed)] {
            bytes[offset..offset + 8].copy_from_slice(&position.next_index.to_be_bytes());
            bytes[offset + 8..offset + 16].copy_from_slice(&position.next_offset.to_be_bytes());
        }
        bytes[56..64].copy_from_slice(&self.start_term.to_be_bytes());
        let checksum = crc32c(&bytes[..64]);
        bytes[64..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn write_manifest(self, root: &Path, name: &str, sync: bool) -> io::Result<()> {
        let temporary = root.join(format!("{name}.tmp"));
        let mut file = File::create(&temporary)?;
        file.write_all(&self.encode())?;
        if sync {
            file.sync_all()?;
        }
        fs::rename(&temporary, root.join(name))?;
        if sync {
            File::open(root)?.sync_all()?;
        }
        Ok(())
    }
}

pub(crate) struct ProgressManifest {
    file: File,
    generation: u64,
}

impl ProgressManifest {
    pub fn open(root: &Path) -> io::Result<Option<(Self, Checkpoint)>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(MANIFEST))
        {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        match file.metadata()?.len() {
            len if len == CHECKPOINT_LEN as u64 => return Ok(None),
            len if len == 2 * SLOT_STRIDE => {}
            _ => return Err(corrupt("invalid progress manifest size")),
        }
        let mut latest = None;
        for slot in 0..2 {
            let mut record = [0; SLOT_LEN];
            file.seek(SeekFrom::Start(slot * SLOT_STRIDE))?;
            file.read_exact(&mut record)?;
            let generation = u64::from_be_bytes(field(&record, 0));
            if generation == 0
                || generation % 2 != slot
                || crc32c(&record[..SLOT_LEN - 4])
                    != u32::from_be_bytes(field(&record, SLOT_LEN - 4))
            {
                continue;
            }
            let checkpoint = Checkpoint::decode(&field(&record, 8))?;
            if latest.is_none_or(|(previous, _)| generation > previous) {
                latest = Some((generation, checkpoint));
            }
        }
        let (generation, checkpoint) =
            latest.ok_or_else(|| corrupt("no valid progress manifest slot"))?;
        Ok(Some((Self { file, generation }, checkpoint)))
    }

    pub fn create(root: &Path, checkpoint: Checkpoint) -> io::Result<Self> {
        let temporary = root.join("manifest.tmp");
        let file = File::create(&temporary)?;
        file.set_len(2 * SLOT_STRIDE)?;
        let mut manifest = Self {
            file,
            generation: 0,
        };
        manifest.save(checkpoint)?;
        fs::rename(temporary, root.join(MANIFEST))?;
        File::open(root)?.sync_all()?;
        Ok(manifest)
    }

    pub fn save(&mut self, checkpoint: Checkpoint) -> io::Result<()> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("progress manifest generation exhausted"))?;
        let mut record = [0; SLOT_LEN];
        record[..8].copy_from_slice(&generation.to_be_bytes());
        record[8..SLOT_LEN - 4].copy_from_slice(&checkpoint.encode());
        let checksum = crc32c(&record[..SLOT_LEN - 4]);
        record[SLOT_LEN - 4..].copy_from_slice(&checksum.to_be_bytes());
        self.file
            .seek(SeekFrom::Start((generation % 2) * SLOT_STRIDE))?;
        self.file.write_all(&record)?;
        self.file.sync_data()?;
        self.generation = generation;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    pub position: Position,
    pub term: u64,
    pub append_timestamp_ms: i64,
    pub record_count: u32,
    pub payload_len: u32,
}

pub(crate) fn write_entry(writer: &mut File, entry: Entry, payload: &[u8]) -> io::Result<()> {
    let mut header = [0; HEADER_LEN as usize];
    header[..4].copy_from_slice(&entry.payload_len.to_be_bytes());
    header[4..12].copy_from_slice(&entry.position.next_index.to_be_bytes());
    header[12..20].copy_from_slice(&entry.term.to_be_bytes());
    header[20..28].copy_from_slice(&entry.append_timestamp_ms.to_be_bytes());
    header[28..32].copy_from_slice(&entry.record_count.to_be_bytes());

    let base_offset = entry.position.next_offset.to_be_bytes();
    let mut checksum = crc32c(&header[..32]);
    if entry.record_count == 0 {
        checksum = crc32c_append(checksum, payload);
    } else {
        checksum = crc32c_append(checksum, &base_offset);
        checksum = crc32c_append(checksum, &payload[8..]);
    }
    header[32..].copy_from_slice(&checksum.to_be_bytes());
    let (offset, payload) = if entry.record_count == 0 {
        (&[][..], payload)
    } else {
        (base_offset.as_slice(), &payload[8..])
    };
    let mut data = [
        IoSlice::new(&header),
        IoSlice::new(offset),
        IoSlice::new(payload),
    ];
    let mut slices = &mut data[..];
    while !slices.is_empty() {
        match writer.write_vectored(slices) {
            Ok(0) => return Err(io::Error::from(ErrorKind::WriteZero)),
            Ok(written) => IoSlice::advance_slices(&mut slices, written),
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn read_entry(
    reader: &mut impl Read,
    remaining: u64,
    position: Position,
    payload: &mut Vec<u8>,
) -> io::Result<Entry> {
    if remaining < HEADER_LEN {
        return Err(corrupt("truncated entry header"));
    }
    let mut header = [0; HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    let payload_len = u32::from_be_bytes(field(&header, 0));
    let index = u64::from_be_bytes(field(&header, 4));
    let term = u64::from_be_bytes(field(&header, 12));
    let append_timestamp_ms = i64::from_be_bytes(field(&header, 20));
    let record_count = u32::from_be_bytes(field(&header, 28));
    if payload_len as usize > MAX_PAYLOAD
        || u64::from(payload_len) > remaining - HEADER_LEN
        || index != position.next_index
        || term == 0
    {
        return Err(corrupt("invalid entry header"));
    }
    payload.resize(payload_len as usize, 0);
    reader.read_exact(payload)?;
    let checksum = crc32c_append(crc32c(&header[..32]), payload);
    if checksum != u32::from_be_bytes(field(&header, 32)) {
        return Err(corrupt("entry checksum mismatch"));
    }
    if record_count > 0
        && (batch_fields(payload)
            .map_err(|_| corrupt("invalid stored Kafka batch"))?
            .0
            != record_count
            || i64::from_be_bytes(field(payload, 0)) != position.next_offset)
    {
        return Err(corrupt("invalid stored Kafka batch offset or record count"));
    }
    Ok(Entry {
        position,
        term,
        append_timestamp_ms,
        record_count,
        payload_len,
    })
}

/// Validates the v2 envelope. Record-body and codec validation belongs to Kafka admission.
pub(crate) fn batch_fields(batch: &[u8]) -> io::Result<(u32, i64)> {
    if batch.len() < 61
        || batch[16] != 2
        || i32::from_be_bytes(field(batch, 8)) as usize != batch.len() - 12
    {
        return Err(invalid("invalid Kafka v2 batch header"));
    }
    let count = i32::from_be_bytes(field(batch, 57));
    let delta = i32::from_be_bytes(field(batch, 23));
    if count <= 0 || delta != count - 1 {
        return Err(invalid("data batches require contiguous record offsets"));
    }
    Ok((count as u32, i64::from_be_bytes(field(batch, 35))))
}

pub(crate) fn batch_max_timestamp(batch: &[u8]) -> i64 {
    i64::from_be_bytes(field(batch, 35))
}

pub(crate) fn field<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("fixed-width field")
}

pub(crate) fn corrupt(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message)
}
