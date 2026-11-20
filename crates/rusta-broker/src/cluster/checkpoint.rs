use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::path::Path;

use anyhow::{Result, ensure};
use raft::eraftpb::ConfState;
use rusta_log::{PartitionLog, Position};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{disk::replace, producers::Producers};

const FILE: &str = "data.checkpoint";
const MAGIC: &[u8; 8] = b"RUSTAD01";
const LIMIT: u64 = 512 * 1024;
pub const MAX_SNAPSHOT: u64 = 12 * 1024 * 1024;
const MAX_EXPANDED_STATE: u64 = 64 * 1024 * 1024;
const INTERNAL_MAGIC: &[u8; 8] = b"RUSTAI01";

#[derive(Deserialize, Serialize)]
pub struct DataCheckpoint<P = Producers> {
    pub next_index: u64,
    pub term: u64,
    pub next_offset: i64,
    pub retained_index: u64,
    pub retained_offset: i64,
    #[serde(default)]
    pub last_data_epoch: Option<i32>,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub producers: P,
}

impl DataCheckpoint {
    pub fn load(root: &Path, log: &PartitionLog) -> Result<Option<Self>> {
        let Some(bytes) = read(root, FILE, LIMIT)? else {
            return Ok(None);
        };
        let state = Self::decode(&bytes)?;
        state.validate(log)?;
        Ok(Some(state))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            (12..=LIMIT).contains(&(bytes.len() as u64)) && &bytes[..8] == MAGIC,
            "invalid data checkpoint size or version"
        );
        let checksum = u32::from_be_bytes(bytes[8..12].try_into()?);
        ensure!(
            checksum == crc32c::crc32c(&bytes[12..]),
            "data checkpoint checksum mismatch"
        );
        let state: Self = serde_json::from_slice(&bytes[12..])?;
        ensure!(
            state
                .last_data_epoch
                .is_none_or(|epoch| epoch >= 0 && epoch as u64 <= state.term)
                && !state.voters.is_empty()
                && valid_members(&state.voters)
                && valid_members(&state.learners)
                && state.learners.iter().all(|id| !state.voters.contains(id)),
            "invalid checkpoint membership"
        );
        state.producers.validate_checkpoint()?;
        Ok(state)
    }

    pub fn validate(&self, log: &PartitionLog) -> Result<()> {
        ensure!(
            (log.start().next_index..=log.committed().next_index).contains(&self.next_index)
                && (log.start().next_index..=self.next_index).contains(&self.retained_index)
                && log.term(self.next_index - 1)? == self.term
                && log.position_at(self.next_index)? == self.position()
                && log.position_at(self.retained_index)? == self.retained(),
            "data checkpoint disagrees with the committed log"
        );
        Ok(())
    }

    pub fn retained(&self) -> Position {
        Position {
            next_index: self.retained_index,
            next_offset: self.retained_offset,
        }
    }

    pub fn position(&self) -> Position {
        Position {
            next_index: self.next_index,
            next_offset: self.next_offset,
        }
    }
}

pub fn read(root: &Path, name: &str, limit: u64) -> Result<Option<Vec<u8>>> {
    let file = match File::open(root.join(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "checkpoint exceeds size limit");
    Ok(Some(bytes))
}

pub fn snapshot_parts(bytes: &[u8], internal: bool) -> Result<(&[u8], &[u8])> {
    if !internal {
        return Ok((bytes, &[]));
    }
    ensure!(
        (16..=MAX_SNAPSHOT).contains(&(bytes.len() as u64)) && &bytes[..8] == INTERNAL_MAGIC,
        "invalid internal snapshot size or version"
    );
    let size = u32::from_be_bytes(bytes[8..12].try_into()?) as usize;
    let crc = u32::from_be_bytes(bytes[12..16].try_into()?);
    ensure!(
        crc == crc32c::crc32c(&bytes[16..]),
        "internal snapshot checksum mismatch"
    );
    bytes[16..]
        .split_at_checked(size)
        .filter(|(_, state)| !state.is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid internal snapshot layout"))
}

pub fn seal_snapshot(metadata: &[u8], state: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16 + metadata.len() + state.len());
    bytes.extend_from_slice(INTERNAL_MAGIC);
    bytes.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(metadata);
    bytes.extend_from_slice(state);
    let crc = crc32c::crc32c(&bytes[16..]).to_be_bytes();
    bytes[12..16].copy_from_slice(&crc);
    bytes
}

pub fn encode_state(state: &impl Serialize) -> Result<Vec<u8>> {
    let output = Bounded {
        bytes: Vec::new(),
        limit: (MAX_SNAPSHOT - LIMIT - 16) as usize,
    };
    let mut encoder = zstd::stream::write::Encoder::new(output, 1)?;
    serde_json::to_writer(&mut encoder, state)?;
    Ok(encoder.finish()?.bytes)
}

pub fn decode_state<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let mut decoder = zstd::stream::read::Decoder::new(bytes)?;
    decoder.window_log_max(24)?;
    let mut input = decoder.take(MAX_EXPANDED_STATE + 1);
    let result = serde_json::from_reader(&mut input)?;
    ensure!(input.limit() > 0, "internal state exceeds decode budget");
    Ok(result)
}

struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other("internal state exceeds snapshot budget"));
        }
        self.bytes.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn valid_members(ids: &[u64]) -> bool {
    ids.iter().all(|id| (1..=255).contains(id)) && ids.windows(2).all(|ids| ids[0] < ids[1])
}

impl<'a> DataCheckpoint<&'a Producers> {
    pub fn save(
        root: &Path,
        log: &PartitionLog,
        retained: Position,
        membership: &ConfState,
        producers: &'a Producers,
    ) -> Result<u64> {
        let boundary = log.committed();
        let mut checkpoint = Self {
            next_index: boundary.next_index,
            term: log.term(boundary.next_index - 1)?,
            next_offset: boundary.next_offset,
            retained_index: retained.next_index,
            retained_offset: retained.next_offset,
            last_data_epoch: None,
            voters: membership.voters.clone(),
            learners: membership.learners.clone(),
            producers,
        };
        checkpoint.voters.sort_unstable();
        checkpoint.learners.sort_unstable();
        let bytes = checkpoint.encode()?;
        replace(root, FILE, &bytes)?;
        Ok(boundary.next_index)
    }
}

impl<P: Serialize> DataCheckpoint<P> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&[0; 4]);
        serde_json::to_writer(&mut bytes, self)?;
        ensure!(
            bytes.len() as u64 <= LIMIT,
            "data checkpoint exceeds size limit"
        );
        let checksum = crc32c::crc32c(&bytes[12..]).to_be_bytes();
        bytes[8..12].copy_from_slice(&checksum);
        Ok(bytes)
    }
}
