use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use rusta_kafka::records::Batch;
use rusta_log::{Options, PartitionLog, Retention};
use serde::{Deserialize, Serialize};

use crate::catalog::{DEFAULT_RETENTION_MS, TopicConfig};

use crate::protocol::Error;

pub const MAX_BATCH: usize = 1024 * 1024;
const MAX_PRODUCER_STATES: usize = 256;
const MAX_CHECKPOINT_BYTES: u64 = 512 * 1024;
const PRODUCER_CHECKPOINT: &str = "producer-state.json";

pub struct Partition {
    pub log: PartitionLog,
    producers: HashMap<i64, History>,
    root: PathBuf,
    failed_maintenance: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct Written {
    sequence: i32,
    records: u32,
    crc: u32,
    offset: i64,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
struct History {
    recent: [Option<Written>; 5],
    next_sequence: i32,
}

impl History {
    fn find(&self, batch: Batch) -> Option<i64> {
        self.recent
            .iter()
            .flatten()
            .find(|written| {
                written.sequence == batch.sequence
                    && written.records == batch.records
                    && written.crc == batch.crc
            })
            .map(|written| written.offset)
    }

    fn add(&mut self, batch: Batch, offset: i64) {
        self.recent.rotate_right(1);
        self.recent[0] = Some(Written {
            sequence: batch.sequence,
            records: batch.records,
            crc: batch.crc,
            offset,
        });
        self.next_sequence = ((batch.sequence as u32 + batch.records) & i32::MAX as u32) as i32;
    }
}

impl Partition {
    pub fn open(path: PathBuf) -> Result<Self> {
        let log = PartitionLog::open(
            &path,
            Options {
                max_entry_bytes: MAX_BATCH,
                ..Options::default()
            },
        )?;
        let mut producers = HashMap::<i64, History>::new();
        let mut next = log.start().next_index;
        let end = log.committed().next_index;
        match File::open(path.join(PRODUCER_CHECKPOINT)) {
            Ok(file) => {
                let mut payload = Vec::new();
                file.take(MAX_CHECKPOINT_BYTES + 1)
                    .read_to_end(&mut payload)?;
                ensure!(
                    payload.len() as u64 <= MAX_CHECKPOINT_BYTES,
                    "oversized producer checkpoint"
                );
                let (checksum, payload) = payload
                    .split_first_chunk::<4>()
                    .ok_or_else(|| io::Error::other("incomplete producer checkpoint"))?;
                ensure!(
                    u32::from_be_bytes(*checksum) == crc32c::crc32c(payload),
                    "invalid producer checkpoint checksum"
                );
                let checkpoint: ProducerCheckpoint = serde_json::from_slice(payload)?;
                ensure!(
                    checkpoint.version == 1
                        && (next..=end).contains(&checkpoint.next_index)
                        && checkpoint.producers.len() <= MAX_PRODUCER_STATES,
                    "invalid producer checkpoint"
                );
                next = checkpoint.next_index;
                producers = checkpoint.producers;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && next == 1 => {}
            Err(error) => return Err(error.into()),
        }
        while next < end {
            next = log.visit_entries(next, end, 4 * 1024 * 1024, |entry, bytes| {
                if entry.record_count != 0 {
                    let batch = Batch::parse(bytes).map_err(std::io::Error::other)?;
                    if batch.producer > 0 {
                        if !producers.contains_key(&batch.producer)
                            && producers.len() >= MAX_PRODUCER_STATES
                        {
                            return Err(std::io::Error::other(
                                "producer history exceeds partition capacity",
                            ));
                        }
                        producers
                            .entry(batch.producer)
                            .or_default()
                            .add(batch, entry.position.next_offset);
                    }
                }
                Ok(())
            })?;
        }
        Ok(Self {
            log,
            producers,
            root: path,
            failed_maintenance: false,
        })
    }

    pub fn append(
        &mut self,
        records: &Bytes,
        last_reserved_producer: i64,
        durable: bool,
    ) -> Result<i64, Error> {
        if records.len() > MAX_BATCH {
            return Err(Error::MessageTooLarge);
        }
        if records.is_empty() {
            return Err(Error::InvalidRecord);
        }
        let batch = Batch::parse(records).map_err(|_| Error::CorruptMessage)?;
        if !batch.compression_is_supported() {
            return Err(Error::UnsupportedCompression);
        }
        batch
            .scan(records, i64::MAX)
            .map_err(|_| Error::InvalidRecord)?;

        let offset = self.log.end().next_offset;
        let mut producer_state = None;
        if batch.producer > 0 {
            if !durable {
                return Err(Error::InvalidRequiredAcks);
            }
            if batch.producer > last_reserved_producer {
                return Err(Error::UnknownProducerId);
            }
            if batch.epoch != 0 {
                return Err(Error::InvalidProducerEpoch);
            }
            let mut history = match self.producers.get(&batch.producer) {
                Some(history) => *history,
                None if batch.sequence != 0 => return Err(Error::UnknownProducerId),
                None if self.producers.len() >= MAX_PRODUCER_STATES => {
                    return Err(Error::PolicyViolation);
                }
                None => History::default(),
            };
            if batch.sequence != history.next_sequence {
                return history.find(batch).ok_or(Error::OutOfOrderSequence);
            }
            history.add(batch, offset);
            producer_state = Some(history);
        }

        let mut bytes = BytesMut::with_capacity(records.len());
        bytes.extend_from_slice(&records[..12]);
        bytes.put_i32(0); // Singleton Kafka leader epoch.
        bytes.extend_from_slice(&records[16..]);
        self.log
            .append_batch(1, now_ms(), &bytes)
            .map_err(Error::storage)?;
        if durable {
            self.log.commit().map_err(Error::storage)?;
        }
        if let Some(history) = producer_state {
            self.producers.insert(batch.producer, history);
        }
        Ok(offset)
    }

    pub fn maintain(&mut self, config: Option<&TopicConfig>) -> Result<bool> {
        if self.failed_maintenance {
            return Ok(false);
        }
        let result = self.maintain_inner(config);
        if result.is_err() {
            self.failed_maintenance = true;
        }
        result
    }

    fn maintain_inner(&mut self, config: Option<&TopicConfig>) -> Result<bool> {
        let changed = self.log.end() != self.log.committed();
        if changed {
            self.log.commit()?;
        }
        if let Some(config) = config {
            let time = config.retention_ms.unwrap_or(DEFAULT_RETENTION_MS);
            let size = config.retention_bytes.unwrap_or(-1);
            let retention = Retention {
                keep_since_ms: (time >= 0).then(|| now_ms().saturating_sub(time)),
                max_bytes: u64::try_from(size).ok(),
            };
            let next_index = self.log.committed().next_index;
            self.log.apply_retention_after(retention, || {
                save_producers(&self.root, next_index, &self.producers)
            })?;
        }
        Ok(changed)
    }
}

#[derive(Deserialize)]
struct ProducerCheckpoint {
    version: u8,
    next_index: u64,
    producers: HashMap<i64, History>,
}

fn save_producers(
    root: &Path,
    next_index: u64,
    producers: &HashMap<i64, History>,
) -> io::Result<()> {
    #[derive(Serialize)]
    struct Checkpoint<'a> {
        version: u8,
        next_index: u64,
        producers: &'a HashMap<i64, History>,
    }
    let checkpoint = Checkpoint {
        version: 1,
        next_index,
        producers,
    };
    let bytes = serde_json::to_vec(&checkpoint)?;
    if bytes.len() as u64 + 4 > MAX_CHECKPOINT_BYTES {
        return Err(io::Error::other("producer checkpoint is too large"));
    }
    let temporary = root.join("producer-state.tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(&crc32c::crc32c(&bytes).to_be_bytes())?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temporary, root.join(PRODUCER_CHECKPOINT))?;
    File::open(root)?.sync_all()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
