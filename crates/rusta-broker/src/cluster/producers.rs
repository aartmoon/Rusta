use std::collections::HashMap;

use anyhow::{Result, ensure};
use rusta_kafka::records::Batch;
use serde::{Deserialize, Serialize};

use crate::{
    partition::{History, MAX_PRODUCER_STATES},
    protocol::Error,
};

pub const PRODUCER_CLOCK: u8 = 251;
pub const RETIRE_PRODUCERS: u8 = 252;
pub const IMPORTED_PRODUCERS: u8 = 250;
const DEFAULT_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

pub fn retention_ms() -> Result<u64> {
    match std::env::var("RUSTA_CLUSTER_PRODUCER_RETENTION_MS") {
        Ok(value) => {
            let value = value.parse()?;
            ensure!(value > 0, "producer retention must be positive");
            Ok(value)
        }
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_RETENTION_MS),
        Err(error) => Err(error.into()),
    }
}

#[derive(Deserialize, Serialize)]
struct Producer {
    history: History,
    append_ms: u64,
}

#[derive(Default, Deserialize, Serialize)]
pub struct Producers {
    active: HashMap<i64, Producer>,
    legacy_since_ms: Option<u64>,
    // The low byte is the broker that allocated the ID. A fence also covers
    // unknown older IDs from that broker. Existing active histories take precedence.
    retired_through: HashMap<u8, i64>,
}

impl Producers {
    pub fn imported_record(histories: &HashMap<i64, History>) -> Result<Vec<u8>> {
        let histories: std::collections::BTreeMap<_, _> = histories.iter().collect();
        let mut bytes = vec![IMPORTED_PRODUCERS];
        serde_json::to_writer(&mut bytes, &histories)?;
        Ok(bytes)
    }

    pub fn import(&mut self, record: &[u8]) -> Result<()> {
        ensure!(
            self.legacy_since_ms.is_none() && self.retired_through.is_empty(),
            "cannot replace live producer state"
        );
        let histories: HashMap<i64, History> = serde_json::from_slice(&record[1..])?;
        self.active = histories
            .into_iter()
            .map(|(id, history)| {
                (
                    id,
                    Producer {
                        history,
                        append_ms: 0,
                    },
                )
            })
            .collect();
        self.validate_checkpoint()
    }

    pub fn validate_checkpoint(&self) -> Result<()> {
        ensure!(
            self.active.len() <= MAX_PRODUCER_STATES
                && self.active.keys().all(|id| *id > 0)
                && self
                    .retired_through
                    .iter()
                    .all(|(&allocator, &id)| id > 0 && id as u8 == allocator)
                && self.legacy_since_ms != Some(0),
            "invalid producer checkpoint"
        );
        Ok(())
    }

    pub fn check(&self, batch: Batch) -> Result<Option<i64>, Error> {
        if let Some(producer) = self.active.get(&batch.producer) {
            return if batch.sequence == producer.history.next_sequence {
                Ok(None)
            } else {
                producer
                    .history
                    .find(batch)
                    .map(Some)
                    .ok_or(Error::OutOfOrderSequence)
            };
        }
        if batch.sequence != 0 || self.fenced(batch.producer) {
            return Err(Error::UnknownProducerId);
        }
        if self.active.len() == MAX_PRODUCER_STATES {
            return Err(Error::PolicyViolation);
        }
        Ok(None)
    }

    fn fenced(&self, id: i64) -> bool {
        self.retired_through
            .get(&(id as u8))
            .is_some_and(|through| id <= *through)
    }

    pub fn note_batch(&mut self, bytes: &[u8], offset: i64, append_ms: i64) -> Result<()> {
        let batch = Batch::parse(bytes)?;
        if batch.producer == -1 {
            return Ok(());
        }
        ensure!(
            self.active.contains_key(&batch.producer)
                || (self.active.len() < MAX_PRODUCER_STATES && !self.fenced(batch.producer)),
            "producer admission disagrees with committed log"
        );
        let append_ms = u64::try_from(append_ms)?;
        let state = self.active.entry(batch.producer).or_insert(Producer {
            history: History::default(),
            append_ms: 0,
        });
        state.history.add(batch, offset);
        state.append_ms = state.append_ms.max(append_ms);
        Ok(())
    }

    pub fn maintenance_record(&self, now_ms: u64, retention_ms: u64) -> Option<Vec<u8>> {
        if self.legacy_since_ms.is_none()
            && self.active.values().any(|producer| producer.append_ms == 0)
        {
            let mut bytes = Vec::with_capacity(9);
            bytes.push(PRODUCER_CLOCK);
            bytes.extend_from_slice(&now_ms.max(1).to_be_bytes());
            return Some(bytes);
        }
        let mut expired: Vec<_> = self
            .active
            .iter()
            .filter(|(_, producer)| {
                let last_append = if producer.append_ms == 0 {
                    self.legacy_since_ms.expect("legacy producer grace period")
                } else {
                    producer.append_ms
                };
                now_ms.saturating_sub(last_append) >= retention_ms
            })
            .map(|(&id, _)| id)
            .collect();
        if expired.is_empty() {
            return None;
        }
        expired.sort_unstable();
        let mut bytes = Vec::with_capacity(1 + expired.len() * 8);
        bytes.push(RETIRE_PRODUCERS);
        for id in expired {
            bytes.extend_from_slice(&id.to_be_bytes());
        }
        Some(bytes)
    }

    pub fn start_legacy_grace(&mut self, record: &[u8]) -> Result<()> {
        let rest = record
            .strip_prefix(&[PRODUCER_CLOCK])
            .ok_or_else(|| anyhow::anyhow!("invalid producer clock record"))?;
        let time = u64::from_be_bytes(rest.try_into()?);
        ensure!(
            self.legacy_since_ms.is_none() && time != 0,
            "invalid producer grace period"
        );
        self.legacy_since_ms = Some(time);
        Ok(())
    }

    pub fn retire(&mut self, record: &[u8]) -> Result<()> {
        let rest = record
            .strip_prefix(&[RETIRE_PRODUCERS])
            .ok_or_else(|| anyhow::anyhow!("invalid producer retirement record"))?;
        let (ids, rest) = rest.as_chunks::<8>();
        ensure!(
            !ids.is_empty() && ids.len() <= MAX_PRODUCER_STATES && rest.is_empty(),
            "invalid producer retirement record"
        );
        let mut previous = 0;
        for bytes in ids {
            let id = i64::from_be_bytes(*bytes);
            ensure!(
                id > previous && self.active.contains_key(&id),
                "invalid retired producer ID"
            );
            previous = id;
        }
        for bytes in ids {
            let id = i64::from_be_bytes(*bytes);
            self.active.remove(&id);
            self.retired_through
                .entry(id as u8)
                .and_modify(|fence| *fence = (*fence).max(id))
                .or_insert(id);
        }
        Ok(())
    }
}
