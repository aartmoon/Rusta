//! Bounded Kafka request decoding and record-batch validation.
#![forbid(unsafe_code)]

pub mod records;
mod wire;

pub use kafka_protocol::{messages, protocol};
pub use wire::{Header, MAX_FRAME, Request, api_versions, decode, encode};
