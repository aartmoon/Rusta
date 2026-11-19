use std::borrow::Cow;
use std::io::{Cursor, Read};

use anyhow::{Result, bail, ensure};
use bytes::Buf;

const HEADER: usize = 61;
const MAX_EXPANDED: usize = 8 * 1024 * 1024;
const SNAPPY_MAGIC: &[u8; 8] = b"\x82SNAPPY\0";

#[derive(Clone, Copy, Debug)]
pub struct Batch {
    pub records: u32,
    pub producer: i64,
    pub epoch: i16,
    pub sequence: i32,
    pub crc: u32,
    pub last_timestamp: i64,
    first_timestamp: i64,
    codec: u16,
}

#[derive(Default)]
pub struct Timestamps {
    pub first_match: Option<(i64, i64)>,
    pub maximum: Option<(i64, i64)>,
}

/// Split one batch using the Kafka v2 length prefix.
fn take_batch(bytes: &mut &[u8]) -> Result<usize> {
    ensure!(bytes.len() >= HEADER, "incomplete record batch");
    let size = i32::from_be_bytes(bytes[8..12].try_into()?) as usize;
    ensure!(
        (HEADER - 12..=bytes.len() - 12).contains(&size),
        "invalid record batch size"
    );
    let len = size + 12;
    *bytes = &bytes[len..];
    Ok(len)
}

impl Batch {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let mut rest = bytes;
        take_batch(&mut rest)?;
        ensure!(
            rest.is_empty() && bytes[16] == 2,
            "expected exactly one v2 batch"
        );
        let mut data = &bytes[17..];
        let crc = data.get_u32();
        let attributes = data.get_u16();
        ensure!(
            attributes & 0xfff8 == 0,
            "unsupported record batch attributes"
        );
        let last_offset = data.get_i32();
        let first_timestamp = data.get_i64();
        let last_timestamp = data.get_i64();
        let producer = data.get_i64();
        let epoch = data.get_i16();
        let sequence = data.get_i32();
        let records = data.get_u32();
        ensure!(
            records > 0 && u32::try_from(last_offset)? == records - 1,
            "invalid batch offset range"
        );
        ensure!(
            last_timestamp >= first_timestamp,
            "invalid batch timestamps"
        );
        ensure!(
            (producer == -1 && epoch == -1 && sequence == -1)
                || (producer > 0 && epoch >= 0 && sequence >= 0),
            "invalid producer fields"
        );
        ensure!(
            crc32c::crc32c(&bytes[21..]) == crc,
            "Kafka batch checksum mismatch"
        );
        Ok(Self {
            records,
            producer,
            epoch,
            sequence,
            crc,
            last_timestamp,
            first_timestamp,
            codec: attributes & 7,
        })
    }

    pub fn compression_is_supported(self) -> bool {
        self.codec <= 4
    }

    pub fn scan(self, bytes: &[u8], target: i64) -> Result<Timestamps> {
        let body = expand(self.codec, &bytes[HEADER..])?;
        let base = i64::from_be_bytes(bytes[..8].try_into()?);
        let mut rest = body.as_ref();
        let mut result = Timestamps::default();

        for delta in 0..self.records {
            let length = signed(&mut rest, 5)?;
            let mut record = take(&mut rest, usize::try_from(length)?)?;
            ensure!(take(&mut record, 1)?[0] == 0, "invalid record attributes");
            let timestamp_delta = signed(&mut record, 10)?;
            let timestamp = self
                .first_timestamp
                .checked_add(timestamp_delta)
                .ok_or_else(|| anyhow::anyhow!("record timestamp overflow"))?;
            ensure!(
                signed(&mut record, 5)? == i64::from(delta),
                "noncontiguous record offset"
            );
            nullable(&mut record)?;
            nullable(&mut record)?;
            let headers = usize::try_from(signed(&mut record, 5)?)?;
            ensure!(headers <= record.len() / 2, "invalid record headers count");
            for _ in 0..headers {
                let length = usize::try_from(signed(&mut record, 5)?)?;
                std::str::from_utf8(take(&mut record, length)?)?;
                nullable(&mut record)?;
            }
            ensure!(record.is_empty(), "extra bytes after Kafka record");

            if timestamp >= target || result.maximum.is_none_or(|(_, max)| timestamp > max) {
                let offset = base
                    .checked_add(i64::from(delta))
                    .ok_or_else(|| anyhow::anyhow!("record offset overflow"))?;
                if timestamp >= target && result.first_match.is_none() {
                    result.first_match = Some((offset, timestamp));
                }
                if result.maximum.is_none_or(|(_, max)| timestamp > max) {
                    result.maximum = Some((offset, timestamp));
                }
            }
        }
        ensure!(rest.is_empty(), "record count does not match the batch");
        ensure!(
            result.maximum.map(|(_, timestamp)| timestamp) == Some(self.last_timestamp),
            "invalid batch maximum timestamp"
        );
        Ok(result)
    }
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    ensure!(len <= input.len(), "truncated Kafka record");
    let (data, rest) = input.split_at(len);
    *input = rest;
    Ok(data)
}

fn signed(input: &mut &[u8], max_bytes: u32) -> Result<i64> {
    let bits = if max_bytes == 5 { 32 } else { 64 };
    let mut result = 0_u64;
    for shift in (0..max_bytes).map(|i| i * 7) {
        let byte = take(input, 1)?[0];
        let size = (bits - shift).min(7);
        ensure!(size == 7 || byte < (1 << size), "record varint overflow");
        result |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Ok(((result >> 1) as i64) ^ -((result & 1) as i64));
        }
    }
    bail!("invalid record varint")
}

fn nullable(input: &mut &[u8]) -> Result<()> {
    let length = signed(input, 5)?;
    ensure!(length >= -1, "invalid nullable record field length");
    take(input, length.max(0) as usize)?;
    Ok(())
}

fn bounded(reader: impl Read) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    reader
        .take((MAX_EXPANDED + 1) as u64)
        .read_to_end(&mut output)?;
    ensure!(
        output.len() <= MAX_EXPANDED,
        "expanded Kafka batch exceeds its byte limit"
    );
    Ok(output)
}

fn expand(codec: u16, input: &[u8]) -> Result<Cow<'_, [u8]>> {
    let result = match codec {
        0 => return Ok(Cow::Borrowed(input)),
        1 => bounded(flate2::read::MultiGzDecoder::new(input))?,
        2 => snappy(input)?,
        3 => bounded(lz4_flex::frame::FrameDecoder::new(input))?,
        4 => {
            let mut reader = zstd::stream::read::Decoder::new(Cursor::new(input))?;
            reader.window_log_max(23)?;
            bounded(reader)?
        }
        _ => bail!("unsupported Kafka compression type"),
    };
    Ok(Cow::Owned(result))
}

fn snappy(mut input: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut decoder = snap::raw::Decoder::new();
    let mut append = |chunk: &[u8]| -> Result<()> {
        let size = snap::raw::decompress_len(chunk)?;
        ensure!(
            size <= MAX_EXPANDED - output.len(),
            "expanded Snappy batch exceeds its byte limit"
        );
        let start = output.len();
        output.resize(start + size, 0);
        ensure!(
            decoder.decompress(chunk, &mut output[start..])? == size,
            "bad Snappy block size"
        );
        Ok(())
    };
    if input.starts_with(SNAPPY_MAGIC) {
        ensure!(
            input.get(8..16) == Some(&[0, 0, 0, 1, 0, 0, 0, 1]),
            "invalid Xerial Snappy header"
        );
        input = &input[16..];
        while !input.is_empty() {
            let size = u32::from_be_bytes(take(&mut input, 4)?.try_into()?) as usize;
            append(take(&mut input, size)?)?;
        }
    } else {
        append(input)?;
    }
    Ok(output)
}
