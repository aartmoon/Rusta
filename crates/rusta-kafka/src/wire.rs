use anyhow::{Result, bail, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{self, api_versions_response::ApiVersion};
use kafka_protocol::protocol::{Decodable, Encodable};

pub const MAX_FRAME: usize = 8 * 1024 * 1024;
const API_RANGES: &[(i16, i16, i16)] = &[
    (0, 9, 11),  // Produce
    (1, 12, 13), // Fetch
    (2, 6, 7),   // ListOffsets
    (3, 9, 12),  // Metadata
    (18, 0, 4),  // ApiVersions
    (19, 5, 7),  // CreateTopics
    (22, 2, 4),  // InitProducerId
    (23, 4, 4),  // OffsetForLeaderEpoch
];

pub enum Request {
    ApiVersions(messages::ApiVersionsRequest),
    Metadata(messages::MetadataRequest),
    CreateTopics(messages::CreateTopicsRequest),
    Produce(messages::ProduceRequest),
    Fetch(messages::FetchRequest),
    ListOffsets(messages::ListOffsetsRequest),
    InitProducerId(messages::InitProducerIdRequest),
    OffsetForLeaderEpoch(messages::OffsetForLeaderEpochRequest),
    UnsupportedApiVersions,
}

pub struct Header {
    pub key: i16,
    pub version: i16,
    pub correlation: i32,
}

pub fn api_versions(error_code: i16) -> messages::ApiVersionsResponse {
    let api_keys = API_RANGES
        .iter()
        .map(|&(key, min, max)| {
            ApiVersion::default()
                .with_api_key(key)
                .with_min_version(min)
                .with_max_version(max)
        })
        .collect();
    messages::ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(api_keys)
}

pub fn decode(mut frame: Bytes) -> Result<(Header, Request)> {
    ensure!(
        (8..=MAX_FRAME).contains(&frame.len()),
        "invalid request frame length"
    );
    let header = Header {
        key: frame.get_i16(),
        version: frame.get_i16(),
        correlation: frame.get_i32(),
    };
    let version = header.version;
    let Some(&(_, min, max)) = API_RANGES.iter().find(|r| r.0 == header.key) else {
        bail!("unsupported Kafka API {}", header.key);
    };
    if !(min..=max).contains(&version) {
        if header.key == 18 {
            return Ok((header, Request::UnsupportedApiVersions));
        }
        bail!("unsupported API version {version} for key {}", header.key);
    }

    let mut cursor = Guard {
        bytes: &frame,
        items: 0,
    };
    let client_id = cursor.take(2)?.try_into().map(i16::from_be_bytes).unwrap();
    ensure!(client_id >= -1, "invalid client ID length");
    cursor.take(client_id.max(0) as usize)?;
    if header.key != 18 || version >= 3 {
        cursor.tags()?;
    }
    let skipped = frame.len() - cursor.bytes.len();
    let items = cursor.items;
    frame.advance(skipped);

    // The generated array decoder preallocates from the wire count. Bound every
    // collection before calling it, and retain its normal typed decoding.
    let mut guard = Guard {
        bytes: &frame,
        items,
    };
    guard.body(header.key, version)?;
    ensure!(guard.bytes.is_empty(), "extra bytes after Kafka request");

    let request = match header.key {
        0 => Request::Produce(messages::ProduceRequest::decode(&mut frame, version)?),
        1 => Request::Fetch(messages::FetchRequest::decode(&mut frame, version)?),
        2 => Request::ListOffsets(messages::ListOffsetsRequest::decode(&mut frame, version)?),
        3 => Request::Metadata(messages::MetadataRequest::decode(&mut frame, version)?),
        18 => Request::ApiVersions(messages::ApiVersionsRequest::decode(&mut frame, version)?),
        19 => Request::CreateTopics(messages::CreateTopicsRequest::decode(&mut frame, version)?),
        22 => Request::InitProducerId(messages::InitProducerIdRequest::decode(
            &mut frame, version,
        )?),
        23 => Request::OffsetForLeaderEpoch(messages::OffsetForLeaderEpochRequest::decode(
            &mut frame, version,
        )?),
        _ => unreachable!(),
    };
    ensure!(
        frame.is_empty(),
        "Kafka decoder did not consume the request"
    );
    Ok((header, request))
}

pub fn encode(header: &Header, version: i16, response: &impl Encodable) -> Result<Bytes> {
    let body_len = response.compute_size(version)?;
    let flex = header.key != 18;
    let frame_len = body_len + 4 + usize::from(flex);
    ensure!(frame_len <= MAX_FRAME, "response exceeds the frame budget");
    let mut output = BytesMut::with_capacity(frame_len + 4);
    output.put_i32(frame_len as i32);
    output.put_i32(header.correlation);
    if flex {
        output.put_u8(0);
    }
    response.encode(&mut output, version)?;
    ensure!(
        output.len() == frame_len + 4,
        "incorrect encoded response size"
    );
    Ok(output.freeze())
}

struct Guard<'a> {
    bytes: &'a [u8],
    items: usize,
}

impl<'a> Guard<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        ensure!(size <= self.bytes.len(), "truncated Kafka request");
        let (value, rest) = self.bytes.split_at(size);
        self.bytes = rest;
        Ok(value)
    }

    fn varuint(&mut self) -> Result<u32> {
        let mut result = 0;
        for shift in (0..=28).step_by(7) {
            let byte = self.take(1)?[0];
            ensure!(shift != 28 || byte <= 15, "Kafka varint overflow");
            result |= u32::from(byte & 127) << shift;
            if byte < 128 {
                return Ok(result);
            }
        }
        bail!("invalid Kafka varint")
    }

    fn compact(&mut self) -> Result<()> {
        let size = self.varuint()?.saturating_sub(1) as usize;
        self.take(size)?;
        Ok(())
    }

    fn charge(&mut self, count: usize) -> Result<()> {
        ensure!(
            count <= 4096 - self.items,
            "too many Kafka collection items"
        );
        self.items += count;
        Ok(())
    }

    fn array(&mut self, mut element: impl FnMut(&mut Self) -> Result<()>) -> Result<()> {
        let count = self.varuint()?.saturating_sub(1) as usize;
        self.charge(count)?;
        for _ in 0..count {
            element(self)?;
        }
        Ok(())
    }

    fn tags(&mut self) -> Result<()> {
        let count = self.varuint()? as usize;
        self.charge(count)?;
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varuint()?;
            ensure!(
                previous.is_none_or(|previous| tag > previous),
                "invalid Kafka tag order"
            );
            previous = Some(tag);
            let size = self.varuint()? as usize;
            self.take(size)?;
        }
        Ok(())
    }

    fn body(&mut self, key: i16, version: i16) -> Result<()> {
        match key {
            0 => {
                self.compact()?;
                self.take(6)?;
                self.array(|topic| {
                    topic.compact()?;
                    topic.array(|partition| {
                        partition.take(4)?;
                        partition.compact()?;
                        partition.tags()
                    })?;
                    topic.tags()
                })?;
            }
            1 => {
                self.take(25)?;
                let topic_id = |topic: &mut Self| -> Result<()> {
                    if version >= 13 {
                        topic.take(16)?;
                    } else {
                        topic.compact()?;
                    }
                    Ok(())
                };
                self.array(|topic| {
                    topic_id(topic)?;
                    topic.array(|partition| {
                        partition.take(32)?;
                        partition.tags()
                    })?;
                    topic.tags()
                })?;
                self.array(|topic| {
                    topic_id(topic)?;
                    topic.array(|partition| {
                        partition.take(4)?;
                        Ok(())
                    })?;
                    topic.tags()
                })?;
                self.compact()?;
            }
            2 | 23 => {
                self.take(if key == 2 { 5 } else { 4 })?;
                self.array(|topic| {
                    topic.compact()?;
                    topic.array(|partition| {
                        partition.take(if key == 2 { 16 } else { 12 })?;
                        partition.tags()
                    })?;
                    topic.tags()
                })?;
            }
            3 => {
                self.array(|topic| {
                    if version >= 10 {
                        topic.take(16)?;
                    }
                    topic.compact()?;
                    topic.tags()
                })?;
                self.take(if version <= 10 { 3 } else { 2 })?;
            }
            18 if version < 3 => return Ok(()),
            18 => {
                self.compact()?;
                self.compact()?;
            }
            19 => {
                self.array(|topic| {
                    topic.compact()?;
                    topic.take(6)?;
                    topic.array(|assignment| {
                        assignment.take(4)?;
                        assignment.array(|broker| {
                            broker.take(4)?;
                            Ok(())
                        })?;
                        assignment.tags()
                    })?;
                    topic.array(|config| {
                        config.compact()?;
                        config.compact()?;
                        config.tags()
                    })?;
                    topic.tags()
                })?;
                self.take(5)?;
            }
            22 => {
                self.compact()?;
                self.take(if version >= 3 { 14 } else { 4 })?;
            }
            _ => unreachable!(),
        }
        self.tags()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::protocol::encode_request_header_into_buffer;

    #[test]
    fn rejects_huge_collection_before_generated_decoder() {
        let mut bytes = BytesMut::new();
        let header = messages::RequestHeader::default()
            .with_request_api_key(19)
            .with_request_api_version(7);
        encode_request_header_into_buffer(&mut bytes, &header).unwrap();
        bytes.extend_from_slice(&[255, 255, 255, 255, 7, 0, 0, 0, 0, 0, 0]);
        let error = decode(bytes.freeze()).err().unwrap();
        assert!(error.to_string().contains("collection"));
    }

    #[test]
    fn all_advertised_defaults_pass_structural_guard() {
        fn check(key: i16, version: i16, request: impl Encodable) {
            let mut bytes = BytesMut::new();
            let header = messages::RequestHeader::default()
                .with_request_api_key(key)
                .with_request_api_version(version);
            encode_request_header_into_buffer(&mut bytes, &header).unwrap();
            request.encode(&mut bytes, version).unwrap();
            assert!(decode(bytes.freeze()).is_ok(), "API {key} v{version}");
        }
        for &(key, min, max) in API_RANGES {
            for v in min..=max {
                match key {
                    0 => check(key, v, messages::ProduceRequest::default()),
                    1 => check(key, v, messages::FetchRequest::default()),
                    2 => check(key, v, messages::ListOffsetsRequest::default()),
                    3 => check(key, v, messages::MetadataRequest::default()),
                    18 => check(key, v, messages::ApiVersionsRequest::default()),
                    19 => check(key, v, messages::CreateTopicsRequest::default()),
                    22 => check(key, v, messages::InitProducerIdRequest::default()),
                    23 => check(key, v, messages::OffsetForLeaderEpochRequest::default()),
                    _ => unreachable!(),
                }
            }
        }
    }
}
