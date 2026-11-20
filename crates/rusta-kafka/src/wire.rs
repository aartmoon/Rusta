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
    (8, 8, 9),   // OffsetCommit
    (9, 6, 9),   // OffsetFetch
    (10, 2, 4),  // FindCoordinator
    (11, 5, 9),  // JoinGroup
    (12, 3, 4),  // Heartbeat
    (13, 1, 5),  // LeaveGroup
    (14, 3, 5),  // SyncGroup
    (15, 5, 6),  // DescribeGroups
    (16, 3, 5),  // ListGroups
    (18, 0, 4),  // ApiVersions
    (19, 4, 7),  // CreateTopics
    (20, 1, 6),  // DeleteTopics
    (22, 2, 4),  // InitProducerId
    (23, 2, 4),  // OffsetForLeaderEpoch
    (32, 1, 4),  // DescribeConfigs
    (44, 1, 1),  // IncrementalAlterConfigs
];

fn flexible(key: i16, version: i16) -> bool {
    let min = match key {
        10 | 18 => 3,
        11 => 6,
        12 | 13 | 14 | 20 | 23 | 32 => 4,
        19 => 5,
        _ => 0,
    };
    version >= min
}

pub enum Request {
    OffsetCommit(messages::OffsetCommitRequest),
    OffsetFetch(messages::OffsetFetchRequest),
    FindCoordinator(messages::FindCoordinatorRequest),
    JoinGroup(messages::JoinGroupRequest),
    Heartbeat(messages::HeartbeatRequest),
    LeaveGroup(messages::LeaveGroupRequest),
    SyncGroup(messages::SyncGroupRequest),
    DescribeGroups(messages::DescribeGroupsRequest),
    ListGroups(messages::ListGroupsRequest),
    ApiVersions(messages::ApiVersionsRequest),
    Metadata(messages::MetadataRequest),
    CreateTopics(messages::CreateTopicsRequest),
    DeleteTopics(messages::DeleteTopicsRequest),
    DescribeConfigs(messages::DescribeConfigsRequest),
    IncrementalAlterConfigs(messages::IncrementalAlterConfigsRequest),
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
    pub client_id: String,
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
    let mut header = Header {
        key: frame.get_i16(),
        version: frame.get_i16(),
        correlation: frame.get_i32(),
        client_id: String::new(),
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
        flexible: flexible(header.key, version),
    };
    let client_id = cursor.take(2)?.try_into().map(i16::from_be_bytes).unwrap();
    ensure!(client_id >= -1, "invalid client ID length");
    let client = std::str::from_utf8(cursor.take(client_id.max(0) as usize)?)?;
    if client.len() <= 256 {
        header.client_id = client.to_owned();
    }
    cursor.tags()?;
    let skipped = frame.len() - cursor.bytes.len();
    let items = cursor.items;
    let flexible = cursor.flexible;
    frame.advance(skipped);

    // The generated array decoder preallocates from the wire count. Bound every
    // collection before calling it, and retain its normal typed decoding.
    let validate = |bytes| {
        let mut guard = Guard {
            bytes,
            items,
            flexible,
        };
        guard.body(header.key, version)?;
        ensure!(
            guard.bytes.is_empty(),
            "extra bytes after Kafka API {} v{version}",
            header.key
        );
        Ok(())
    };
    if let Err(error) = validate(&frame) {
        // librdkafka 2.12.1 reserves four zero bytes for the compact null
        // topic array in its request for all topics.
        if header.key != 3 || !frame.starts_with(&[0; 4]) {
            return Err(error);
        }
        validate(&frame[3..])?;
        frame.advance(3);
    }

    let request = match header.key {
        0 => Request::Produce(messages::ProduceRequest::decode(&mut frame, version)?),
        1 => Request::Fetch(messages::FetchRequest::decode(&mut frame, version)?),
        2 => Request::ListOffsets(messages::ListOffsetsRequest::decode(&mut frame, version)?),
        3 => Request::Metadata(messages::MetadataRequest::decode(&mut frame, version)?),
        8 => Request::OffsetCommit(messages::OffsetCommitRequest::decode(&mut frame, version)?),
        9 => Request::OffsetFetch(messages::OffsetFetchRequest::decode(&mut frame, version)?),
        10 => Request::FindCoordinator(messages::FindCoordinatorRequest::decode(
            &mut frame, version,
        )?),
        11 => Request::JoinGroup(messages::JoinGroupRequest::decode(&mut frame, version)?),
        12 => Request::Heartbeat(messages::HeartbeatRequest::decode(&mut frame, version)?),
        13 => Request::LeaveGroup(messages::LeaveGroupRequest::decode(&mut frame, version)?),
        14 => Request::SyncGroup(messages::SyncGroupRequest::decode(&mut frame, version)?),
        15 => Request::DescribeGroups(messages::DescribeGroupsRequest::decode(
            &mut frame, version,
        )?),
        16 => Request::ListGroups(messages::ListGroupsRequest::decode(&mut frame, version)?),
        18 => Request::ApiVersions(messages::ApiVersionsRequest::decode(&mut frame, version)?),
        19 => Request::CreateTopics(messages::CreateTopicsRequest::decode(&mut frame, version)?),
        20 => Request::DeleteTopics(messages::DeleteTopicsRequest::decode(&mut frame, version)?),
        22 => Request::InitProducerId(messages::InitProducerIdRequest::decode(
            &mut frame, version,
        )?),
        23 => Request::OffsetForLeaderEpoch(messages::OffsetForLeaderEpochRequest::decode(
            &mut frame, version,
        )?),
        32 => Request::DescribeConfigs(messages::DescribeConfigsRequest::decode(
            &mut frame, version,
        )?),
        44 => Request::IncrementalAlterConfigs(messages::IncrementalAlterConfigsRequest::decode(
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
    let flex = header.key != 18 && flexible(header.key, version);
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
    flexible: bool,
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
        let size = if self.flexible {
            self.varuint()?.saturating_sub(1) as usize
        } else {
            let size = i16::from_be_bytes(self.take(2)?.try_into().unwrap());
            ensure!(size >= -1, "invalid Kafka string length");
            size.max(0) as usize
        };
        self.take(size)?;
        Ok(())
    }

    fn binary(&mut self) -> Result<()> {
        if self.flexible {
            return self.compact();
        }
        let size = self.classic_length()?;
        self.take(size)?;
        Ok(())
    }

    fn classic_length(&mut self) -> Result<usize> {
        let size = i32::from_be_bytes(self.take(4)?.try_into().unwrap());
        ensure!(size >= -1, "invalid Kafka collection length");
        Ok(size.max(0) as usize)
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
        let count = if self.flexible {
            self.varuint()?.saturating_sub(1) as usize
        } else {
            self.classic_length()?
        };
        self.charge(count)?;
        for _ in 0..count {
            element(self)?;
        }
        Ok(())
    }

    fn tags(&mut self) -> Result<()> {
        if !self.flexible {
            return Ok(());
        }
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

    fn compact_many(&mut self, count: usize) -> Result<()> {
        for _ in 0..count {
            self.compact()?;
        }
        Ok(())
    }

    fn offset_fetch_topic(&mut self) -> Result<()> {
        self.compact()?;
        self.array(|partition| {
            partition.take(4)?;
            Ok(())
        })?;
        self.tags()
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
                if key == 2 {
                    self.take(5)?;
                } else if version >= 3 {
                    self.take(4)?;
                }
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
            8 => {
                self.compact()?;
                self.take(4)?;
                self.compact_many(2)?;
                self.array(|topic| {
                    topic.compact()?;
                    topic.array(|partition| {
                        partition.take(16)?;
                        partition.compact()?;
                        partition.tags()
                    })?;
                    topic.tags()
                })?;
            }
            9 => {
                if version <= 7 {
                    self.compact()?;
                    self.array(Self::offset_fetch_topic)?;
                } else {
                    self.array(|group| {
                        group.compact()?;
                        if version >= 9 {
                            group.compact()?;
                            group.take(4)?;
                        }
                        group.array(Self::offset_fetch_topic)?;
                        group.tags()
                    })?;
                }
                if version >= 7 {
                    self.take(1)?;
                }
            }
            10 => {
                if version <= 3 {
                    self.compact()?;
                }
                self.take(1)?;
                if version >= 4 {
                    self.array(Self::compact)?;
                }
            }
            11 => {
                self.compact()?;
                self.take(8)?;
                self.compact_many(3)?;
                self.array(|protocol| {
                    protocol.compact()?;
                    protocol.binary()?;
                    protocol.tags()
                })?;
                if version >= 8 {
                    self.compact()?;
                }
            }
            12 | 14 => {
                self.compact()?;
                self.take(4)?;
                self.compact_many(2)?;
                if key == 14 {
                    if version >= 5 {
                        self.compact_many(2)?;
                    }
                    self.array(|member| {
                        member.compact()?;
                        member.binary()?;
                        member.tags()
                    })?;
                }
            }
            13 => {
                self.compact()?;
                if version < 3 {
                    self.compact()?;
                } else {
                    self.array(|member| {
                        member.compact_many(if version >= 5 { 3 } else { 2 })?;
                        member.tags()
                    })?;
                }
            }
            15 => {
                self.array(Self::compact)?;
                self.take(1)?;
            }
            16 => {
                if version >= 4 {
                    self.array(Self::compact)?;
                }
                if version >= 5 {
                    self.array(Self::compact)?;
                }
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
            20 => {
                if version == 6 {
                    self.array(|topic| {
                        topic.compact()?;
                        topic.take(16)?;
                        topic.tags()
                    })?;
                } else {
                    self.array(Self::compact)?;
                }
                self.take(4)?;
            }
            32 => {
                self.array(|resource| {
                    resource.take(1)?;
                    resource.compact()?;
                    resource.array(Self::compact)?;
                    resource.tags()
                })?;
                self.take(if version >= 3 { 2 } else { 1 })?;
            }
            44 => {
                self.array(|resource| {
                    resource.take(1)?;
                    resource.compact()?;
                    resource.array(|config| {
                        config.compact()?;
                        config.take(1)?;
                        config.compact()?;
                        config.tags()
                    })?;
                    resource.tags()
                })?;
                self.take(1)?;
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
    fn accepts_librdkafka_metadata_with_a_padded_null_topic_array() {
        let mut bytes = BytesMut::new();
        let header = messages::RequestHeader::default()
            .with_request_api_key(3)
            .with_request_api_version(12);
        encode_request_header_into_buffer(&mut bytes, &header).unwrap();
        bytes.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0]);
        let (_, request) = decode(bytes.freeze()).unwrap();
        let Request::Metadata(request) = request else {
            panic!("wrong request");
        };
        assert!(request.topics.is_none() && request.allow_auto_topic_creation);
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
                    8 => check(key, v, messages::OffsetCommitRequest::default()),
                    9 => check(key, v, messages::OffsetFetchRequest::default()),
                    10 => check(key, v, messages::FindCoordinatorRequest::default()),
                    11 => check(key, v, messages::JoinGroupRequest::default()),
                    12 => check(key, v, messages::HeartbeatRequest::default()),
                    13 => check(key, v, messages::LeaveGroupRequest::default()),
                    14 => check(key, v, messages::SyncGroupRequest::default()),
                    15 => check(key, v, messages::DescribeGroupsRequest::default()),
                    16 => check(key, v, messages::ListGroupsRequest::default()),
                    18 => check(key, v, messages::ApiVersionsRequest::default()),
                    19 => check(key, v, messages::CreateTopicsRequest::default()),
                    20 => check(key, v, messages::DeleteTopicsRequest::default()),
                    22 => check(key, v, messages::InitProducerIdRequest::default()),
                    23 => check(key, v, messages::OffsetForLeaderEpochRequest::default()),
                    32 => check(key, v, messages::DescribeConfigsRequest::default()),
                    44 => check(key, v, messages::IncrementalAlterConfigsRequest::default()),
                    _ => unreachable!(),
                }
            }
        }
    }
}
