use anyhow::Result;
use bytes::{Bytes, BytesMut};
use rusta_kafka::{
    Header, Request, api_versions, encode,
    messages::{self, BrokerId, ProducerId, TopicName},
    protocol::StrBytes,
    records::Batch,
};
use uuid::Uuid;

use crate::catalog::{Catalog, DEFAULT_RETENTION_MS, MAX_PARTITIONS, Topic, TopicConfig};
use crate::partition::MAX_BATCH;

pub const BROKER: BrokerId = BrokerId(1);
const FETCH_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
#[repr(i16)]
pub enum Error {
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopic = 3,
    LeaderUnavailable = 5,
    NotLeader = 6,
    RequestTimedOut = 7,
    MessageTooLarge = 10,
    OffsetMetadataTooLarge = 12,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMember = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    UnsupportedVersion = 35,
    TopicExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    PolicyViolation = 44,
    OutOfOrderSequence = 45,
    InvalidProducerEpoch = 47,
    Storage = 56,
    UnknownProducerId = 59,
    GroupIdNotFound = 69,
    FetchSessionNotFound = 70,
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnsupportedCompression = 76,
    MemberIdRequired = 79,
    GroupMaxSizeReached = 81,
    InvalidRecord = 87,
    UnknownTopicId = 100,
    InvalidTopicName = 17,
}

impl Error {
    pub fn storage(error: std::io::Error) -> Self {
        eprintln!("partition storage: {error}");
        Self::Storage
    }
}

#[derive(Clone)]
pub struct Endpoint {
    pub host: StrBytes,
    pub port: i32,
}

impl Catalog {
    pub fn request(
        &mut self,
        head: &Header,
        request: Request,
        endpoint: &Endpoint,
    ) -> Result<Option<Bytes>> {
        let v = head.version;
        let result = match request {
            Request::ApiVersions(_) => encode(head, v, &api_versions(0))?,
            Request::UnsupportedApiVersions => {
                encode(head, 0, &api_versions(Error::UnsupportedVersion as i16))?
            }
            Request::FindCoordinator(request) => {
                encode(head, v, &Self::find_coordinator(request, v, endpoint))?
            }
            Request::Heartbeat(request) => encode(head, v, &self.heartbeat(request))?,
            Request::LeaveGroup(request) => encode(head, v, &self.leave_group(request))?,
            Request::OffsetCommit(request) => encode(head, v, &self.offset_commit(request))?,
            Request::OffsetFetch(request) => encode(head, v, &self.offset_fetch(request, v))?,
            Request::DescribeGroups(request) => encode(head, v, &self.describe_groups(request, v))?,
            Request::ListGroups(request) => encode(head, v, &self.list_groups(request))?,
            Request::JoinGroup(_) | Request::SyncGroup(_) => {
                anyhow::bail!("group request must run through its completion channel")
            }
            Request::Metadata(request) => encode(head, v, &self.metadata(request, endpoint))?,
            Request::CreateTopics(request) => encode(head, v, &self.create_topics(request))?,
            Request::InitProducerId(request) => encode(head, v, &self.init_producer(request))?,
            Request::Produce(request) => {
                let no_reply = request.acks == 0;
                let response = self.produce(request);
                if no_reply {
                    anyhow::ensure!(
                        response
                            .responses
                            .iter()
                            .flat_map(|t| &t.partition_responses)
                            .all(|p| p.error_code == 0),
                        "unacknowledged produce failed"
                    );
                    return Ok(None);
                }
                encode(head, v, &response)?
            }
            Request::ListOffsets(request) => encode(head, v, &self.list_offsets(request))?,
            Request::OffsetForLeaderEpoch(request) => {
                encode(head, v, &self.epoch_offsets(request))?
            }
            Request::Fetch(request) => encode(head, v, &self.fetch(&request, v).0)?,
        };
        Ok(Some(result))
    }

    fn metadata(
        &self,
        request: messages::MetadataRequest,
        endpoint: &Endpoint,
    ) -> messages::MetadataResponse {
        use messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};

        let topics = match request.topics {
            None => self.topics.values().map(metadata_topic).collect(),
            Some(topics) => {
                let mut seen = std::collections::HashSet::new();
                topics
                    .into_iter()
                    .filter_map(
                        |topic| match self.topic(topic.name.as_ref(), topic.topic_id) {
                            Ok(found) if seen.insert(found.config.id) => {
                                Some(metadata_topic(found))
                            }
                            Ok(_) => None,
                            Err(error) => Some(
                                MetadataResponseTopic::default()
                                    .with_name(topic.name)
                                    .with_topic_id(topic.topic_id)
                                    .with_error_code(error as i16),
                            ),
                        },
                    )
                    .collect()
            }
        };
        let broker = MetadataResponseBroker::default()
            .with_node_id(BROKER)
            .with_host(endpoint.host.clone())
            .with_port(endpoint.port);
        messages::MetadataResponse::default()
            .with_brokers(vec![broker])
            .with_cluster_id(Some(StrBytes::from_string(self.cluster_id.to_string())))
            .with_controller_id(BROKER)
            .with_topics(topics)
    }

    fn topic(&self, name: Option<&TopicName>, id: Uuid) -> Result<&Topic, Error> {
        if !id.is_nil() {
            let from_id = self.names_by_id.get(&id).ok_or(Error::UnknownTopicId)?;
            if name.is_some_and(|name| name.0.as_str() != from_id.as_str()) {
                return Err(Error::UnknownTopicId);
            }
            return self.topics.get(from_id).ok_or(Error::UnknownTopicId);
        }
        let name = name.ok_or(Error::UnknownTopicId)?;
        self.topics.get(name.0.as_str()).ok_or(Error::UnknownTopic)
    }

    fn create_topics(
        &mut self,
        request: messages::CreateTopicsRequest,
    ) -> messages::CreateTopicsResponse {
        use messages::create_topics_response::{CreatableTopicConfigs, CreatableTopicResult};

        let mut remaining = MAX_PARTITIONS.saturating_sub(self.partition_count);
        let mut names = std::collections::HashSet::new();
        let results = request
            .topics
            .into_iter()
            .map(|topic| {
                let name = topic.name.0.as_str();
                let mut result = CreatableTopicResult::default().with_name(topic.name.clone());
                let outcome = if !names.insert(topic.name.clone()) {
                    Err(Error::InvalidRequest)
                } else if self.topics.contains_key(name) {
                    Err(Error::TopicExists)
                } else {
                    topic_config(&topic, remaining).and_then(|config| {
                        if !request.validate_only {
                            self.create_topic(config.clone()).map_err(|error| {
                                eprintln!("create topic {name}: {error:#}");
                                Error::Storage
                            })?;
                        }
                        remaining -= config.partitions;
                        Ok(config)
                    })
                };
                match outcome {
                    Ok(config) => {
                        result.num_partitions = config.partitions as i32;
                        result.replication_factor = 1;
                        if !request.validate_only {
                            result.topic_id = config.id;
                        }
                        let report = |name: &str, value: String, explicit: bool| {
                            CreatableTopicConfigs::default()
                                .with_name(StrBytes::from_string(name.to_owned()))
                                .with_value(Some(StrBytes::from_string(value)))
                                .with_config_source(if explicit { 1 } else { 5 })
                        };
                        result.configs = Some(vec![
                            report(
                                "retention.ms",
                                config
                                    .retention_ms
                                    .unwrap_or(DEFAULT_RETENTION_MS)
                                    .to_string(),
                                config.retention_ms.is_some(),
                            ),
                            report(
                                "retention.bytes",
                                config.retention_bytes.unwrap_or(-1).to_string(),
                                config.retention_bytes.is_some(),
                            ),
                            report("cleanup.policy", "delete".to_owned(), false),
                            report(
                                "rusta.allow.weak.acks",
                                config.allow_weak_acks.to_string(),
                                config.allow_weak_acks,
                            ),
                        ]);
                    }
                    Err(error) => result.error_code = error as i16,
                }
                result
            })
            .collect();
        messages::CreateTopicsResponse::default().with_topics(results)
    }

    fn init_producer(
        &mut self,
        request: messages::InitProducerIdRequest,
    ) -> messages::InitProducerIdResponse {
        let mut response = messages::InitProducerIdResponse::default();
        if request.transactional_id.is_some() {
            response.error_code = Error::UnsupportedVersion as i16;
            return response;
        }
        match self.allocate_producer() {
            Ok(id) => {
                response.producer_id = ProducerId(id);
                response.producer_epoch = 0;
            }
            Err(error) => {
                eprintln!("allocate producer ID: {error:#}");
                response.error_code = Error::Storage as i16;
            }
        }
        response
    }

    fn produce(&mut self, request: messages::ProduceRequest) -> messages::ProduceResponse {
        use messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};

        let highest = self.last_reserved_producer();
        let responses = request
            .topic_data
            .into_iter()
            .map(|topic| {
                let mut resolved = self.topics.get_mut(topic.name.0.as_str());
                let results = topic
                    .partition_data
                    .into_iter()
                    .map(|data| {
                        let mut response =
                            PartitionProduceResponse::default().with_index(data.index);
                        let result = (|| {
                            if request.transactional_id.is_some() {
                                return Err(Error::UnsupportedVersion);
                            }
                            if ![-1, 0, 1].contains(&request.acks) {
                                return Err(Error::InvalidRequiredAcks);
                            }
                            let topic = resolved.as_mut().ok_or(Error::UnknownTopic)?;
                            if request.acks != -1 && !topic.config.allow_weak_acks {
                                return Err(Error::PolicyViolation);
                            }
                            let partition = topic
                                .partitions
                                .get_mut(data.index as usize)
                                .ok_or(Error::UnknownTopic)?;
                            response.log_start_offset = partition.log.start().next_offset;
                            partition.append(
                                &data.records.ok_or(Error::InvalidRecord)?,
                                highest,
                                request.acks == -1,
                            )
                        })();
                        match result {
                            Ok(offset) => response.base_offset = offset,
                            Err(error) => response.error_code = error as i16,
                        }
                        response
                    })
                    .collect();
                TopicProduceResponse::default()
                    .with_name(topic.name)
                    .with_partition_responses(results)
            })
            .collect();
        messages::ProduceResponse::default().with_responses(responses)
    }

    pub fn fetch(
        &self,
        request: &messages::FetchRequest,
        version: i16,
    ) -> (messages::FetchResponse, usize, bool) {
        use messages::fetch_response::{FetchableTopicResponse, PartitionData};

        if request.session_id != 0 {
            return (
                messages::FetchResponse::default()
                    .with_error_code(Error::FetchSessionNotFound as i16),
                0,
                true,
            );
        }
        if request.replica_id.0 != -1
            || ![0, 1].contains(&request.isolation_level)
            || request.max_bytes < 0
            || request.min_bytes < 0
        {
            return (
                messages::FetchResponse::default().with_error_code(Error::InvalidRequest as i16),
                0,
                true,
            );
        }
        let maximum = (request.max_bytes as usize).min(FETCH_MAX_BYTES);
        let mut used = 0;
        let mut has_error = false;

        let responses = request
            .topics
            .iter()
            .map(|topic| {
                let resolved = self.topic((version == 12).then_some(&topic.topic), topic.topic_id);
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|part| {
                        let mut response =
                            PartitionData::default().with_partition_index(part.partition);
                        let result = (|| {
                            let topic = resolved?;
                            let partition = topic
                                .partitions
                                .get(part.partition as usize)
                                .ok_or(Error::UnknownTopic)?;
                            let end = partition.log.committed().next_offset;
                            let start = partition.log.start().next_offset;
                            response.high_watermark = end;
                            response.last_stable_offset = end;
                            response.log_start_offset = start;
                            if part.current_leader_epoch > 0 {
                                return Err(Error::UnknownLeaderEpoch);
                            }
                            if !(start..=end).contains(&part.fetch_offset) {
                                return Err(Error::OffsetOutOfRange);
                            }
                            if part.partition_max_bytes < 0 {
                                return Err(Error::InvalidRequest);
                            }
                            if used > 0 && used >= maximum {
                                return Ok(Bytes::new());
                            }
                            let remaining = maximum
                                .saturating_sub(used)
                                .min(part.partition_max_bytes as usize);
                            let mut records = BytesMut::new();
                            partition
                                .log
                                .visit_batches(part.fetch_offset, &mut Vec::new(), |_, bytes| {
                                    if (used != 0 || !records.is_empty())
                                        && records.len() + bytes.len() > remaining
                                    {
                                        return Ok(false);
                                    }
                                    if bytes.len() > MAX_BATCH {
                                        return Err(std::io::Error::other(
                                            "stored batch exceeds broker limit",
                                        ));
                                    }
                                    records.extend_from_slice(bytes);
                                    Ok(records.len() < remaining)
                                })
                                .map_err(Error::storage)?;
                            used += records.len();
                            Ok(records.freeze())
                        })();
                        match result {
                            Ok(records) => response.records = Some(records),
                            Err(error) => {
                                response.error_code = error as i16;
                                has_error = true;
                            }
                        }
                        response
                    })
                    .collect();
                FetchableTopicResponse::default()
                    .with_topic(topic.topic.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions)
            })
            .collect();
        (
            messages::FetchResponse::default().with_responses(responses),
            used,
            has_error,
        )
    }

    fn list_offsets(&self, request: messages::ListOffsetsRequest) -> messages::ListOffsetsResponse {
        use messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let resolved = self.topic(Some(&topic.name), Uuid::nil());
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|part| {
                        let mut response = ListOffsetsPartitionResponse::default()
                            .with_partition_index(part.partition_index);
                        let result = (|| {
                            if ![0, 1].contains(&request.isolation_level)
                                || request.replica_id.0 != -1
                            {
                                return Err(Error::InvalidRequest);
                            }
                            if part.current_leader_epoch > 0 {
                                return Err(Error::UnknownLeaderEpoch);
                            }
                            let partition = resolved?
                                .partitions
                                .get(part.partition_index as usize)
                                .ok_or(Error::UnknownTopic)?;
                            lookup(&partition.log, part.timestamp)
                        })();
                        match result {
                            Ok(Some((offset, timestamp))) => {
                                response.offset = offset;
                                response.timestamp = timestamp;
                                response.leader_epoch = 0;
                            }
                            Ok(None) => {}
                            Err(error) => response.error_code = error as i16,
                        }
                        response
                    })
                    .collect();
                ListOffsetsTopicResponse::default()
                    .with_name(topic.name)
                    .with_partitions(partitions)
            })
            .collect();
        messages::ListOffsetsResponse::default().with_topics(topics)
    }

    fn epoch_offsets(
        &self,
        request: messages::OffsetForLeaderEpochRequest,
    ) -> messages::OffsetForLeaderEpochResponse {
        use messages::offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderTopicResult,
        };
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let resolved = self.topic(Some(&topic.topic), Uuid::nil());
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|part| {
                        let mut response = EpochEndOffset::default().with_partition(part.partition);
                        let result = (|| {
                            if ![-1, -2].contains(&request.replica_id.0) {
                                return Err(Error::InvalidRequest);
                            }
                            if part.current_leader_epoch > 0 {
                                return Err(Error::UnknownLeaderEpoch);
                            }
                            resolved?
                                .partitions
                                .get(part.partition as usize)
                                .ok_or(Error::UnknownTopic)
                        })();
                        match result {
                            Ok(partition) if part.leader_epoch >= 0 => {
                                response.leader_epoch = 0;
                                response.end_offset = partition.log.committed().next_offset;
                            }
                            Ok(_) => {}
                            Err(error) => response.error_code = error as i16,
                        }
                        response
                    })
                    .collect();
                OffsetForLeaderTopicResult::default()
                    .with_topic(topic.topic)
                    .with_partitions(partitions)
            })
            .collect();
        messages::OffsetForLeaderEpochResponse::default().with_topics(topics)
    }
}

fn metadata_topic(topic: &Topic) -> messages::metadata_response::MetadataResponseTopic {
    use messages::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
    let name = TopicName(StrBytes::from_string(topic.config.name.clone()));
    let partitions = (0..topic.config.partitions)
        .map(|index| {
            MetadataResponsePartition::default()
                .with_partition_index(index as i32)
                .with_leader_id(BROKER)
                .with_leader_epoch(0)
                .with_replica_nodes(vec![BROKER])
                .with_isr_nodes(vec![BROKER])
        })
        .collect();
    MetadataResponseTopic::default()
        .with_name(Some(name))
        .with_topic_id(topic.config.id)
        .with_partitions(partitions)
}

pub fn topic_config(
    topic: &messages::create_topics_request::CreatableTopic,
    remaining: usize,
) -> Result<TopicConfig, Error> {
    let name = topic.name.0.as_str();
    if name.is_empty()
        || name.len() > 249
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-_".contains(&byte))
    {
        return Err(Error::InvalidTopicName);
    }
    let count = if topic.assignments.is_empty() {
        if ![-1, 1].contains(&topic.replication_factor) {
            return Err(Error::InvalidReplicationFactor);
        }
        match topic.num_partitions {
            -1 => 1,
            p if p > 0 => p as usize,
            _ => return Err(Error::InvalidPartitions),
        }
    } else {
        if topic.num_partitions != -1 || topic.replication_factor != -1 {
            return Err(Error::InvalidRequest);
        }
        let mut seen = std::collections::HashSet::new();
        for assignment in &topic.assignments {
            if !(0..topic.assignments.len() as i32).contains(&assignment.partition_index)
                || assignment.broker_ids != [BROKER]
                || !seen.insert(assignment.partition_index)
            {
                return Err(Error::InvalidAssignment);
            }
        }
        topic.assignments.len()
    };
    if count > remaining {
        return Err(Error::InvalidPartitions);
    }
    let mut retention_ms = None;
    let mut retention_bytes = None;
    let mut allow_weak_acks = false;
    let mut seen = std::collections::HashSet::new();
    for config in &topic.configs {
        let key = config.name.as_str();
        if !seen.insert(key) {
            return Err(Error::InvalidConfig);
        }
        let value = config.value.as_ref().ok_or(Error::InvalidConfig)?.as_str();
        let limit = || {
            value
                .parse::<i64>()
                .ok()
                .filter(|value| *value >= -1)
                .ok_or(Error::InvalidConfig)
        };
        match key {
            "retention.ms" => retention_ms = Some(limit()?),
            "retention.bytes" => retention_bytes = Some(limit()?),
            "cleanup.policy" if value == "delete" => {}
            "rusta.allow.weak.acks" => {
                allow_weak_acks = value.parse().map_err(|_| Error::InvalidConfig)?
            }
            _ => return Err(Error::InvalidConfig),
        }
    }
    Ok(TopicConfig {
        name: name.to_owned(),
        id: Uuid::new_v4(),
        partitions: count,
        allow_weak_acks,
        retention_ms,
        retention_bytes,
    })
}

pub fn lookup(log: &rusta_log::PartitionLog, timestamp: i64) -> Result<Option<(i64, i64)>, Error> {
    lookup_from(log, log.start().next_offset, timestamp)
}

pub fn lookup_from(
    log: &rusta_log::PartitionLog,
    start: i64,
    timestamp: i64,
) -> Result<Option<(i64, i64)>, Error> {
    match timestamp {
        -2 => Ok(Some((start, -1))),
        -1 => Ok(Some((log.committed().next_offset, -1))),
        -3 => {
            let mut maximum = None;
            log.visit_batches(start, &mut Vec::new(), |_, bytes| {
                let batch = Batch::parse(bytes).map_err(std::io::Error::other)?;
                if maximum.is_none_or(|(_, time)| batch.last_timestamp > time) {
                    maximum = batch
                        .scan(bytes, i64::MAX)
                        .map_err(std::io::Error::other)?
                        .maximum;
                }
                Ok(true)
            })
            .map_err(Error::storage)?;
            Ok(maximum)
        }
        time if time >= 0 => {
            let mut result = None;
            log.offset_for_timestamp_from(start, time, &mut Vec::new(), |_, bytes, target| {
                let batch = Batch::parse(bytes).map_err(std::io::Error::other)?;
                result = batch
                    .scan(bytes, target)
                    .map_err(std::io::Error::other)?
                    .first_match;
                Ok(result.map(|(offset, _)| offset))
            })
            .map_err(Error::storage)?;
            Ok(result)
        }
        _ => Err(Error::InvalidRequest),
    }
}
