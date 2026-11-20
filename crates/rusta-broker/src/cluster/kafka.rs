use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, ensure};
use bytes::{Bytes, BytesMut};
use rusta_kafka::{
    Header, Request, api_versions, encode,
    messages::{self, BrokerId, ProducerId, TopicName},
    protocol::StrBytes,
};
use tokio::{sync::oneshot, time::timeout};
use uuid::Uuid;

use crate::protocol::{Error, lookup, topic_config};

use super::{
    Cluster,
    engine::{CONTROL, Engine, GroupId, Node, Write},
};

const DEADLINE: Duration = Duration::from_secs(3);
const FETCH_MAX: usize = 4 * 1024 * 1024;

fn code(error: Option<Error>) -> i16 {
    error.map_or(0, |error| error as i16)
}
fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
fn broker(id: u64) -> BrokerId {
    BrokerId(if id == 0 { -1 } else { id as i32 })
}

async fn reply<T>(receiver: oneshot::Receiver<Result<T, Error>>) -> Result<T, Error> {
    timeout(DEADLINE, receiver)
        .await
        .map_err(|_| Error::RequestTimedOut)?
        .map_err(|_| Error::Storage)?
}

impl Cluster {
    pub async fn request(
        self: &Arc<Self>,
        head: &Header,
        request: Request,
        internal: bool,
    ) -> Result<Option<Bytes>> {
        let version = head.version;
        let response = match request {
            Request::ApiVersions(_) | Request::UnsupportedApiVersions => {
                let unsupported = matches!(request, Request::UnsupportedApiVersions);
                let mut versions = api_versions(if unsupported {
                    Error::UnsupportedVersion as i16
                } else {
                    0
                });
                versions
                    .api_keys
                    .retain(|api| !(8..=16).contains(&api.api_key));
                encode(head, if unsupported { 0 } else { version }, &versions)?
            }
            Request::CreateTopics(request) => {
                encode(head, version, &self.create_topics(request).await?)?
            }
            Request::Metadata(request) => {
                let response = self
                    .call(move |engine| Ok(metadata(engine, request, internal)))
                    .await?;
                encode(head, version, &response)?
            }
            Request::InitProducerId(request) => {
                let mut response = messages::InitProducerIdResponse::default();
                if request.transactional_id.is_none() {
                    response.producer_id = ProducerId(self.call(Engine::allocate_producer).await?);
                    response.producer_epoch = 0;
                } else {
                    response.error_code = Error::UnsupportedVersion as i16;
                }
                encode(head, version, &response)?
            }
            Request::Produce(request) => {
                ensure!(request.acks != 0, "cluster requires acknowledged writes");
                encode(head, version, &self.produce(request).await?)?
            }
            Request::Fetch(request) => encode(head, version, &self.fetch(request, version).await?)?,
            Request::ListOffsets(request) => {
                encode(head, version, &self.list_offsets(request).await?)?
            }
            Request::OffsetForLeaderEpoch(request) => {
                encode(head, version, &self.epoch_offsets(request).await?)?
            }
            _ => anyhow::bail!("requested API is not available in cluster mode"),
        };
        Ok(Some(response))
    }

    async fn create_topics(
        self: &Arc<Self>,
        request: messages::CreateTopicsRequest,
    ) -> Result<messages::CreateTopicsResponse> {
        use messages::create_topics_response::CreatableTopicResult;
        let mut results = Vec::new();
        for topic in request.topics {
            let mut result = CreatableTopicResult::default().with_name(topic.name.clone());
            let prepare = self
                .call(move |engine| {
                    let outcome = (|| {
                        if engine.nodes[&CONTROL].leader() != engine.node_id() {
                            return Err(Error::NotController);
                        }
                        if engine.topics.contains_key(topic.name.0.as_str()) {
                            return Err(Error::TopicExists);
                        }
                        let size = engine.placement_count() as i16;
                        let factor = if topic.replication_factor == -1 {
                            size.min(3)
                        } else {
                            topic.replication_factor
                        };
                        if !(1..=size).contains(&factor) || !topic.assignments.is_empty() {
                            return Err(Error::InvalidReplicationFactor);
                        }
                        let mut cfg = topic_config(
                            &topic.with_replication_factor(1),
                            engine.cap().saturating_sub(engine.partitions()),
                        )?;
                        if cfg.retention_bytes.is_some_and(|limit| limit != -1)
                            || cfg.retention_ms.is_some_and(|limit| limit != -1)
                            || cfg.allow_weak_acks
                        {
                            return Err(Error::InvalidConfig);
                        }
                        cfg.retention_ms = Some(-1);
                        Ok((cfg, factor))
                    })();
                    Ok(outcome)
                })
                .await?;
            match prepare {
                Err(error) => result.error_code = error as i16,
                Ok((config, factor)) => {
                    let id = config.id;
                    let partitions = config.partitions as i32;
                    let outcome = if request.validate_only {
                        Ok(())
                    } else {
                        match self
                            .call(move |engine| engine.create(config, factor as usize))
                            .await?
                        {
                            Ok(receiver) => reply(receiver).await.map(|_| ()),
                            Err(error) => Err(error),
                        }
                    };
                    match outcome {
                        Ok(()) => {
                            result.num_partitions = partitions;
                            result.replication_factor = factor;
                            if !request.validate_only {
                                result.topic_id = id;
                            }
                        }
                        Err(error) => {
                            result.error_code = if matches!(error, Error::NotLeader) {
                                Error::NotController as i16
                            } else {
                                error as i16
                            }
                        }
                    }
                }
            }
            results.push(result);
        }
        Ok(messages::CreateTopicsResponse::default().with_topics(results))
    }

    async fn produce(
        self: &Arc<Self>,
        request: messages::ProduceRequest,
    ) -> Result<messages::ProduceResponse> {
        use messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
        let request_error = if request.transactional_id.is_some() {
            Some(Error::UnsupportedVersion)
        } else if request.acks != -1 {
            Some(Error::InvalidRequiredAcks)
        } else {
            None
        };
        let mut responses = Vec::new();
        for topic in request.topic_data {
            let mut partitions = Vec::new();
            for part in topic.partition_data {
                let index = part.index;
                let mut result = PartitionProduceResponse::default().with_index(index);
                let outcome = if let Some(error) = request_error {
                    Err(error)
                } else if let Some(records) = part.records {
                    let name = topic.name.0.as_str().to_owned();
                    let proposed = self
                        .call(move |engine| match engine.topic_key(&name, index) {
                            Ok(key) => engine.produce(key, records),
                            Err(error) => Ok(Err(error)),
                        })
                        .await?;
                    match proposed {
                        Ok(Write::Existing(offset, barrier)) => {
                            reply(barrier).await.map(|()| offset)
                        }
                        Ok(Write::Pending(receiver)) => reply(receiver).await,
                        Err(error) => Err(error),
                    }
                } else {
                    Err(Error::InvalidRecord)
                };
                match outcome {
                    Ok(offset) => {
                        result.base_offset = offset;
                        result.log_start_offset = 0;
                    }
                    Err(error) => result.error_code = error as i16,
                }
                partitions.push(result);
            }
            responses.push(
                TopicProduceResponse::default()
                    .with_name(topic.name)
                    .with_partition_responses(partitions),
            );
        }
        Ok(messages::ProduceResponse::default().with_responses(responses))
    }

    async fn guarded<T: Send + 'static>(
        self: &Arc<Self>,
        name: Option<String>,
        id: Uuid,
        partition: i32,
        expected_epoch: i32,
        read: impl FnOnce(&Node) -> Result<T, Error> + Send + 'static,
    ) -> Result<Result<T, Error>> {
        let authorization = self
            .call(move |engine| {
                let name = match name.as_deref() {
                    Some(name) => name,
                    None => match engine.names.get(&id) {
                        Some(name) => name,
                        None => return Ok(Err(Error::UnknownTopicId)),
                    },
                };
                let key = match engine.topic_key(name, partition) {
                    Ok(key) => key,
                    Err(error) => return Ok(Err(error)),
                };
                let Some(node) = engine.nodes.get(&key) else {
                    return Ok(Err(Error::NotLeader));
                };
                let epoch = node.epoch();
                if !node.local_leader() {
                    return Ok(Err(Error::NotLeader));
                }
                if expected_epoch > epoch {
                    return Ok(Err(Error::UnknownLeaderEpoch));
                }
                if expected_epoch >= 0 && expected_epoch < epoch {
                    return Ok(Err(Error::FencedLeaderEpoch));
                }
                engine
                    .read(key)
                    .map(|result| result.map(|receiver| (key, epoch, receiver)))
            })
            .await?;
        let (key, epoch, receiver) = match authorization {
            Ok(data) => data,
            Err(error) => return Ok(Err(error)),
        };
        if let Err(error) = reply(receiver).await {
            return Ok(Err(error));
        }
        self.call(move |engine| {
            let node = &engine.nodes[&key];
            if !node.local_leader() || node.epoch() != epoch {
                return Ok(Err(Error::NotLeader));
            }
            Ok(read(node))
        })
        .await
    }

    async fn fetch(
        self: &Arc<Self>,
        request: messages::FetchRequest,
        version: i16,
    ) -> Result<messages::FetchResponse> {
        use messages::fetch_response::{FetchableTopicResponse, PartitionData};
        if request.session_id != 0 {
            return Ok(messages::FetchResponse::default()
                .with_error_code(Error::FetchSessionNotFound as i16));
        }
        if request.replica_id.0 != -1
            || ![0, 1].contains(&request.isolation_level)
            || request.max_bytes < 0
        {
            return Ok(
                messages::FetchResponse::default().with_error_code(Error::InvalidRequest as i16)
            );
        }
        let maximum = (request.max_bytes as usize).min(FETCH_MAX);
        let mut used = 0;
        let mut responses = Vec::new();
        for topic in request.topics {
            let mut partitions = Vec::new();
            for part in topic.partitions {
                let mut response = PartitionData::default().with_partition_index(part.partition);
                let name = (version == 12).then(|| topic.topic.0.as_str().to_owned());
                let remaining = maximum
                    .saturating_sub(used)
                    .min(part.partition_max_bytes.max(0) as usize);
                let first = used == 0;
                let result = self
                    .guarded(
                        name,
                        topic.topic_id,
                        part.partition,
                        part.current_leader_epoch,
                        move |node| {
                            let log = node.log();
                            let start = log.start().next_offset;
                            let end = log.committed().next_offset;
                            if !(start..=end).contains(&part.fetch_offset) {
                                return Err(Error::OffsetOutOfRange);
                            }
                            if part.partition_max_bytes < 0 {
                                return Err(Error::InvalidRequest);
                            }
                            let mut records = BytesMut::new();
                            log.visit_batches(part.fetch_offset, &mut Vec::new(), |_, bytes| {
                                if (!first || !records.is_empty())
                                    && records.len() + bytes.len() > remaining
                                {
                                    return Ok(false);
                                }
                                records.extend_from_slice(bytes);
                                Ok(records.len() < remaining)
                            })
                            .map_err(Error::storage)?;
                            Ok((start, end, records.freeze()))
                        },
                    )
                    .await?;
                match result {
                    Ok((start, end, records)) => {
                        response.log_start_offset = start;
                        response.high_watermark = end;
                        response.last_stable_offset = end;
                        used += records.len();
                        response.records = Some(records);
                    }
                    Err(error) => response.error_code = error as i16,
                }
                partitions.push(response);
            }
            responses.push(
                FetchableTopicResponse::default()
                    .with_topic(topic.topic)
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        Ok(messages::FetchResponse::default().with_responses(responses))
    }

    async fn list_offsets(
        self: &Arc<Self>,
        request: messages::ListOffsetsRequest,
    ) -> Result<messages::ListOffsetsResponse> {
        use messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };
        let mut topics = Vec::new();
        for topic in request.topics {
            let mut partitions = Vec::new();
            for part in topic.partitions {
                let mut result = ListOffsetsPartitionResponse::default()
                    .with_partition_index(part.partition_index);
                let name = Some(topic.name.0.as_str().to_owned());
                let outcome = self
                    .guarded(
                        name,
                        Uuid::nil(),
                        part.partition_index,
                        part.current_leader_epoch,
                        move |node| {
                            if request.replica_id.0 != -1
                                || ![0, 1].contains(&request.isolation_level)
                            {
                                return Err(Error::InvalidRequest);
                            }
                            lookup(node.log(), part.timestamp).map(|value| {
                                value.map(|(offset, time)| (offset, time, node.epoch()))
                            })
                        },
                    )
                    .await?;
                match outcome {
                    Ok(Some((offset, timestamp, epoch))) => {
                        result.offset = offset;
                        result.timestamp = timestamp;
                        result.leader_epoch = epoch;
                    }
                    Ok(None) => {}
                    Err(error) => result.error_code = error as i16,
                }
                partitions.push(result);
            }
            topics.push(
                ListOffsetsTopicResponse::default()
                    .with_name(topic.name)
                    .with_partitions(partitions),
            );
        }
        Ok(messages::ListOffsetsResponse::default().with_topics(topics))
    }

    async fn epoch_offsets(
        self: &Arc<Self>,
        request: messages::OffsetForLeaderEpochRequest,
    ) -> Result<messages::OffsetForLeaderEpochResponse> {
        use messages::offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderTopicResult,
        };
        let mut topics = Vec::new();
        for topic in request.topics {
            let mut partitions = Vec::new();
            for part in topic.partitions {
                let mut response = EpochEndOffset::default().with_partition(part.partition);
                let outcome = self
                    .guarded(
                        Some(topic.topic.0.as_str().to_owned()),
                        Uuid::nil(),
                        part.partition,
                        part.current_leader_epoch,
                        move |node| {
                            if ![-1, -2].contains(&request.replica_id.0) {
                                return Err(Error::InvalidRequest);
                            }
                            if part.leader_epoch < 0 {
                                return Ok((-1, -1));
                            }
                            let log = node.log();
                            if part.leader_epoch >= node.epoch() {
                                return Ok((node.epoch(), log.committed().next_offset));
                            }
                            let mut value = (-1, log.start().next_offset);
                            log.visit_batches(
                                log.start().next_offset,
                                &mut Vec::new(),
                                |entry, _| {
                                    if entry.term > part.leader_epoch as u64 {
                                        return Ok(false);
                                    }
                                    value = (
                                        entry.term as i32,
                                        entry.position.next_offset + i64::from(entry.record_count),
                                    );
                                    Ok(true)
                                },
                            )
                            .map_err(Error::storage)?;
                            Ok(value)
                        },
                    )
                    .await?;
                match outcome {
                    Ok((epoch, end)) => {
                        response.leader_epoch = epoch;
                        response.end_offset = end;
                    }
                    Err(error) => response.error_code = error as i16,
                }
                partitions.push(response);
            }
            topics.push(
                OffsetForLeaderTopicResult::default()
                    .with_topic(topic.topic)
                    .with_partitions(partitions),
            );
        }
        Ok(messages::OffsetForLeaderEpochResponse::default().with_topics(topics))
    }
}

fn metadata(
    engine: &Engine,
    request: messages::MetadataRequest,
    internal: bool,
) -> messages::MetadataResponse {
    use messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    let brokers = engine
        .voters()
        .iter()
        .map(|&id| {
            let peer = &engine.settings.peers[&id];
            let endpoint = if internal { &peer.internal } else { &peer.host };
            MetadataResponseBroker::default()
                .with_node_id(broker(id))
                .with_host(endpoint.host.clone())
                .with_port(endpoint.port)
        })
        .collect();
    let topic_metadata = |name: &str| {
        let cfg = &engine.topics[name];
        let partitions = (0..cfg.partitions)
            .map(|partition| {
                let key = GroupId {
                    topic: cfg.id,
                    partition: partition as u32,
                };
                let (leader, epoch) = engine.leader_view(key);
                let replicas = engine.replicas(key);
                let isr = engine
                    .nodes
                    .get(&key)
                    .map_or_else(Vec::new, |node| node.in_sync());
                MetadataResponsePartition::default()
                    .with_partition_index(partition as i32)
                    .with_leader_id(broker(leader))
                    .with_leader_epoch(epoch)
                    .with_error_code(code((leader == 0).then_some(Error::LeaderUnavailable)))
                    .with_replica_nodes(replicas.iter().copied().map(broker).collect())
                    .with_isr_nodes(
                        isr.into_iter()
                            .filter(|id| replicas.contains(id))
                            .map(broker)
                            .collect(),
                    )
            })
            .collect();
        MetadataResponseTopic::default()
            .with_name(Some(TopicName(text(name))))
            .with_topic_id(cfg.id)
            .with_partitions(partitions)
    };
    let topics = match request.topics {
        None => engine
            .topics
            .keys()
            .map(|name| topic_metadata(name))
            .collect(),
        Some(topics) => {
            let mut seen = std::collections::HashSet::new();
            topics
                .into_iter()
                .filter_map(|topic| {
                    let name = if topic.topic_id.is_nil() {
                        topic.name.as_ref().map(|name| name.0.as_str())
                    } else {
                        engine.names.get(&topic.topic_id).map(String::as_str)
                    };
                    if let Some(name) = name.filter(|name| engine.topics.contains_key(*name)) {
                        return seen.insert(name.to_owned()).then(|| topic_metadata(name));
                    }
                    let error = if topic.topic_id.is_nil() {
                        Error::UnknownTopic
                    } else {
                        Error::UnknownTopicId
                    };
                    Some(
                        MetadataResponseTopic::default()
                            .with_name(topic.name)
                            .with_topic_id(topic.topic_id)
                            .with_error_code(error as i16),
                    )
                })
                .collect()
        }
    };
    messages::MetadataResponse::default()
        .with_brokers(brokers)
        .with_topics(topics)
        .with_cluster_id(Some(text(&engine.settings.cluster.to_string())))
        .with_controller_id(broker(engine.nodes[&CONTROL].leader()))
}
