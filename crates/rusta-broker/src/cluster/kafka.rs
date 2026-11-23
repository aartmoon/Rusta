use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Result, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::{
    StreamExt,
    stream::{self, FuturesUnordered},
};
use rusta_kafka::{
    Header, Request, api_versions, encode,
    messages::{self, BrokerId, ProducerId, TopicName, produce_response::PartitionProduceResponse},
    protocol::StrBytes,
    records::Batch,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};
use uuid::Uuid;

use crate::{
    group_protocol,
    protocol::{Error, lookup_from, topic_config},
    topic_admin,
};

use super::{
    Cluster, ack_profile,
    engine::{CONTROL, Engine, GroupId, Node, ProduceBatch, Write},
    executor::DiskSlot,
};

pub(super) const DEADLINE: Duration = Duration::from_secs(3);
const FETCH_MAX: usize = 4 * 1024 * 1024;
const FETCH_READ_CONCURRENCY: usize = 8;
const INLINE_FETCH_BYTES: usize = 64 * 1024;

fn code(error: Option<Error>) -> i16 {
    error.map_or(0, |error| error as i16)
}
fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
fn broker(id: u64) -> BrokerId {
    BrokerId(if id == 0 { -1 } else { id as i32 })
}

pub(super) async fn reply<T>(receiver: oneshot::Receiver<Result<T, Error>>) -> Result<T, Error> {
    timeout(DEADLINE, receiver)
        .await
        .map_err(|_| Error::RequestTimedOut)?
        .map_err(|_| Error::Storage)?
}

async fn write_result(write: Write, deadline: Instant) -> Result<i64, Error> {
    let answer = async {
        match write {
            Write::Existing(offset, barrier) => {
                barrier.await.map_err(|_| Error::Storage)??;
                Ok(offset)
            }
            Write::Pending(receiver) => receiver.await.map_err(|_| Error::Storage)?,
        }
    };
    timeout_at(deadline, answer)
        .await
        .map_err(|_| Error::RequestTimedOut)?
}

async fn produce_result(admission: oneshot::Receiver<Result<Write, Error>>) -> Result<i64, Error> {
    let deadline = Instant::now() + DEADLINE;
    let write = timeout_at(deadline, admission)
        .await
        .map_err(|_| Error::RequestTimedOut)?
        .map_err(|_| Error::Storage)??;
    write_result(write, deadline).await
}

type ProduceAdmissionReply = oneshot::Sender<Result<Write, Error>>;
type PendingAdmission = (usize, usize, oneshot::Receiver<Result<Write, Error>>);

pub(super) struct ProduceAdmission {
    topic: TopicName,
    partition: i32,
    input: ProduceBatch,
    reply: ProduceAdmissionReply,
    queued: Option<std::time::Instant>,
}

pub(super) async fn admit_produce(
    cluster: Weak<Cluster>,
    mut receiver: mpsc::Receiver<ProduceAdmission>,
    group_commit_wait: Duration,
) {
    while let Some(first) = receiver.recv().await {
        if !group_commit_wait.is_zero() && !receiver.is_empty() {
            tokio::time::sleep(group_commit_wait).await;
        }
        let Some(cluster) = cluster.upgrade() else {
            break;
        };
        if !cluster.page_cache {
            let routed = cluster
                .call_unfenced(move |engine| {
                    let mut groups = BTreeMap::<_, Vec<_>>::new();
                    for admission in std::iter::once(first)
                        .chain(std::iter::from_fn(|| receiver.try_recv().ok()))
                        .take(64)
                    {
                        match engine.topic_key(admission.topic.0.as_str(), admission.partition) {
                            Ok(key) => groups.entry(key).or_default().push(admission),
                            Err(error) => {
                                let _ = admission.reply.send(Err(error));
                            }
                        }
                    }
                    Ok((receiver, groups))
                })
                .await;
            let (next, groups) = match routed {
                Ok(routed) => routed,
                Err(error) => {
                    eprintln!("Produce routing stopped: {error:#}");
                    break;
                }
            };
            receiver = next;
            for (key, group) in groups {
                if let Err(error) = cluster.executor.submit_produce(&cluster, key, group).await {
                    eprintln!("Produce enqueue failed: {error:#}");
                }
            }
            continue;
        }
        let submitted = cluster
            .call(move |engine| {
                let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
                // Take existing work once the engine is available. An idle partition pays no linger.
                for admission in std::iter::once(first)
                    .chain(std::iter::from_fn(|| receiver.try_recv().ok()))
                    .take(32)
                {
                    ack_profile::finish("produce_worker_queue", admission.queued);
                    match engine.topic_key(admission.topic.0.as_str(), admission.partition) {
                        Ok(key) => groups
                            .entry(key)
                            .or_default()
                            .push((admission.input, admission.reply)),
                        Err(error) => {
                            let _ = admission.reply.send(Err(error));
                        }
                    }
                }
                for (key, group) in groups {
                    let (inputs, replies): (Vec<_>, Vec<_>) = group.into_iter().unzip();
                    for (reply, write) in
                        replies.into_iter().zip(engine.produce_batch(key, inputs)?)
                    {
                        let _ = reply.send(write);
                    }
                }
                Ok(receiver)
            })
            .await;
        match submitted {
            Ok(next) => receiver = next,
            Err(error) => {
                eprintln!("Produce admission stopped: {error:#}");
                break;
            }
        }
    }
}

fn complete_admissions(
    group: Vec<ProduceAdmission>,
    produce: impl FnOnce(Vec<ProduceBatch>) -> Result<Vec<Result<Write, Error>>>,
) -> Result<()> {
    let (inputs, replies): (Vec<_>, Vec<_>) = group
        .into_iter()
        .map(|entry| {
            ack_profile::finish("produce_worker_queue", entry.queued);
            (entry.input, entry.reply)
        })
        .unzip();
    for (reply, result) in replies.into_iter().zip(produce(inputs)?) {
        let _ = reply.send(result);
    }
    Ok(())
}

pub(crate) struct ProduceReply {
    response: messages::ProduceResponse,
    writes: Vec<PendingAdmission>,
    started: Option<std::time::Instant>,
}

fn record_produce_result(result: &mut PartitionProduceResponse, outcome: Result<i64, Error>) {
    match outcome {
        Ok(offset) => {
            result.base_offset = offset;
            result.log_start_offset = 0;
        }
        Err(error) => result.error_code = error as i16,
    }
}

impl ProduceReply {
    pub(crate) async fn finish(mut self, head: &Header) -> Result<Bytes> {
        let mut pending: FuturesUnordered<_> = self
            .writes
            .into_iter()
            .map(|(topic, partition, admission)| async move {
                (topic, partition, produce_result(admission).await)
            })
            .collect();
        while let Some((topic, partition, outcome)) = pending.next().await {
            record_produce_result(
                &mut self.response.responses[topic].partition_responses[partition],
                outcome,
            );
        }
        let response = encode(head, head.version, &self.response)?;
        ack_profile::finish("produce_request", self.started);
        Ok(response)
    }
}

type ReadBarrier = oneshot::Receiver<Result<(), Error>>;
type ReadAuthorization = (GroupId, i32, ReadBarrier);

type ReadAdmissionResult = Result<Result<ReadAuthorization, Error>>;
type ReadAdmissionReply = oneshot::Sender<ReadAdmissionResult>;

pub(super) struct FetchReadAdmission {
    expected: i32,
    reply: ReadAdmissionReply,
}

// All admitted requests were queued before the quorum probe is sent.
fn authorize_fetch_reads(
    batch: Vec<FetchReadAdmission>,
    node: Option<&Node>,
) -> Vec<(i32, ReadAdmissionReply)> {
    let mut accepted = Vec::new();
    for admission in batch.into_iter().filter(|item| !item.reply.is_closed()) {
        match node
            .ok_or(Error::NotLeader)
            .and_then(|node| check_read_epoch(node, admission.expected))
        {
            Ok(epoch) => accepted.push((epoch, admission.reply)),
            Err(error) => {
                let _ = admission.reply.send(Ok(Err(error)));
            }
        }
    }
    accepted
}

fn complete_fetch_reads(
    key: GroupId,
    mut accepted: Vec<(i32, ReadAdmissionReply)>,
    read: impl FnOnce() -> Result<Result<ReadBarrier, Error>>,
) -> Result<()> {
    if accepted.is_empty() {
        return Ok(());
    }
    let barrier = match read()? {
        Ok(barrier) => barrier,
        Err(error) => {
            for (_, sender) in accepted {
                let _ = sender.send(Ok(Err(error)));
            }
            return Ok(());
        }
    };
    if accepted.len() == 1 {
        let (epoch, sender) = accepted.pop().expect("one Fetch admission");
        let _ = sender.send(Ok(Ok((key, epoch, barrier))));
    } else {
        let replies: Vec<_> = accepted
            .into_iter()
            .filter_map(|(epoch, sender)| {
                let (reply, receiver) = oneshot::channel();
                sender
                    .send(Ok(Ok((key, epoch, receiver))))
                    .is_ok()
                    .then_some(reply)
            })
            .collect();
        tokio::spawn(async move {
            let outcome = reply(barrier).await;
            for reply in replies {
                let _ = reply.send(outcome);
            }
        });
    }
    Ok(())
}

async fn confirm_fetch_read(
    authorization: Result<ReadAuthorization, Error>,
    deadline: Instant,
) -> Result<(GroupId, i32), Error> {
    let (key, epoch, receiver) = authorization?;
    timeout_at(deadline, reply(receiver))
        .await
        .map_err(|_| Error::RequestTimedOut)??;
    Ok((key, epoch))
}

fn read_key(
    engine: &Engine,
    name: Option<&str>,
    id: Uuid,
    partition: i32,
) -> Result<GroupId, Error> {
    let name = match name {
        Some(name) => name,
        None => engine.names.get(&id).ok_or(Error::UnknownTopicId)?.as_str(),
    };
    engine.topic_key(name, partition)
}

fn check_read_epoch(node: &Node, expected: i32) -> Result<i32, Error> {
    let epoch = node.epoch();
    if !node.local_leader() {
        return Err(Error::NotLeader);
    }
    if expected > epoch {
        return Err(Error::UnknownLeaderEpoch);
    }
    if expected >= 0 && expected < epoch {
        return Err(Error::FencedLeaderEpoch);
    }
    Ok(epoch)
}

fn authorize_key(
    engine: &mut Engine,
    key: GroupId,
    expected: i32,
) -> Result<Result<ReadAuthorization, Error>> {
    let Some(node) = engine.nodes.get(&key) else {
        return Ok(Err(Error::NotLeader));
    };
    let epoch = match check_read_epoch(node, expected) {
        Ok(epoch) => epoch,
        Err(error) => return Ok(Err(error)),
    };
    engine
        .read(key)
        .map(|result| result.map(|receiver| (key, epoch, receiver)))
}

fn authorize_read(
    engine: &mut Engine,
    name: Option<&str>,
    id: Uuid,
    partition: i32,
    epoch: i32,
) -> Result<Result<ReadAuthorization, Error>> {
    match read_key(engine, name, id, partition) {
        Ok(key) => authorize_key(engine, key, epoch),
        Err(error) => Ok(Err(error)),
    }
}

fn read_at_epoch<T>(
    node: &Node,
    epoch: i32,
    read: impl FnOnce(&Node) -> Result<T, Error>,
) -> Result<T, Error> {
    if !node.local_leader() || node.epoch() != epoch {
        return Err(Error::NotLeader);
    }
    read(node)
}

#[derive(Clone, Copy)]
struct FetchRead {
    offset: i64,
    limit: i32,
    remaining: usize,
    first: bool,
}

struct FetchRecords {
    start: i64,
    end: i64,
    bytes: Bytes,
    batch_ends: Vec<usize>,
    error: Option<Error>,
}

impl FetchRecords {
    fn fit(&mut self, remaining: usize) {
        if self.bytes.len() > remaining {
            let count = self.batch_ends.partition_point(|&end| end <= remaining);
            self.bytes
                .truncate(self.batch_ends[..count].last().copied().unwrap_or(0));
        }
    }
}

fn fetch_partition(node: &Node, input: FetchRead) -> Result<FetchRecords, Error> {
    let log = node.log();
    let start = node.log_start();
    let end = log.committed().next_offset;
    let mut result = FetchRecords {
        start,
        end,
        bytes: Bytes::new(),
        batch_ends: Vec::new(),
        error: None,
    };
    if !(start..=end).contains(&input.offset) {
        result.error = Some(Error::OffsetOutOfRange);
        return Ok(result);
    }
    if input.limit < 0 {
        return Err(Error::InvalidRequest);
    }
    if input.remaining == 0 && !input.first {
        return Ok(result);
    }
    let mut records = BytesMut::new();
    log.visit_batches(input.offset, &mut Vec::new(), |_, bytes| {
        if (!input.first || !records.is_empty()) && records.len() + bytes.len() > input.remaining {
            return Ok(false);
        }
        records.extend_from_slice(bytes);
        result.batch_ends.push(records.len());
        Ok(records.len() < input.remaining)
    })
    .map_err(Error::storage)?;
    result.bytes = records.freeze();
    Ok(result)
}

fn fetch_response(
    partition: i32,
    result: Result<FetchRecords, Error>,
    used: &mut usize,
) -> messages::fetch_response::PartitionData {
    use messages::fetch_response::PartitionData;
    let mut response = PartitionData::default().with_partition_index(partition);
    match result {
        Ok(records) => {
            response.error_code = code(records.error);
            response.log_start_offset = records.start;
            response.high_watermark = records.end;
            response.last_stable_offset = records.end;
            *used += records.bytes.len();
            response.records = Some(records.bytes);
        }
        // Kafka consumers retry when a quorum cannot yet confirm this offset.
        Err(Error::RequestTimedOut) => response.error_code = Error::OffsetNotAvailable as i16,
        Err(error) => response.error_code = error as i16,
    }
    response
}

impl Cluster {
    pub async fn request(
        self: &Arc<Self>,
        head: &Header,
        request: Request,
        internal: bool,
        client_host: String,
    ) -> Result<Option<Bytes>> {
        let version = head.version;
        let response = match request {
            Request::ApiVersions(_) | Request::UnsupportedApiVersions => {
                let unsupported = matches!(request, Request::UnsupportedApiVersions);
                let versions = api_versions(if unsupported {
                    Error::UnsupportedVersion as i16
                } else {
                    0
                });
                encode(head, if unsupported { 0 } else { version }, &versions)?
            }
            Request::CreateTopics(request) => {
                let response =
                    topic_admin::create_response(self.create_topics(request).await?, version);
                encode(head, version, &response)?
            }
            Request::DeleteTopics(request) => {
                encode(head, version, &self.delete_topics(request, version).await?)?
            }
            Request::DescribeConfigs(request) => encode(
                head,
                version,
                &self.describe_configs(request, version).await?,
            )?,
            Request::IncrementalAlterConfigs(request) => {
                encode(head, version, &self.alter_configs(request).await?)?
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
                    let producer = self
                        .call(|engine| {
                            if engine.voters().contains(&engine.node_id()) {
                                engine.allocate_producer().map(Some)
                            } else {
                                Ok(None)
                            }
                        })
                        .await?;
                    if let Some(id) = producer {
                        response.producer_id = ProducerId(id);
                        response.producer_epoch = 0;
                    } else {
                        response.error_code = Error::BrokerNotAvailable as i16;
                    }
                } else {
                    response.error_code = Error::UnsupportedVersion as i16;
                }
                encode(head, version, &response)?
            }
            Request::Produce(request) => self.stage_produce(request).await?.finish(head).await?,
            Request::Fetch(request) => encode(head, version, &self.fetch(request, version).await?)?,
            Request::ListOffsets(request) => {
                encode(head, version, &self.list_offsets(request).await?)?
            }
            Request::OffsetForLeaderEpoch(request) => {
                encode(head, version, &self.epoch_offsets(request).await?)?
            }
            Request::FindCoordinator(request) => encode(
                head,
                version,
                &self.find_coordinator(request, version, internal).await?,
            )?,
            Request::JoinGroup(request) => {
                let response = self
                    .join_group(request, head.client_id.clone(), client_host)
                    .await?;
                encode(
                    head,
                    version,
                    &group_protocol::join_response(response, version),
                )?
            }
            Request::SyncGroup(request) => {
                let response =
                    group_protocol::sync_response(self.sync_group(request).await?, version);
                encode(head, version, &response)?
            }
            Request::Heartbeat(request) => encode(head, version, &self.heartbeat(request).await?)?,
            Request::LeaveGroup(request) => {
                encode(head, version, &self.leave_group(request, version).await?)?
            }
            Request::OffsetCommit(request) => {
                encode(head, version, &self.offset_commit(request).await?)?
            }
            Request::OffsetFetch(request) => {
                encode(head, version, &self.offset_fetch(request, version).await?)?
            }
            Request::DescribeGroups(request) => encode(
                head,
                version,
                &self.describe_groups(request, version).await?,
            )?,
            Request::ListGroups(request) => {
                encode(head, version, &self.list_groups(request).await?)?
            }
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
                        let cfg = topic_config(
                            &topic.with_replication_factor(1),
                            engine.cap().saturating_sub(engine.partitions()),
                        )?;
                        if cfg.allow_weak_acks {
                            return Err(Error::InvalidConfig);
                        }
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

    pub(crate) async fn stage_produce(
        self: &Arc<Self>,
        request: messages::ProduceRequest,
    ) -> Result<ProduceReply> {
        use messages::produce_response::TopicProduceResponse;
        ensure!(request.acks != 0, "cluster requires acknowledged writes");
        let started = ack_profile::start();
        let request_error = if request.transactional_id.is_some() {
            Some(Error::UnsupportedVersion)
        } else if request.acks != -1 {
            Some(Error::InvalidRequiredAcks)
        } else {
            None
        };
        let mut responses = Vec::new();
        let mut writes = HashMap::<_, Vec<_>>::new();
        for topic in request.topic_data {
            let topic_slot = responses.len();
            let mut partitions = Vec::new();
            for part in topic.partition_data {
                let partition_slot = partitions.len();
                let mut result = PartitionProduceResponse::default().with_index(part.index);
                if let Some(error) = request_error {
                    result.error_code = error as i16;
                } else if let Some(records) = part.records {
                    writes
                        .entry((topic.name.clone(), part.index))
                        .or_default()
                        .push((topic_slot, partition_slot, records));
                } else {
                    result.error_code = Error::InvalidRecord as i16;
                }
                partitions.push(result);
            }
            responses.push(
                TopicProduceResponse::default()
                    .with_name(topic.name)
                    .with_partition_responses(partitions),
            );
        }

        // A repeated partition only waits for its own previous write.
        let mut completions: FuturesUnordered<_> = writes
            .into_iter()
            .map(|((name, index), writes)| async move {
                let mut outcomes = Vec::with_capacity(writes.len());
                let mut writes = writes.into_iter().peekable();
                let mut tail = None;
                while let Some((topic_slot, partition_slot, records)) = writes.next() {
                    let input =
                        if records.len() <= 64 * 1024 && !Batch::needs_decompression(&records) {
                            ProduceBatch::inspect(records)
                        } else {
                            let slot = self.batch_checks.clone().acquire_owned().await?;
                            tokio::task::spawn_blocking(move || {
                                let _slot = slot;
                                ProduceBatch::inspect(records)
                            })
                            .await?
                        };
                    let (reply, admitted) = oneshot::channel();
                    self.produce_admissions
                        .send(ProduceAdmission {
                            topic: name.clone(),
                            partition: index,
                            input,
                            reply,
                            queued: ack_profile::start(),
                        })
                        .await?;
                    if writes.peek().is_none() {
                        // Queue the final batch before reading the next Produce request.
                        // The connection can pipeline while the partition waits for disk/consensus.
                        tail = Some((topic_slot, partition_slot, admitted));
                        break;
                    }
                    let outcome = produce_result(admitted).await;
                    outcomes.push((topic_slot, partition_slot, outcome));
                }
                Ok::<_, anyhow::Error>((outcomes, tail))
            })
            .collect();
        let mut pending = Vec::new();
        while let Some(result) = completions.next().await {
            let (outcomes, tail) = result?;
            pending.extend(tail);
            for (topic, partition, outcome) in outcomes {
                record_produce_result(
                    &mut responses[topic].partition_responses[partition],
                    outcome,
                );
            }
        }
        Ok(ProduceReply {
            response: messages::ProduceResponse::default().with_responses(responses),
            writes: pending,
            started,
        })
    }

    async fn guarded<T: Send + 'static>(
        self: &Arc<Self>,
        name: Option<String>,
        id: Uuid,
        partition: i32,
        expected_epoch: i32,
        read: impl FnOnce(&Node) -> Result<T, Error> + Send + 'static,
    ) -> Result<Result<T, Error>> {
        if !self.page_cache {
            let key = self
                .call_unfenced(move |engine| Ok(read_key(engine, name.as_deref(), id, partition)))
                .await?;
            return match key {
                Ok(key) => {
                    let authorization = self.submit_read(key, expected_epoch).await?.await??;
                    self.read_authorized(authorization, read).await
                }
                Err(error) => Ok(Err(error)),
            };
        }
        let authorization = self
            .call(move |engine| {
                authorize_read(engine, name.as_deref(), id, partition, expected_epoch)
            })
            .await?;
        self.read_authorized(authorization, read).await
    }

    async fn read_authorized<T: Send + 'static>(
        self: &Arc<Self>,
        authorization: Result<ReadAuthorization, Error>,
        read: impl FnOnce(&Node) -> Result<T, Error> + Send + 'static,
    ) -> Result<Result<T, Error>> {
        let (key, epoch, receiver) = match authorization {
            Ok(data) => data,
            Err(error) => return Ok(Err(error)),
        };
        if let Err(error) = reply(receiver).await {
            return Ok(Err(error));
        }
        if !self.page_cache {
            return self.read_partition(key, epoch, read).await;
        }
        self.call(move |engine| {
            Ok(engine
                .nodes
                .get(&key)
                .ok_or(Error::NotLeader)
                .and_then(|node| read_at_epoch(node, epoch, read)))
        })
        .await
    }

    async fn submit_read(
        self: &Arc<Self>,
        key: GroupId,
        expected: i32,
    ) -> Result<oneshot::Receiver<Result<Result<ReadAuthorization, Error>>>> {
        self.executor
            .submit(
                self,
                key,
                expected,
                move |partition, expected| {
                    let epoch = match check_read_epoch(&partition.node, expected) {
                        Ok(epoch) => epoch,
                        Err(error) => return Ok(Err(error)),
                    };
                    partition
                        .read()
                        .map(|result| result.map(|receiver| (key, epoch, receiver)))
                },
                move |engine, expected| authorize_key(engine, key, expected),
            )
            .await
    }

    pub(super) async fn run_produce_batch(
        self: &Arc<Self>,
        key: GroupId,
        disk: DiskSlot,
        batch: Vec<ProduceAdmission>,
    ) -> Result<()> {
        self.run_partition(
            key,
            disk,
            batch,
            |partition, batch| complete_admissions(batch, |inputs| partition.produce(inputs)),
            move |engine, batch| {
                complete_admissions(batch, |inputs| engine.produce_batch(key, inputs))
            },
        )
        .await
    }

    pub(super) fn try_fetch_reads(
        &self,
        key: GroupId,
        admissions: &mut Vec<FetchReadAdmission>,
    ) -> Option<Result<()>> {
        let mut slot = self.engine.try_lock().ok()?;
        let engine = slot.as_mut()?;
        let node = engine.local_partition(key)?;
        // With no Ready pending and the committed term cached, ReadIndex can
        // emit its quorum probe without a disk lookup or a persistence step.
        if !node.can_stage_read_in_memory() {
            return None;
        }
        let accepted = authorize_fetch_reads(std::mem::take(admissions), Some(node));
        let result = complete_fetch_reads(key, accepted, || engine.read(key));
        if result.is_err() {
            *slot = None;
        }
        Some(result)
    }

    pub(super) async fn run_fetch_reads(
        self: &Arc<Self>,
        key: GroupId,
        disk: DiskSlot,
        batch: Vec<FetchReadAdmission>,
    ) -> Result<()> {
        self.run_partition(
            key,
            disk,
            batch,
            move |partition, batch| {
                let accepted = authorize_fetch_reads(batch, Some(&partition.node));
                complete_fetch_reads(key, accepted, || partition.read())
            },
            move |engine, batch| {
                let accepted = authorize_fetch_reads(batch, engine.nodes.get(&key));
                complete_fetch_reads(key, accepted, || engine.read(key))
            },
        )
        .await
    }

    async fn read_partition<T: Send + 'static>(
        self: &Arc<Self>,
        key: GroupId,
        epoch: i32,
        read: impl FnOnce(&Node) -> Result<T, Error> + Send + 'static,
    ) -> Result<Result<T, Error>> {
        self.executor
            .submit(
                self,
                key,
                read,
                move |partition, read| Ok(read_at_epoch(&partition.node, epoch, read)),
                move |engine, read| {
                    Ok(engine
                        .nodes
                        .get(&key)
                        .ok_or(Error::NotLeader)
                        .and_then(|node| read_at_epoch(node, epoch, read)))
                },
            )
            .await?
            .await?
    }

    async fn fetch(
        self: &Arc<Self>,
        request: messages::FetchRequest,
        version: i16,
    ) -> Result<messages::FetchResponse> {
        if request.session_id != 0 {
            return Ok(messages::FetchResponse::default()
                .with_error_code(Error::FetchSessionNotFound as i16));
        }
        if request.replica_id.0 != -1
            || ![0, 1].contains(&request.isolation_level)
            || request.max_bytes < 0
            || request.min_bytes < 0
            || request.max_wait_ms < 0
        {
            return Ok(
                messages::FetchResponse::default().with_error_code(Error::InvalidRequest as i16)
            );
        }
        let minimum = (request.min_bytes as usize).min(FETCH_MAX);
        let deadline =
            Instant::now() + Duration::from_millis(request.max_wait_ms.min(2_000) as u64);
        let wait_slot = self.fetch_waiters.try_acquire().ok();
        loop {
            let notified = self.data_available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (response, used, failed) = self
                .fetch_once(&request, version, deadline + DEADLINE)
                .await?;
            if failed || used >= minimum || wait_slot.is_none() || Instant::now() >= deadline {
                return Ok(response);
            }
            drop(response);
            let _ = timeout_at(deadline, notified).await;
        }
    }

    async fn fetch_once(
        self: &Arc<Self>,
        request: &messages::FetchRequest,
        version: i16,
        deadline: Instant,
    ) -> Result<(messages::FetchResponse, usize, bool)> {
        let maximum = (request.max_bytes as usize).min(FETCH_MAX);
        let targets: Vec<_> = request
            .topics
            .iter()
            .flat_map(|topic| {
                topic.partitions.iter().map(move |part| {
                    let name = (version == 12).then(|| topic.topic.0.clone());
                    (
                        name,
                        topic.topic_id,
                        part.partition,
                        part.current_leader_epoch,
                    )
                })
            })
            .collect();
        let count = targets.len();
        // Start the quorum barriers together, then spend the byte budget in wire order.
        let authorizations = if self.page_cache {
            match timeout_at(
                deadline,
                self.call(move |engine| {
                    targets
                        .into_iter()
                        .map(|(name, id, partition, epoch)| {
                            if Instant::now() >= deadline {
                                return Ok(Err(Error::RequestTimedOut));
                            }
                            authorize_read(
                                engine,
                                name.as_ref().map(StrBytes::as_str),
                                id,
                                partition,
                                epoch,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                }),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => (0..count).map(|_| Err(Error::RequestTimedOut)).collect(),
            }
        } else {
            let resolved = self
                .call_unfenced(move |engine| {
                    Ok(targets
                        .into_iter()
                        .map(|(name, id, part, epoch)| {
                            read_key(engine, name.as_ref().map(StrBytes::as_str), id, part)
                                .map(|key| (key, epoch))
                        })
                        .collect::<Vec<_>>())
                })
                .await?;
            let pending: Vec<_> = stream::iter(resolved)
                .map(|target| async move {
                    let (key, expected) = match target {
                        Ok(target) => target,
                        Err(error) => return Ok(Err(error)),
                    };
                    let (reply, receiver) = oneshot::channel();
                    match timeout_at(
                        deadline,
                        self.executor.submit_fetch_read(
                            self,
                            key,
                            FetchReadAdmission { expected, reply },
                        ),
                    )
                    .await
                    {
                        Ok(result) => {
                            result?;
                            Ok(Ok(receiver))
                        }
                        Err(_) => Ok(Err(Error::RequestTimedOut)),
                    }
                })
                .buffered(FETCH_READ_CONCURRENCY)
                .collect::<Vec<Result<_>>>()
                .await;
            let mut authorizations = Vec::with_capacity(count);
            for item in pending {
                authorizations.push(match item? {
                    Ok(reply) => match timeout_at(deadline, reply).await {
                        Ok(Ok(result)) => result?,
                        Ok(Err(_)) => Err(Error::Storage),
                        Err(_) => Err(Error::RequestTimedOut),
                    },
                    Err(error) => Err(error),
                });
            }
            authorizations
        };
        let topics = request.topics.clone();
        if !self.page_cache {
            return self
                .materialize_detached_fetch(topics, authorizations, maximum, deadline)
                .await;
        }
        let mut confirmed = Vec::with_capacity(count);
        for authorization in authorizations {
            confirmed.push(confirm_fetch_read(authorization, deadline).await);
        }
        self.call(move |engine| {
            Ok(Self::materialize_fetch(
                engine, topics, confirmed, maximum, deadline,
            ))
        })
        .await
    }

    async fn materialize_detached_fetch(
        self: &Arc<Self>,
        topics: Vec<messages::fetch_request::FetchTopic>,
        authorizations: Vec<Result<ReadAuthorization, Error>>,
        maximum: usize,
        deadline: Instant,
    ) -> Result<(messages::FetchResponse, usize, bool)> {
        use messages::fetch_response::FetchableTopicResponse;
        let mut failed = false;
        let mut used = 0;
        let mut responses: Vec<_> = topics
            .iter()
            .map(|topic| {
                FetchableTopicResponse::default()
                    .with_topic(topic.topic.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(Vec::with_capacity(topic.partitions.len()))
            })
            .collect();
        let mut targets = topics
            .iter()
            .enumerate()
            .flat_map(|(index, topic)| {
                topic.partitions.iter().map(move |part| {
                    (
                        index,
                        part.partition,
                        part.fetch_offset,
                        part.partition_max_bytes,
                    )
                })
            })
            .zip(authorizations)
            .collect::<Vec<_>>()
            .into_iter()
            .peekable();
        while targets.peek().is_some() {
            let remaining = maximum.saturating_sub(used);
            let mut reads = stream::iter(targets.by_ref().take(FETCH_READ_CONCURRENCY))
                .map(
                    |((index, partition, offset, limit), authorization)| async move {
                        let input = FetchRead {
                            offset,
                            limit,
                            remaining: remaining.min(limit.max(0) as usize),
                            first: false,
                        };
                        let authorization = confirm_fetch_read(authorization, deadline).await;
                        let result = self
                            .fetch_detached_partition(authorization, input, deadline)
                            .await;
                        (index, partition, authorization, input, result)
                    },
                )
                .buffered(FETCH_READ_CONCURRENCY);
            while let Some((index, partition, authorization, mut input, result)) =
                reads.next().await
            {
                let mut result = result?;
                if let Ok(records) = &mut result {
                    if used == 0
                        && records.bytes.is_empty()
                        && records.error.is_none()
                        && input.offset < records.end
                    {
                        // Kafka permits one oversized batch, from the first partition with data.
                        input.first = true;
                        result = self
                            .fetch_detached_partition(authorization, input, deadline)
                            .await?;
                    } else {
                        records.fit(maximum.saturating_sub(used));
                    }
                }
                let response = fetch_response(partition, result, &mut used);
                failed |= response.error_code != 0;
                responses[index].partitions.push(response);
            }
        }
        Ok((
            messages::FetchResponse::default().with_responses(responses),
            used,
            failed,
        ))
    }

    async fn fetch_detached_partition(
        self: &Arc<Self>,
        authorization: Result<(GroupId, i32), Error>,
        input: FetchRead,
        deadline: Instant,
    ) -> Result<Result<FetchRecords, Error>> {
        let (key, epoch) = match authorization {
            Ok(authorization) => authorization,
            Err(error) => return Ok(Err(error)),
        };
        if Instant::now() >= deadline {
            return Ok(Err(Error::RequestTimedOut));
        }
        // The quorum barrier has already completed. A bounded cache hit can
        // be served while holding the registry lock, with no disk admission.
        if let Ok(slot) = self.engine.try_lock()
            && let Some(node) = slot.as_ref().and_then(|engine| engine.local_partition(key))
        {
            let log = node.log();
            if input.offset < node.log_start()
                || input.offset >= log.committed().next_offset
                || (!input.first && input.remaining == 0)
                || log
                    .recent_batch_bytes(input.offset)
                    .is_some_and(|bytes| bytes <= INLINE_FETCH_BYTES)
            {
                return Ok(read_at_epoch(node, epoch, |node| {
                    fetch_partition(node, input)
                }));
            }
        }
        match timeout_at(
            deadline,
            self.read_partition(key, epoch, move |node| fetch_partition(node, input)),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(Err(Error::RequestTimedOut)),
        }
    }

    fn materialize_fetch(
        engine: &Engine,
        topics: Vec<messages::fetch_request::FetchTopic>,
        confirmed: Vec<Result<(GroupId, i32), Error>>,
        maximum: usize,
        deadline: Instant,
    ) -> (messages::FetchResponse, usize, bool) {
        use messages::fetch_response::FetchableTopicResponse;
        let mut failed = false;
        let mut used = 0;
        let mut responses = Vec::new();
        let mut confirmed = confirmed.into_iter();
        for topic in topics {
            let mut partitions = Vec::new();
            for part in topic.partitions {
                let remaining = maximum
                    .saturating_sub(used)
                    .min(part.partition_max_bytes.max(0) as usize);
                let first = used == 0;
                let authorization = confirmed.next().expect("one barrier per Fetch partition");
                let result = authorization.and_then(|(key, epoch)| {
                    if Instant::now() >= deadline {
                        return Err(Error::RequestTimedOut);
                    }
                    let node = engine
                        .nodes
                        .get(&key)
                        .filter(|node| node.local_leader() && node.epoch() == epoch)
                        .ok_or(Error::NotLeader)?;
                    fetch_partition(
                        node,
                        FetchRead {
                            offset: part.fetch_offset,
                            limit: part.partition_max_bytes,
                            remaining,
                            first,
                        },
                    )
                });
                let response = fetch_response(part.partition, result, &mut used);
                failed |= response.error_code != 0;
                partitions.push(response);
            }
            responses.push(
                FetchableTopicResponse::default()
                    .with_topic(topic.topic.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            );
        }
        (
            messages::FetchResponse::default().with_responses(responses),
            used,
            failed,
        )
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
                            lookup_from(node.log(), node.log_start(), part.timestamp).map(|value| {
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
                            let physical_start = log.start().next_offset;
                            let previous_epoch = node
                                .compacted_data_epoch()
                                .filter(|&epoch| epoch <= part.leader_epoch)
                                .unwrap_or(-1);
                            let mut value = (previous_epoch, physical_start);
                            log.visit_batches(physical_start, &mut Vec::new(), |entry, bytes| {
                                let epoch = super::disk::batch_epoch(bytes);
                                if epoch > part.leader_epoch {
                                    return Ok(false);
                                }
                                value = (
                                    epoch,
                                    entry.position.next_offset + i64::from(entry.record_count),
                                );
                                Ok(true)
                            })
                            .map_err(Error::storage)?;
                            Ok((value.0, value.1.max(node.log_start())))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_budget_only_keeps_complete_batches() {
        let mut records = FetchRecords {
            start: 0,
            end: 3,
            bytes: Bytes::from_static(b"aaabbbbcc"),
            batch_ends: vec![3, 7, 9],
            error: None,
        };
        records.fit(8);
        assert_eq!(records.bytes, Bytes::from_static(b"aaabbbb"));
        records.fit(2);
        assert!(records.bytes.is_empty());
    }
}
