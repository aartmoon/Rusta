//! Bounded FIFO execution for user-data Raft partitions.
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, TryLockError, Weak};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use raft::eraftpb::{Message, MessageType};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore, mpsc, oneshot};

use super::{
    Cluster, ack_profile,
    engine::{Engine, GroupId, PartitionWork},
    kafka::{FetchReadAdmission, ProduceAdmission},
};

const MAX_QUEUES: usize = 256;
const QUEUE_DEPTH: usize = 32;
pub(super) const DEFAULT_DISK_WORKERS: usize = 8;
const RAFT_BATCH_LIMIT: usize = 8;
const PRODUCE_BATCH_LIMIT: usize = 64;

enum Job {
    Work(Pin<Box<dyn Future<Output = ()> + Send>>),
    Produce {
        cluster: Weak<Cluster>,
        admissions: Vec<ProduceAdmission>,
    },
    FetchRead {
        cluster: Weak<Cluster>,
        admission: FetchReadAdmission,
    },
    Raft {
        cluster: Weak<Cluster>,
        admission: Box<RaftAdmission>,
    },
}

fn collect_fetch_reads(
    receiver: &mut mpsc::Receiver<Job>,
    next: &mut Option<Job>,
    batch: &mut Vec<FetchReadAdmission>,
) {
    while next.is_none() && batch.len() < QUEUE_DEPTH {
        match receiver.try_recv() {
            Ok(Job::FetchRead { admission, .. }) => batch.push(admission),
            Ok(job) => *next = Some(job),
            Err(_) => break,
        }
    }
}

struct RaftAdmission {
    message: Message,
    permit: OwnedSemaphorePermit,
    queued: Option<(&'static str, std::time::Instant)>,
    reply: oneshot::Sender<Result<()>>,
}

impl RaftAdmission {
    fn can_batch(&self, other: &Self) -> bool {
        let kind = self.message.get_msg_type();
        if kind != other.message.get_msg_type() || self.message.term != other.message.term {
            return false;
        }
        match kind {
            MessageType::MsgAppend => self.message.from == other.message.from,
            MessageType::MsgAppendResponse => !self.message.reject && !other.message.reject,
            _ => false,
        }
    }

    fn run(self, work: impl FnOnce(Message) -> Result<()>) -> Result<RaftCompletion> {
        let Self {
            message,
            permit,
            queued,
            reply,
        } = self;
        if let Some((stage, time)) = queued {
            ack_profile::record(stage, time.elapsed());
        }
        work(message)?;
        Ok(RaftCompletion {
            _permit: permit,
            reply,
        })
    }
}

struct RaftCompletion {
    _permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<()>>,
}
pub(super) struct DiskSlot {
    _worker: OwnedSemaphorePermit,
    _client: Option<OwnedSemaphorePermit>,
}

type Queues = Arc<Mutex<HashMap<GroupId, mpsc::Sender<Job>>>>;

pub(super) struct Executor {
    // Topology changes wait until all leased nodes are back in the registry.
    pub(super) topology: Arc<RwLock<()>>,
    disk: Arc<Semaphore>,
    client_disk: Arc<Semaphore>,
    queues: Queues,
    pending_ticks: Mutex<HashSet<GroupId>>,
}

impl Executor {
    pub(super) fn new(disk_workers: usize) -> Self {
        Self {
            topology: Arc::new(RwLock::new(())),
            disk: Arc::new(Semaphore::new(disk_workers)),
            client_disk: Arc::new(Semaphore::new(disk_workers - disk_workers / 4)),
            queues: Arc::new(Mutex::new(HashMap::new())),
            pending_ticks: Mutex::new(HashSet::new()),
        }
    }

    async fn reserve_disk(&self, replication: bool) -> Result<DiskSlot> {
        let queued = ack_profile::start();
        // Keep worker capacity available for the messages that advance consensus,
        // even when every partition has client work queued.
        let client = if replication {
            None
        } else {
            Some(self.client_disk.clone().acquire_owned().await?)
        };
        let worker = self.disk.clone().acquire_owned().await?;
        ack_profile::finish("partition_disk_worker_wait", queued);
        Ok(DiskSlot {
            _worker: worker,
            _client: client,
        })
    }

    fn queue(&self, key: GroupId) -> Result<mpsc::Sender<Job>> {
        let mut queues = self.queues.lock().expect("partition queue registry");
        if let Some(queue) = queues.get(&key) {
            return Ok(queue.clone());
        }
        ensure!(
            queues.len() < MAX_QUEUES,
            "data partition queue limit reached"
        );
        let (queue, mut receiver) = mpsc::channel::<Job>(QUEUE_DEPTH);
        queues.insert(key, queue.clone());
        let registry = Arc::downgrade(&self.queues);
        tokio::spawn(async move {
            let mut next = None;
            loop {
                let received = match next.take() {
                    Some(job) => Ok(Some(job)),
                    None => tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await,
                };
                match received {
                    Ok(Some(Job::Work(job))) => job.await,
                    Ok(Some(Job::Produce {
                        cluster,
                        mut admissions,
                    })) => {
                        let Some(cluster) = cluster.upgrade() else {
                            break;
                        };
                        let Ok(disk) = cluster.executor.reserve_disk(false).await else {
                            break;
                        };
                        // Merge writes that accumulated while waiting for a worker.
                        while admissions.len() < PRODUCE_BATCH_LIMIT {
                            match receiver.try_recv() {
                                Ok(Job::Produce {
                                    admissions: mut more,
                                    ..
                                }) if admissions.len() + more.len() <= PRODUCE_BATCH_LIMIT => {
                                    admissions.append(&mut more);
                                }
                                Ok(job) => {
                                    next = Some(job);
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        if let Err(error) = cluster.run_produce_batch(key, disk, admissions).await {
                            eprintln!("Produce partition admission failed: {error:#}");
                        }
                    }
                    Ok(Some(Job::FetchRead { cluster, admission })) => {
                        let Some(cluster) = cluster.upgrade() else {
                            break;
                        };
                        let mut batch = vec![admission];
                        collect_fetch_reads(&mut receiver, &mut next, &mut batch);
                        if let Some(result) = cluster.try_fetch_reads(key, &mut batch) {
                            if let Err(error) = result {
                                eprintln!("Fetch read admission failed: {error:#}");
                            }
                            continue;
                        }
                        let Ok(disk) = cluster.executor.reserve_disk(false).await else {
                            break;
                        };
                        collect_fetch_reads(&mut receiver, &mut next, &mut batch);
                        if let Err(error) = cluster.run_fetch_reads(key, disk, batch).await {
                            eprintln!("Fetch read admission failed: {error:#}");
                        }
                    }
                    Ok(Some(Job::Raft {
                        cluster,
                        mut admission,
                    })) => {
                        let Some(cluster) = cluster.upgrade() else {
                            break;
                        };
                        if let Some(result) = cluster.try_inline_raft(key, &mut admission.message) {
                            if let Some((stage, time)) = admission.queued {
                                ack_profile::record(stage, time.elapsed());
                            }
                            let _ = admission.reply.send(result);
                            continue;
                        }
                        let Ok(disk) = cluster.executor.reserve_disk(true).await else {
                            break;
                        };
                        let mut batch = vec![*admission];
                        while batch.len() < RAFT_BATCH_LIMIT {
                            match receiver.try_recv() {
                                Ok(Job::Raft { admission, .. })
                                    if batch[0].can_batch(&admission) =>
                                {
                                    batch.push(*admission);
                                }
                                Ok(job) => {
                                    next = Some(job);
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        cluster.run_raft_batch(key, disk, batch).await;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        let Some(registry) = registry.upgrade() else {
                            break;
                        };
                        let mut queues = registry.lock().expect("partition queue registry");
                        if receiver.is_empty() && queues[&key].strong_count() == 1 {
                            queues.remove(&key);
                            break;
                        }
                    }
                }
            }
        });
        Ok(queue)
    }

    pub(super) async fn submit<I, T>(
        &self,
        cluster: &Arc<Cluster>,
        key: GroupId,
        input: I,
        work: impl FnOnce(&mut PartitionWork, I) -> Result<T> + Send + 'static,
        fallback: impl FnOnce(&mut Engine, I) -> Result<T> + Send + 'static,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        let (reply, done) = oneshot::channel();
        let cluster = Arc::downgrade(cluster);
        let job = Job::Work(Box::pin(async move {
            if let Some(cluster) = cluster.upgrade() {
                if reply.is_closed() {
                    return;
                }
                let result = match cluster.executor.reserve_disk(false).await {
                    Ok(disk) if !reply.is_closed() => {
                        cluster
                            .run_partition(key, disk, input, work, fallback)
                            .await
                    }
                    Ok(_) => return,
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
        }));
        self.queue(key)?
            .send(job)
            .await
            .map_err(|_| anyhow::anyhow!("partition executor stopped"))?;
        Ok(done)
    }

    pub(super) async fn submit_produce(
        &self,
        cluster: &Arc<Cluster>,
        key: GroupId,
        admissions: Vec<ProduceAdmission>,
    ) -> Result<()> {
        self.queue(key)?
            .send(Job::Produce {
                cluster: Arc::downgrade(cluster),
                admissions,
            })
            .await
            .map_err(|_| anyhow::anyhow!("partition executor stopped"))
    }

    pub(super) async fn submit_fetch_read(
        &self,
        cluster: &Arc<Cluster>,
        key: GroupId,
        admission: FetchReadAdmission,
    ) -> Result<()> {
        self.queue(key)?
            .send(Job::FetchRead {
                cluster: Arc::downgrade(cluster),
                admission,
            })
            .await
            .map_err(|_| anyhow::anyhow!("partition executor stopped"))
    }

    pub(super) async fn submit_raft(
        &self,
        cluster: &Arc<Cluster>,
        key: GroupId,
        message: Message,
        permit: OwnedSemaphorePermit,
        queued: Option<(&'static str, std::time::Instant)>,
    ) -> Result<oneshot::Receiver<Result<()>>> {
        let (reply, done) = oneshot::channel();
        self.queue(key)?
            .send(Job::Raft {
                cluster: Arc::downgrade(cluster),
                admission: Box::new(RaftAdmission {
                    message,
                    permit,
                    queued,
                    reply,
                }),
            })
            .await
            .map_err(|_| anyhow::anyhow!("partition executor stopped"))?;
        Ok(done)
    }

    pub(super) fn tick(&self, cluster: &Arc<Cluster>, key: GroupId) {
        let Ok(queue) = self.queue(key) else { return };
        let mut pending = self.pending_ticks.lock().expect("partition timer registry");
        // Coalesce ticks while a partition is busy so it never runs a burst of stale ticks.
        if !pending.insert(key) {
            return;
        }
        let cluster = Arc::downgrade(cluster);
        if queue
            .try_send(Job::Work(Box::pin(async move {
                let Some(cluster) = cluster.upgrade() else {
                    return;
                };
                if let Err(error) = cluster.tick_partition(key).await {
                    eprintln!("data partition timer stopped: {error:#}");
                }
                cluster
                    .executor
                    .pending_ticks
                    .lock()
                    .expect("partition timer registry")
                    .remove(&key);
            })))
            .is_err()
        {
            pending.remove(&key);
        }
    }
}

impl Cluster {
    fn try_inline_raft(&self, key: GroupId, message: &mut Message) -> Option<Result<()>> {
        if !matches!(
            message.get_msg_type(),
            MessageType::MsgHeartbeat
                | MessageType::MsgHeartbeatResponse
                | MessageType::MsgAppendResponse
        ) {
            return None;
        }
        let mut slot = self.engine.try_lock().ok()?;
        let engine = slot.as_mut().filter(|engine| !engine.closing)?;
        match engine.step_inline_raft(key, message) {
            Ok(true) => Some(Ok(())),
            Ok(false) => None,
            Err(error) => {
                eprintln!("partition state failure: {error:#}");
                *slot = None;
                Some(Err(error))
            }
        }
    }

    async fn run_raft_batch(
        self: &Arc<Self>,
        key: GroupId,
        disk: DiskSlot,
        batch: Vec<RaftAdmission>,
    ) {
        let result = self
            .run_partition(
                key,
                disk,
                batch,
                |partition, batch| {
                    let replies = batch
                        .into_iter()
                        .map(|admission| admission.run(|message| partition.step(message)))
                        .collect::<Result<Vec<_>>>()?;
                    partition.drain_raft()?;
                    Ok(replies)
                },
                move |engine, batch| {
                    batch
                        .into_iter()
                        .map(|admission| admission.run(|message| engine.receive(key, message)))
                        .collect()
                },
            )
            .await;
        match result {
            Ok(replies) => {
                for complete in replies {
                    let _ = complete.reply.send(Ok(()));
                }
            }
            Err(error) => eprintln!("Raft partition admission failed: {error:#}"),
        }
    }

    async fn tick_partition(self: &Arc<Self>, key: GroupId) -> Result<()> {
        let ready = match self.engine.try_lock() {
            Ok(mut slot) => {
                let engine = slot
                    .as_mut()
                    .context("cluster is out of service after a state error")?;
                ensure!(!engine.closing, "cluster is shutting down");
                Some(engine.tick_partition_clock(key))
            }
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Poisoned(_)) => anyhow::bail!("cluster state lock poisoned"),
        };
        if ready != Some(false) {
            let disk = self.executor.reserve_disk(true).await?;
            self.run_partition(
                key,
                disk,
                ready.is_none(),
                |job, clock| job.tick_ready(clock),
                move |engine, clock| {
                    if clock {
                        engine.tick_partition_clock(key);
                    }
                    engine.drain_partition_ready(key)
                },
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn run_partition<I, T>(
        self: &Arc<Self>,
        key: GroupId,
        disk: DiskSlot,
        input: I,
        work: impl FnOnce(&mut PartitionWork, I) -> Result<T> + Send + 'static,
        fallback: impl FnOnce(&mut Engine, I) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        let queued = ack_profile::start();
        let topology = self.executor.topology.clone().read_owned().await;
        ack_profile::finish("partition_topology_wait", queued);
        let cluster = self.clone();
        // The blocking task owns the lease and both guards through reinsertion,
        // even when the requesting connection disappears.
        let result = tokio::task::spawn_blocking(move || {
            let leased = {
                let mut slot = cluster
                    .engine
                    .lock()
                    .map_err(|_| anyhow::anyhow!("cluster state lock poisoned"))?;
                let engine = slot
                    .as_mut()
                    .context("cluster is out of service after a state error")?;
                ensure!(!engine.closing, "cluster is shutting down");
                engine.lease_partition(key)
            };
            let Some(mut leased) = leased else {
                return Ok(Err(input));
            };
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(&mut leased, input)));
            let mut slot = cluster
                .engine
                .lock()
                .map_err(|_| anyhow::anyhow!("cluster state lock poisoned"))?;
            let output = match result {
                Ok(Ok(output)) => {
                    slot.as_mut()
                        .context("cluster is out of service after a state error")?
                        .restore_partition(leased);
                    Ok(Ok(output))
                }
                Ok(Err(error)) => {
                    eprintln!("partition state failure: {error:#}");
                    *slot = None;
                    Err(error)
                }
                Err(_) => {
                    *slot = None;
                    Err(anyhow::anyhow!("partition worker panicked"))
                }
            };
            drop(slot);
            drop(topology);
            drop(disk);
            output
        })
        .await??;
        match result {
            Ok(output) => Ok(output),
            Err(input) => self.call(move |engine| fallback(engine, input)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn blocked_partition_preserves_fifo_without_blocking_another_partition() -> Result<()> {
        let executor = Executor::new(DEFAULT_DISK_WORKERS);
        let topic = Uuid::new_v4();
        let first = executor.queue(GroupId {
            topic,
            partition: 0,
        })?;
        let second = executor.queue(GroupId {
            topic,
            partition: 1,
        })?;
        let (release, blocked) = oneshot::channel();
        let (started, start) = oneshot::channel();
        let (same_partition, mut same) = oneshot::channel();
        let (other_partition, other) = oneshot::channel();
        first
            .send(Job::Work(Box::pin(async move {
                let _ = started.send(());
                let _ = blocked.await;
            })))
            .await
            .ok()
            .context("enqueue blocking job")?;
        tokio::time::timeout(Duration::from_secs(1), start).await??;
        first
            .send(Job::Work(Box::pin(async move {
                let _ = same_partition.send(());
            })))
            .await
            .ok()
            .context("enqueue same partition")?;
        second
            .send(Job::Work(Box::pin(async move {
                let _ = other_partition.send(());
            })))
            .await
            .ok()
            .context("enqueue other partition")?;
        tokio::time::timeout(Duration::from_secs(1), other).await??;
        assert!(matches!(
            same.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).ok().context("release blocking job")?;
        tokio::time::timeout(Duration::from_secs(1), same).await??;
        Ok(())
    }
}
