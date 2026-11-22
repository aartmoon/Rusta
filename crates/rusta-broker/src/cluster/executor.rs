//! Bounded FIFO execution for user-data Raft partitions.
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::sync::{RwLock, Semaphore, mpsc, oneshot};

use super::{
    Cluster, ack_profile,
    engine::{Engine, GroupId, PartitionWork},
};

const MAX_QUEUES: usize = 256;
const QUEUE_DEPTH: usize = 32;
const DISK_WORKERS: usize = 4;

type Job = Pin<Box<dyn Future<Output = ()> + Send>>;
type Queues = Arc<Mutex<HashMap<GroupId, mpsc::Sender<Job>>>>;

pub(super) struct Executor {
    // Topology changes wait until all leased nodes are back in the registry.
    pub(super) topology: Arc<RwLock<()>>,
    disk: Arc<Semaphore>,
    queues: Queues,
    pending_ticks: Mutex<HashSet<GroupId>>,
}

impl Executor {
    pub(super) fn new() -> Self {
        Self {
            topology: Arc::new(RwLock::new(())),
            disk: Arc::new(Semaphore::new(DISK_WORKERS)),
            queues: Arc::new(Mutex::new(HashMap::new())),
            pending_ticks: Mutex::new(HashSet::new()),
        }
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
            loop {
                let received = tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await;
                match received {
                    Ok(Some(job)) => job.await,
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
        let job = Box::pin(async move {
            if let Some(cluster) = cluster.upgrade() {
                let _ = reply.send(cluster.run_partition(key, input, work, fallback).await);
            }
        });
        self.queue(key)?
            .send(job)
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
            .try_send(Box::pin(async move {
                let Some(cluster) = cluster.upgrade() else {
                    return;
                };
                if let Err(error) = cluster
                    .run_partition(
                        key,
                        (),
                        |job, ()| job.tick(),
                        move |engine, ()| engine.tick_partition(key),
                    )
                    .await
                {
                    eprintln!("data partition timer stopped: {error:#}");
                }
                cluster
                    .executor
                    .pending_ticks
                    .lock()
                    .expect("partition timer registry")
                    .remove(&key);
            }))
            .is_err()
        {
            pending.remove(&key);
        }
    }
}

impl Cluster {
    async fn run_partition<I, T>(
        self: &Arc<Self>,
        key: GroupId,
        input: I,
        work: impl FnOnce(&mut PartitionWork, I) -> Result<T> + Send + 'static,
        fallback: impl FnOnce(&mut Engine, I) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        let queued = ack_profile::start();
        let disk = self.executor.disk.clone().acquire_owned().await?;
        ack_profile::finish("partition_disk_worker_wait", queued);
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
        let executor = Executor::new();
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
            .send(Box::pin(async move {
                let _ = started.send(());
                let _ = blocked.await;
            }))
            .await
            .ok()
            .context("enqueue blocking job")?;
        tokio::time::timeout(Duration::from_secs(1), start).await??;
        first
            .send(Box::pin(async move {
                let _ = same_partition.send(());
            }))
            .await
            .ok()
            .context("enqueue same partition")?;
        second
            .send(Box::pin(async move {
                let _ = other_partition.send(());
            }))
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
