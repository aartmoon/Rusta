mod ack_profile;
pub mod admin;
mod checkpoint;
mod disk;
mod engine;
mod executor;
mod groups;
mod kafka;
mod producers;
mod topics;
mod transport;

use std::collections::BTreeMap;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rusta_kafka::protocol::StrBytes;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore, mpsc};
use uuid::Uuid;

use crate::protocol::Endpoint;

use self::engine::Engine;
pub use self::engine::migration;
pub(crate) use self::kafka::ProduceReply;

#[derive(Clone)]
pub struct Settings {
    pub id: u64,
    pub cluster: Uuid,
    pub peers: BTreeMap<u64, Peer>,
    pub bootstrap_voters: Vec<u64>,
    page_cache: bool,
    cache_recovery: Option<Uuid>,
    staged_peers: BTreeMap<u64, [u8; 32]>,
    bind: SocketAddr,
    certificates: PathBuf,
}

#[derive(Clone)]
pub struct Peer {
    spec: String,
    raft: String,
    dns: String,
    pub host: Endpoint,
    pub internal: Endpoint,
}

const STAGED_PEERS: &str = "staged-peers.json";
const PAGE_CACHE_BOOT: &str = "page-cache-boot";
const PAGE_CACHE_RECOVERY: &str = "page-cache-recovering";
const MAX_INVENTORY_BYTES: u64 = 512 * 1024;

#[derive(Deserialize, Serialize)]
struct StagedPeer {
    spec: String,
    fingerprint: [u8; 32],
}

impl Peer {
    fn parse(spec: &str) -> Result<(u64, Self)> {
        ensure!(
            spec.len() <= 1024 && !spec.contains([';', '\n', '\r']),
            "invalid peer entry"
        );
        let fields: Vec<_> = spec.split(',').collect();
        ensure!(
            fields.len() == 4,
            "peer entries require id,raft-address,host-kafka,internal-kafka"
        );
        let member: u64 = fields[0].parse()?;
        ensure!(
            (1..=255).contains(&member),
            "node ID must be between 1 and 255"
        );
        let (name, port) = fields[1]
            .rsplit_once(':')
            .context("missing Raft peer port")?;
        ensure!(
            !name.is_empty() && port.parse::<u16>()? != 0,
            "invalid Raft peer address"
        );
        transport::validate_name(name)?;
        Ok((
            member,
            Self {
                spec: spec.to_owned(),
                raft: fields[1].to_owned(),
                dns: name.to_owned(),
                host: endpoint(fields[2])?,
                internal: endpoint(fields[3])?,
            },
        ))
    }
}

fn load_staged_peers(root: &Path) -> Result<BTreeMap<u64, StagedPeer>> {
    let file = match std::fs::File::open(root.join(STAGED_PEERS)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(MAX_INVENTORY_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_INVENTORY_BYTES,
        "staged peer inventory exceeds size limit"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

impl Settings {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(id) = std::env::var_os("RUSTA_NODE_ID") else {
            return Ok(None);
        };
        let id: u64 = id.to_str().context("invalid RUSTA_NODE_ID")?.parse()?;
        let cluster: Uuid = std::env::var("RUSTA_CLUSTER_ID")?.parse()?;
        ensure!(!cluster.is_nil(), "cluster ID cannot be nil");
        let mut peers = BTreeMap::new();
        for line in std::env::var("RUSTA_CLUSTER_PEERS")?.split(';') {
            let (member, peer) = Peer::parse(line)?;
            ensure!(peers.insert(member, peer).is_none(), "duplicate peer ID");
        }
        ensure!(
            !peers.is_empty() && peers.contains_key(&id),
            "the cluster roster must contain the local node"
        );
        let mut bootstrap_voters = match std::env::var("RUSTA_BOOTSTRAP_VOTERS") {
            Ok(voters) => voters
                .split(',')
                .map(str::parse::<u64>)
                .collect::<std::result::Result<Vec<_>, _>>()?,
            Err(std::env::VarError::NotPresent) => peers.keys().copied().collect(),
            Err(error) => return Err(error.into()),
        };
        bootstrap_voters.sort_unstable();
        ensure!(
            !bootstrap_voters.is_empty()
                && bootstrap_voters.windows(2).all(|pair| pair[0] != pair[1])
                && bootstrap_voters.iter().all(|id| peers.contains_key(id)),
            "bootstrap voters must be unique configured brokers"
        );
        let bind = std::env::var("RUSTA_RAFT_BIND")
            .unwrap_or_else(|_| "0.0.0.0:7000".to_owned())
            .parse()?;
        let certificates = PathBuf::from(std::env::var("RUSTA_CLUSTER_CERTS")?);
        let page_cache = match std::env::var("RUSTA_CLUSTER_ACK_MODE") {
            Ok(mode) if mode == "page-cache" => {
                ensure!(cfg!(target_os = "linux"), "page-cache ACK requires Linux");
                ensure!(
                    bootstrap_voters.len() >= 3,
                    "page-cache ACK requires at least three bootstrap voters"
                );
                true
            }
            Ok(mode) if mode == "durable" || mode == "auto" => false,
            Err(std::env::VarError::NotPresent) => false,
            _ => anyhow::bail!("RUSTA_CLUSTER_ACK_MODE must be auto, durable or page-cache"),
        };
        Ok(Some(Self {
            id,
            cluster,
            peers,
            bootstrap_voters,
            page_cache,
            cache_recovery: None,
            staged_peers: BTreeMap::new(),
            bind,
            certificates,
        }))
    }

    fn apply_staged_peers(&mut self, root: &Path) -> Result<()> {
        let staged = load_staged_peers(root)?;
        self.merge_staged_peers(&staged)?;
        for (id, staged) in staged {
            let pin = transport::load_pin(&self.certificates, id)?;
            ensure!(
                transport::peer_fingerprint(&pin, &staged.spec) == staged.fingerprint,
                "staged peer certificate has changed"
            );
        }
        Ok(())
    }

    fn merge_staged_peers(&mut self, peers: &BTreeMap<u64, StagedPeer>) -> Result<()> {
        for (&id, staged) in peers {
            let (member, peer) = Peer::parse(&staged.spec)?;
            ensure!(id == member, "staged peer ID mismatch");
            if let Some(existing) = self.peers.get(&id) {
                ensure!(
                    existing.spec == peer.spec,
                    "staged peer conflicts with startup inventory"
                );
            } else {
                self.peers.insert(id, peer);
            }
            self.staged_peers.insert(id, staged.fingerprint);
        }
        Ok(())
    }

    pub fn refresh_peer_pins(mut self, root: &Path) -> Result<usize> {
        ensure!(
            root.join("cluster-identity").is_file(),
            "pin refresh requires an initialized cluster volume"
        );
        let _lock = crate::lock_data(root).context("stop this broker before changing peer pins")?;
        migration::check_target(root)?;
        self.select_ack_mode(root)?;
        self.ensure_identity(root)?;
        let mut staged = load_staged_peers(root)?;
        self.merge_staged_peers(&staged)?;
        transport::Tls::load(&self)?.validate_inventory(&self)?;
        for (&id, peer) in &mut staged {
            let pin = transport::load_pin(&self.certificates, id)?;
            peer.fingerprint = transport::peer_fingerprint(&pin, &peer.spec);
        }
        disk::replace(root, STAGED_PEERS, &serde_json::to_vec(&staged)?)?;
        Ok(staged.len())
    }

    fn voter_mask(&self) -> [u8; 32] {
        let mut mask = [0; 32];
        for id in self.bootstrap_voters.iter().map(|id| *id as usize) {
            mask[id / 8] |= 1 << (id % 8);
        }
        // Node IDs start at one. Mark the previously unused bit so peers
        // reject incompatible acknowledgement policies before stepping Raft.
        mask[0] |= u8::from(self.page_cache);
        mask
    }

    fn ensure_identity(&self, root: &Path) -> Result<()> {
        ensure!(
            !root.join("catalog").exists(),
            "single-node data requires an explicit cluster migration"
        );
        let identity = if self.page_cache {
            serde_json::to_vec(&(self.cluster, self.id, &self.bootstrap_voters, "page-cache"))?
        } else {
            serde_json::to_vec(&(self.cluster, self.id, &self.bootstrap_voters))?
        };
        match std::fs::read(root.join("cluster-identity")) {
            Ok(previous) => ensure!(
                previous == identity,
                "volume identity does not match the cluster, node or acknowledgement mode"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ensure!(
                    !root.join("raft").exists(),
                    "raft data directory has no cluster identity"
                );
                disk::replace(root, "cluster-identity", &identity)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn select_ack_mode(&mut self, root: &Path) -> Result<()> {
        if std::env::var_os("RUSTA_CLUSTER_ACK_MODE").is_some_and(|mode| mode != "auto") {
            return Ok(());
        }
        self.page_cache = match std::fs::read(root.join("cluster-identity")) {
            Ok(bytes) => serde_json::from_slice::<(Uuid, u64, Vec<u64>, String)>(&bytes)
                .is_ok_and(|(_, _, _, mode)| mode == "page-cache"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                cfg!(target_os = "linux") && self.bootstrap_voters.len() >= 3
            }
            Err(error) => return Err(error.into()),
        };
        ensure!(
            !self.page_cache || cfg!(target_os = "linux"),
            "page-cache ACK requires Linux"
        );
        Ok(())
    }
}

fn activate_page_cache(root: &Path, boot_id: Uuid) -> Result<Option<Uuid>> {
    let previous = match std::fs::read(root.join(PAGE_CACHE_BOOT)) {
        Ok(previous) => Some(previous),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let changed = previous
        .as_deref()
        .is_some_and(|old| old != boot_id.as_bytes());
    let recovering = root.join(PAGE_CACHE_RECOVERY).try_exists()?;
    if changed && !recovering {
        disk::replace(root, PAGE_CACHE_RECOVERY, b"reconcile\n")?;
    }
    if previous.is_none() {
        // A previous binary recognizes this marker and should also refuse
        // a volume whose automatic recovery has not finished.
        let marker = if recovering { Uuid::nil() } else { boot_id };
        disk::replace(root, PAGE_CACHE_BOOT, marker.as_bytes())?;
    }
    Ok((changed || recovering).then(Uuid::new_v4))
}

fn kernel_boot_id() -> Result<Uuid> {
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .context("read kernel boot identity for page-cache ACK")?;
    Ok(boot_id.trim().parse()?)
}

fn finish_page_cache_recovery(root: &Path, boot_id: Uuid) -> Result<()> {
    disk::replace(root, PAGE_CACHE_BOOT, boot_id.as_bytes())?;
    std::fs::remove_file(root.join(PAGE_CACHE_RECOVERY))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn endpoint(value: &str) -> Result<Endpoint> {
    let (host, port) = value
        .rsplit_once(':')
        .context("Kafka endpoint must contain a port")?;
    let port = port.parse::<u16>()?;
    ensure!(!host.is_empty() && port != 0, "invalid Kafka endpoint");
    Ok(Endpoint {
        host: StrBytes::from_string(host.to_owned()),
        port: i32::from(port),
    })
}

pub struct Cluster {
    engine: Mutex<Option<Engine>>,
    worker: Arc<Semaphore>,
    executor: executor::Executor,
    batch_checks: Arc<Semaphore>,
    produce_admissions: mpsc::Sender<kafka::ProduceAdmission>,
    data_available: Arc<Notify>,
    group_progress: Arc<Notify>,
    fetch_waiters: Semaphore,
    transport: transport::Routes,
    page_cache: bool,
    root: PathBuf,
}

pub fn request_decommission(root: &Path) -> Result<()> {
    ensure!(
        root.join("cluster-identity").is_file(),
        "decommission requires an initialized cluster volume"
    );
    if !root.join("decommission").is_file() {
        // A unique temporary name lets repeated commands safely race to publish the same marker.
        let name = format!("decommission-{}", Uuid::new_v4());
        disk::replace(root, &name, b"requested\n")?;
        std::fs::rename(root.join(name), root.join("decommission"))?;
        std::fs::File::open(root)?.sync_all()?;
    }
    Ok(())
}

impl Cluster {
    pub async fn start(mut settings: Settings, root: &Path) -> Result<Arc<Self>> {
        settings.select_ack_mode(root)?;
        settings.ensure_identity(root)?;
        let page_cache = settings.page_cache;
        let group_commit_us = std::env::var("RUSTA_DURABLE_GROUP_COMMIT_US")
            .as_deref()
            .unwrap_or("0")
            .parse::<u64>()
            .context("invalid durable group commit window")?;
        ensure!(
            group_commit_us <= 2_000,
            "durable group commit window exceeds 2000 us"
        );
        let group_commit_wait = if page_cache {
            Duration::ZERO
        } else {
            Duration::from_micros(group_commit_us)
        };
        if page_cache {
            settings.cache_recovery = activate_page_cache(root, kernel_boot_id()?)?;
        }
        settings.apply_staged_peers(root)?;
        let tls = transport::Tls::load(&settings)?;
        let routes = transport::Routes::new(&settings, tls);
        let outbound = routes.connect_all(&settings);
        let node = Engine::open(settings.clone(), root, outbound)?;
        let (produce_admissions, admissions) = mpsc::channel(64);
        let executor = executor::Executor::new();
        let cluster = Arc::new(Self {
            data_available: node.data_available.clone(),
            group_progress: node.group_progress.clone(),
            fetch_waiters: Semaphore::new(crate::WAITING_FETCHES),
            engine: Mutex::new(Some(node)),
            worker: Arc::new(Semaphore::new(1)),
            executor,
            batch_checks: Arc::new(Semaphore::new(2)),
            produce_admissions,
            transport: routes,
            page_cache,
            root: root.to_owned(),
        });
        cluster.transport.listen(&settings, cluster.clone()).await?;
        admin::start(root, cluster.clone())?;
        tokio::spawn(kafka::admit_produce(
            Arc::downgrade(&cluster),
            admissions,
            group_commit_wait,
        ));
        let ticking = cluster.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                match ticking.call(|engine| engine.tick_groups(true)).await {
                    Ok(keys) => {
                        for key in keys {
                            ticking.executor.tick(&ticking, key);
                        }
                    }
                    Err(error) => {
                        eprintln!("Raft timer stopped: {error:#}");
                        break;
                    }
                }
                ack_profile::report();
            }
        });
        Ok(cluster)
    }

    async fn stage_peer(self: &Arc<Self>, spec: String) -> Result<u64> {
        let (id, peer) = Peer::parse(&spec)?;
        let cluster = self.clone();
        let runtime = tokio::runtime::Handle::current();
        self.call(move |engine| {
            let result = (|| {
                engine.check_peer(id, &peer)?;
                let pin = transport::load_pin(&engine.settings.certificates, id)?;
                let fingerprint = transport::peer_fingerprint(&pin, &spec);
                cluster.transport.check_pin(id, &peer, &pin)?;
                if let Some(&previous) = engine.settings.staged_peers.get(&id) {
                    ensure!(
                        previous == fingerprint,
                        "staged peer certificate has changed"
                    );
                    return Ok(id);
                }
                let mut staged = load_staged_peers(&cluster.root)?;
                if let Some(previous) = staged.insert(id, StagedPeer { spec, fingerprint }) {
                    ensure!(
                        previous.fingerprint == fingerprint,
                        "node ID already staged with a different identity"
                    );
                }
                disk::replace(&cluster.root, STAGED_PEERS, &serde_json::to_vec(&staged)?)?;
                if !engine.settings.peers.contains_key(&id) {
                    let sender = cluster.transport.add(id, peer.clone(), pin, &runtime);
                    engine.add_peer(id, peer, sender);
                }
                engine.settings.staged_peers.insert(id, fingerprint);
                Ok(id)
            })();
            Ok(result)
        })
        .await?
    }

    async fn call<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&mut Engine) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let topology = self.executor.topology.clone().write_owned().await;
        self.call_unfenced(move |engine| {
            let _topology = topology;
            work(engine)
        })
        .await
    }

    async fn call_unfenced<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&mut Engine) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self.worker.clone().acquire_owned().await?;
        let cluster = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut slot = cluster
                .engine
                .lock()
                .map_err(|_| anyhow::anyhow!("cluster state lock poisoned"))?;
            let engine = slot
                .as_mut()
                .context("cluster is out of service after a state error")?;
            ensure!(!engine.closing, "cluster is shutting down");
            match work(engine) {
                Ok(value) => Ok(value),
                Err(error) => {
                    eprintln!("cluster state failure: {error:#}");
                    *slot = None;
                    Err(error)
                }
            }
        })
        .await?
    }

    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        if !self.page_cache {
            return Ok(());
        }
        let root = self.root.clone();
        self.call(move |engine| {
            engine.closing = true;
            engine.flush_page_cache()?;
            if engine.settings.cache_recovery.is_none() {
                std::fs::remove_file(root.join(PAGE_CACHE_BOOT))?;
                std::fs::File::open(root)?.sync_all()?;
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod page_cache_tests {
    use super::*;

    #[test]
    fn cache_guard_persists_recovery_across_process_restarts() -> Result<()> {
        let volume = tempfile::tempdir()?;
        let original = Uuid::new_v4();
        let rebooted = Uuid::new_v4();
        assert!(activate_page_cache(volume.path(), original)?.is_none());
        assert!(activate_page_cache(volume.path(), original)?.is_none());
        assert!(activate_page_cache(volume.path(), rebooted)?.is_some());
        assert!(activate_page_cache(volume.path(), rebooted)?.is_some());
        assert_eq!(
            std::fs::read(volume.path().join(PAGE_CACHE_BOOT))?,
            original.as_bytes()
        );
        finish_page_cache_recovery(volume.path(), rebooted)?;
        assert!(activate_page_cache(volume.path(), rebooted)?.is_none());
        Ok(())
    }
}
