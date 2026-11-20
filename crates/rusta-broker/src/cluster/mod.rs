mod disk;
mod engine;
mod kafka;
mod transport;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rusta_kafka::protocol::StrBytes;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::protocol::Endpoint;

use self::engine::Engine;

#[derive(Clone)]
pub struct Settings {
    pub id: u64,
    pub cluster: Uuid,
    pub peers: BTreeMap<u64, Peer>,
    pub bootstrap_voters: Vec<u64>,
    bind: SocketAddr,
    certificates: PathBuf,
}

#[derive(Clone)]
pub struct Peer {
    raft: String,
    dns: String,
    pub host: Endpoint,
    pub internal: Endpoint,
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
            let fields: Vec<_> = line.split(',').collect();
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
            let peer = Peer {
                raft: fields[1].to_owned(),
                dns: name.to_owned(),
                host: endpoint(fields[2])?,
                internal: endpoint(fields[3])?,
            };
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
        Ok(Some(Self {
            id,
            cluster,
            peers,
            bootstrap_voters,
            bind,
            certificates,
        }))
    }

    fn voter_mask(&self) -> [u8; 32] {
        let mut mask = [0; 32];
        for id in self.bootstrap_voters.iter().map(|id| *id as usize) {
            mask[id / 8] |= 1 << (id % 8);
        }
        mask
    }

    fn ensure_identity(&self, root: &Path) -> Result<()> {
        ensure!(
            !root.join("catalog").exists(),
            "single-node data requires an explicit cluster migration"
        );
        let identity = serde_json::to_vec(&(self.cluster, self.id, &self.bootstrap_voters))?;
        match std::fs::read(root.join("cluster-identity")) {
            Ok(previous) => ensure!(
                previous == identity,
                "volume identity does not match the cluster and node"
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
    pub async fn start(settings: Settings, root: &Path) -> Result<Arc<Self>> {
        settings.ensure_identity(root)?;
        let tls = transport::Tls::load(&settings)?;
        let (outbound, routes) = transport::routes(&settings);
        let node = Engine::open(settings.clone(), root, outbound)?;
        let cluster = Arc::new(Self {
            engine: Mutex::new(Some(node)),
            worker: Arc::new(Semaphore::new(1)),
        });
        transport::start(settings, tls, routes, cluster.clone()).await?;
        let ticking = cluster.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if let Err(error) = ticking.call(Engine::tick).await {
                    eprintln!("Raft timer stopped: {error:#}");
                    break;
                }
            }
        });
        Ok(cluster)
    }

    async fn call<T: Send + 'static>(
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
}
