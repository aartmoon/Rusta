use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

use super::{
    Cluster,
    engine::{Engine, RebalanceResult},
    kafka::reply,
};

const SOCKET: &str = "admin/control";
const MAX_REQUEST: usize = 4096;
const MAX_RESPONSE: usize = 16 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const CLIENT_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Deserialize, Serialize)]
pub enum Command {
    Rebalance {
        id: Uuid,
    },
    PeerInventory,
    StagePeer {
        spec: String,
    },
    RelocationStatus,
    Transfer {
        topic: String,
        partition: i32,
        to: u64,
    },
}

#[derive(Deserialize, Serialize)]
enum Response {
    Ok(Value),
    Error(String),
}

pub fn start(root: &Path, cluster: Arc<Cluster>) -> Result<()> {
    let directory = root.join("admin");
    match std::fs::DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = std::fs::symlink_metadata(&directory)?;
    ensure!(
        metadata.is_dir() && metadata.permissions().mode() & 0o077 == 0,
        "cluster admin directory must be private"
    );
    let path = root.join(SOCKET);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket(),
                "expected a cluster admin socket"
            );
            std::fs::remove_file(&path)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(path)?;
    let slots = Arc::new(Semaphore::new(4));
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let Ok(slot) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let cluster = cluster.clone();
            tokio::spawn(async move {
                let _slot = slot;
                let _ = timeout(CLIENT_DEADLINE, serve(cluster, &mut stream)).await;
            });
        }
    });
    Ok(())
}

pub async fn run(root: &Path, command: Command) -> Result<()> {
    let socket = root.join(SOCKET);
    let mut stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("connect to broker admin socket {}", socket.display()))?;
    timeout(CLIENT_DEADLINE, async {
        write(&mut stream, &command, MAX_REQUEST).await?;
        match read(&mut stream, MAX_RESPONSE).await? {
            Response::Ok(value) => {
                println!("{}", serde_json::to_string_pretty(&value)?);
                Ok(())
            }
            Response::Error(message) => anyhow::bail!("{message}"),
        }
    })
    .await?
}

async fn serve(cluster: Arc<Cluster>, stream: &mut UnixStream) -> Result<()> {
    let command = timeout(DEADLINE, read(stream, MAX_REQUEST)).await??;
    let response = match execute(&cluster, command).await {
        Ok(value) => Response::Ok(value),
        Err(error) => Response::Error(error.to_string()),
    };
    write(stream, &response, MAX_RESPONSE).await
}

async fn read<T: serde::de::DeserializeOwned>(stream: &mut UnixStream, limit: usize) -> Result<T> {
    let size = stream.read_u32().await? as usize;
    ensure!((1..=limit).contains(&size), "invalid admin frame size");
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn write<T: Serialize>(stream: &mut UnixStream, message: &T, limit: usize) -> Result<()> {
    let bytes = serde_json::to_vec(message)?;
    ensure!(bytes.len() <= limit, "admin frame exceeds limit");
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

async fn execute(cluster: &Arc<Cluster>, command: Command) -> Result<Value> {
    match command {
        Command::PeerInventory => cluster.call(|engine| Ok(engine.peer_inventory())).await,
        Command::StagePeer { spec } => {
            let id = cluster.stage_peer(spec).await?;
            Ok(json!({ "node_id": id, "result": "staged" }))
        }
        Command::RelocationStatus => cluster.call(|engine| Ok(engine.relocation_status())).await,
        Command::Rebalance { id } => {
            let request = cluster
                .call(move |engine| engine.rebalance(id))
                .await?
                .map_err(anyhow::Error::msg)?;
            let result = match request {
                RebalanceResult::Pending(pending) => {
                    reply(pending).await.map_err(|error| {
                        anyhow::anyhow!(
                            "rebalance {id} outcome unknown ({error:?}), retry the same request ID"
                        )
                    })?;
                    "accepted"
                }
                RebalanceResult::Active => "accepted",
                RebalanceResult::Completed => "completed",
                RebalanceResult::Balanced => "balanced",
            };
            let status = cluster
                .call(|engine| Ok(Engine::relocation_status(engine)))
                .await?;
            Ok(json!({ "id": id, "result": result, "status": status }))
        }
        Command::Transfer {
            topic,
            partition,
            to,
        } => {
            let key = cluster
                .call(move |engine| engine.transfer_leadership(&topic, partition, to))
                .await?
                .map_err(anyhow::Error::msg)?;
            let deadline = Instant::now() + DEADLINE;
            loop {
                let (leader, epoch) = cluster
                    .call(move |engine| Ok(engine.leader_view(key)))
                    .await?;
                if leader == to {
                    return Ok(json!({ "leader": leader, "epoch": epoch }));
                }
                ensure!(
                    Instant::now() < deadline,
                    "transfer outcome unknown, observed leader {leader}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}
