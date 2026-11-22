#![forbid(unsafe_code)]

mod catalog;
mod cluster;
mod group_protocol;
mod groups;
mod partition;
mod protocol;
mod topic_admin;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use rusta_kafka::{Header, MAX_FRAME, Request, decode, encode, protocol::StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, SemaphorePermit, mpsc, oneshot};
use tokio::time::{Instant, timeout, timeout_at};

use crate::catalog::Catalog;
use crate::protocol::Endpoint;

const MAX_CONNECTIONS: usize = 128;
const IN_FLIGHT_REQUESTS: usize = 6;
const CLUSTER_IN_FLIGHT_REQUESTS: usize = 16;
const CLUSTER_REQUEST_BYTES: usize = IN_FLIGHT_REQUESTS * MAX_FRAME;
const QUEUED_PRODUCE_REPLIES: usize = 3;
const WAITING_FETCHES: usize = 3;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

struct Server {
    catalog: Option<Mutex<Catalog>>,
    cluster: Option<Arc<cluster::Cluster>>,
    connections: Arc<Semaphore>,
    requests: Semaphore,
    request_bytes: Semaphore,
    fetch_waiters: Semaphore,
    worker: Arc<Semaphore>,
    data_available: Notify,
}

impl Server {
    async fn work<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&mut Catalog) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self.worker.clone().acquire_owned().await?;
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut catalog = state
                .catalog
                .as_ref()
                .context("singleton catalog requested in cluster mode")?
                .lock()
                .map_err(|_| anyhow::anyhow!("broker state lock failed"))?;
            work(&mut catalog)
        })
        .await?
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let command = std::env::args().nth(1);
    if command.as_deref() == Some("healthcheck") {
        let addr = std::env::var("RUSTA_BIND")
            .unwrap_or_else(|_| "0.0.0.0:9092".into())
            .parse::<SocketAddr>()?;
        return check_health(addr).await;
    }

    let path = std::env::var_os("RUSTA_DATA").map_or_else(|| PathBuf::from("data"), PathBuf::from);
    if command.as_deref() == Some("decommission") {
        cluster::request_decommission(&path)?;
        println!(
            "Decommission requested. Keep the broker running until it leaves cluster metadata."
        );
        return Ok(());
    }
    let args: Vec<_> = std::env::args().skip(2).collect();
    if command.as_deref() == Some("migrate-single-node") {
        let [source] = args.as_slice() else {
            anyhow::bail!("usage: migrate-single-node <source-directory>");
        };
        let settings = cluster::Settings::from_env()?
            .context("migration requires cluster settings for the target")?;
        cluster::migration::run(std::path::Path::new(source), &path, &settings)?;
        println!(
            "Migration target is ready. Import every bootstrap voter before starting the cluster."
        );
        return Ok(());
    }
    if command.as_deref() == Some("refresh-peer-pins") {
        ensure!(args.is_empty(), "usage: refresh-peer-pins");
        let settings =
            cluster::Settings::from_env()?.context("pin refresh requires cluster settings")?;
        let updated = settings.refresh_peer_pins(&path)?;
        println!("Refreshed {updated} staged peer pins.");
        return Ok(());
    }
    let admin = match (command.as_deref(), args.as_slice()) {
        (Some("rebalance"), []) => Some(cluster::admin::Command::Rebalance {
            id: uuid::Uuid::new_v4(),
        }),
        (Some("rebalance"), [id]) => Some(cluster::admin::Command::Rebalance { id: id.parse()? }),
        (Some("peer-inventory"), []) => Some(cluster::admin::Command::PeerInventory),
        (Some("stage-peer"), [spec]) => {
            Some(cluster::admin::Command::StagePeer { spec: spec.clone() })
        }
        (Some("relocation-status"), []) => Some(cluster::admin::Command::RelocationStatus),
        (Some("transfer-leadership"), [topic, partition, to]) => {
            Some(cluster::admin::Command::Transfer {
                topic: topic.clone(),
                partition: partition.parse()?,
                to: to.parse()?,
            })
        }
        (
            Some(
                "rebalance"
                | "peer-inventory"
                | "stage-peer"
                | "relocation-status"
                | "transfer-leadership",
            ),
            _,
        ) => {
            anyhow::bail!(
                "usage: rebalance [request-uuid] | peer-inventory | stage-peer <id,raft,host,internal> | relocation-status | transfer-leadership <topic> <partition> <broker-id>"
            );
        }
        _ => None,
    };
    if let Some(command) = admin {
        return cluster::admin::run(&path, command).await;
    }
    let host = Listener::from_env(
        "RUSTA_BIND",
        "RUSTA_ADVERTISED",
        Some(("0.0.0.0:9092", "127.0.0.1:9092")),
    )?;
    let host = host.context("missing host listener")?;
    let internal = Listener::from_env("RUSTA_INTERNAL_BIND", "RUSTA_INTERNAL_ADVERTISED", None)?;
    std::fs::create_dir_all(&path)?;
    let _mode_lock = lock_data(&path)?;
    cluster::migration::check_target(&path)?;
    let (catalog, cluster) = match cluster::Settings::from_env()? {
        Some(settings) => (None, Some(cluster::Cluster::start(settings, &path).await?)),
        None => {
            cluster::migration::check_single_node(&path)?;
            ensure!(
                !path.join("cluster-identity").exists(),
                "cluster volume cannot be opened as a singleton"
            );
            (
                Some(Mutex::new(
                    Catalog::open(&path).with_context(|| format!("recover {}", path.display()))?,
                )),
                None,
            )
        }
    };
    let in_flight = if cluster.is_some() {
        CLUSTER_IN_FLIGHT_REQUESTS
    } else {
        IN_FLIGHT_REQUESTS
    };
    let state = Arc::new(Server {
        catalog,
        cluster,
        connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        requests: Semaphore::new(in_flight),
        request_bytes: Semaphore::new(CLUSTER_REQUEST_BYTES),
        fetch_waiters: Semaphore::new(WAITING_FETCHES),
        worker: Arc::new(Semaphore::new(1)),
        data_available: Notify::new(),
    });
    let listener = host.bind(false).await?;
    let internal_listener = match internal {
        Some(internal) => Some(internal.bind(true).await?),
        None => None,
    };

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stop = async {
        #[cfg(unix)]
        tokio::select! {
            result = tokio::signal::ctrl_c() => { result?; },
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        Ok(())
    };
    let worker = state.clone();
    let result = tokio::select! {
        result = maintenance(state.clone()) => result,
        result = accept(listener, state.clone()) => result,
        result = async {
            if let Some(listener) = internal_listener { accept(listener, state).await }
            else { std::future::pending::<Result<()>>().await }
        } => result,
        result = stop => result,
    };
    result?;
    if let Some(cluster) = &worker.cluster {
        return cluster.shutdown().await;
    }
    worker
        .work(|catalog| {
            catalog.maintenance(false);
            Ok(())
        })
        .await
}

fn lock_data(root: &std::path::Path) -> Result<std::fs::File> {
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("mode-lock"))?;
    lock.try_lock().map_err(std::io::Error::from)?;
    Ok(lock)
}

async fn maintenance(state: Arc<Server>) -> Result<()> {
    if state.cluster.is_some() {
        return std::future::pending().await;
    }
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_retention = Instant::now();
    loop {
        interval.tick().await;
        let retention = last_retention.elapsed() >= Duration::from_secs(1);
        if retention {
            last_retention = Instant::now();
        }
        if state
            .work(move |catalog| Ok(catalog.maintenance(retention)))
            .await?
        {
            state.data_available.notify_waiters();
        }
    }
}

struct Listener {
    bind: SocketAddr,
    endpoint: Endpoint,
}

impl Listener {
    fn from_env(
        bind: &str,
        advertised: &str,
        defaults: Option<(&str, &str)>,
    ) -> Result<Option<Self>> {
        let bound = std::env::var(bind)
            .ok()
            .or_else(|| defaults.map(|pair| pair.0.to_owned()));
        let endpoint = std::env::var(advertised)
            .ok()
            .or_else(|| defaults.map(|pair| pair.1.to_owned()));
        match (bound, endpoint) {
            (None, None) => Ok(None),
            (Some(bound), Some(endpoint)) => {
                let (host, port) = endpoint
                    .rsplit_once(':')
                    .context("advertised address requires host:port")?;
                let host = host.trim_start_matches('[').trim_end_matches(']');
                let port = port.parse::<u16>()?;
                ensure!(!host.is_empty() && port != 0, "invalid advertised address");
                Ok(Some(Self {
                    bind: bound.parse().with_context(|| format!("invalid {bind}"))?,
                    endpoint: Endpoint {
                        host: StrBytes::from_string(host.to_owned()),
                        port: i32::from(port),
                    },
                }))
            }
            _ => anyhow::bail!("set {bind} and {advertised} together"),
        }
    }

    async fn bind(self, internal: bool) -> Result<(TcpListener, Endpoint, bool)> {
        let listener = TcpListener::bind(self.bind).await?;
        eprintln!(
            "Kafka listener {} advertises {}:{}",
            listener.local_addr()?,
            self.endpoint.host.as_str(),
            self.endpoint.port
        );
        Ok((listener, self.endpoint, internal))
    }
}

async fn accept(
    (listener, endpoint, internal): (TcpListener, Endpoint, bool),
    state: Arc<Server>,
) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let Ok(permit) = state.connections.clone().try_acquire_owned() else {
            continue;
        };
        let state = state.clone();
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = connection(socket, &endpoint, internal, state).await {
                // Unexpected protocol messages and connection resets are scoped to the client.
                if std::env::var("RUSTA_DEBUG_CONNECTIONS").as_deref() == Ok("1") {
                    eprintln!("Kafka connection closed: {error:#}");
                }
            }
        });
    }
}

async fn connection(
    mut socket: TcpStream,
    endpoint: &Endpoint,
    internal: bool,
    state: Arc<Server>,
) -> Result<()> {
    socket.set_nodelay(true)?;
    let client_host = socket.peer_addr()?.ip().to_string();
    if let Some(cluster) = &state.cluster {
        return cluster_connection(
            socket,
            cluster,
            &state.requests,
            &state.request_bytes,
            internal,
            client_host,
        )
        .await;
    }
    loop {
        let size = timeout(IDLE_TIMEOUT, socket.read_i32()).await??;
        ensure!(
            (8..=MAX_FRAME as i32).contains(&size),
            "invalid Kafka frame length"
        );
        let mut request_slot = Some(timeout(IO_TIMEOUT, state.requests.acquire()).await??);
        let mut frame = vec![0; size as usize];
        timeout(IO_TIMEOUT, socket.read_exact(&mut frame)).await??;
        let (head, request) = decode(frame.into())?;
        let response = match request {
            Request::JoinGroup(request) => {
                let host = client_host.clone();
                let client = head.client_id.clone();
                let completion = state
                    .work(move |catalog| Ok(catalog.groups.join(request, client, host)))
                    .await?;
                drop(request_slot.take());
                let response = group_protocol::join_response(completion.await?, head.version);
                Some(encode(&head, head.version, &response)?)
            }
            Request::SyncGroup(request) => {
                let completion = state
                    .work(move |catalog| Ok(catalog.groups.sync(request)))
                    .await?;
                drop(request_slot.take());
                let response = group_protocol::sync_response(completion.await?, head.version);
                Some(encode(&head, head.version, &response)?)
            }
            Request::Fetch(request) => {
                let version = head.version;
                let deadline = Instant::now()
                    + Duration::from_millis(request.max_wait_ms.clamp(0, 2_000) as u64);
                let min = request.min_bytes.clamp(0, 4 * 1024 * 1024) as usize;
                let request = Arc::new(request);
                let wait_slot = state.fetch_waiters.try_acquire().ok();
                loop {
                    let notified = state.data_available.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let request = request.clone();
                    let (response, bytes, error) = state
                        .work(move |catalog| Ok(catalog.fetch(&request, version)))
                        .await?;
                    if error || bytes >= min || wait_slot.is_none() || Instant::now() >= deadline {
                        break Some(encode(&head, version, &response)?);
                    }
                    drop(response);
                    let _ = timeout_at(deadline, notified).await;
                }
            }
            request => {
                let notify = matches!(request, Request::Produce(_));
                let endpoint = endpoint.clone();
                let result = state
                    .work(move |catalog| catalog.request(&head, request, &endpoint))
                    .await?;
                if notify {
                    state.data_available.notify_waiters();
                }
                result
            }
        };
        if let Some(response) = response {
            timeout(IO_TIMEOUT, socket.write_all(&response)).await??;
        }
    }
}

struct RequestBudget<'a> {
    _slot: SemaphorePermit<'a>,
    _bytes: SemaphorePermit<'a>,
}

enum ClusterResponse<'a> {
    Produce(Header, cluster::ProduceReply, RequestBudget<'a>, Instant),
    Other(Header, Request, RequestBudget<'a>, oneshot::Sender<()>),
}

async fn cluster_connection(
    mut socket: TcpStream,
    cluster: &Arc<cluster::Cluster>,
    requests: &Semaphore,
    request_bytes: &Semaphore,
    internal: bool,
    client_host: String,
) -> Result<()> {
    let (mut reader, mut writer) = socket.split();
    let (send, mut receive) = mpsc::channel(QUEUED_PRODUCE_REPLIES);
    let admit = async move {
        loop {
            let slot = send.reserve().await?;
            let size = match timeout(IDLE_TIMEOUT, reader.read_i32()).await? {
                Ok(size) => size,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok::<_, anyhow::Error>(());
                }
                Err(error) => return Err(error.into()),
            };
            ensure!(
                (8..=MAX_FRAME as i32).contains(&size),
                "invalid Kafka frame length"
            );
            let permit = RequestBudget {
                _slot: timeout(IO_TIMEOUT, requests.acquire()).await??,
                _bytes: timeout(IO_TIMEOUT, request_bytes.acquire_many(size as u32)).await??,
            };
            let mut frame = vec![0; size as usize];
            timeout(IO_TIMEOUT, reader.read_exact(&mut frame)).await??;
            let (head, request) = decode(frame.into())?;
            if let Request::Produce(request) = request {
                let deadline = Instant::now() + IO_TIMEOUT;
                let reply = timeout_at(deadline, cluster.stage_produce(request)).await??;
                slot.send(ClusterResponse::Produce(head, reply, permit, deadline));
            } else {
                // Other APIs retain their ordering with respect to Produce on this socket.
                let (done, finished) = oneshot::channel();
                slot.send(ClusterResponse::Other(head, request, permit, done));
                finished.await?;
            }
        }
    };
    let respond = async move {
        while let Some(response) = receive.recv().await {
            match response {
                ClusterResponse::Produce(head, reply, _permit, deadline) => {
                    let bytes = timeout_at(deadline, reply.finish(&head)).await??;
                    timeout(IO_TIMEOUT, writer.write_all(&bytes)).await??;
                }
                ClusterResponse::Other(head, request, permit, done) => {
                    let waits = matches!(request, Request::JoinGroup(_) | Request::SyncGroup(_));
                    let request = cluster.request(&head, request, internal, client_host.clone());
                    let mut permit = Some(permit);
                    let bytes = if waits {
                        drop(permit.take());
                        request.await?
                    } else {
                        timeout(IO_TIMEOUT, request).await??
                    };
                    if let Some(bytes) = bytes {
                        timeout(IO_TIMEOUT, writer.write_all(&bytes)).await??;
                    }
                    let _ = done.send(());
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::try_join!(admit, respond)?;
    Ok(())
}

async fn check_health(mut address: SocketAddr) -> Result<()> {
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    let mut client = timeout(Duration::from_secs(2), TcpStream::connect(address)).await??;
    timeout(Duration::from_secs(2), async {
        // ApiVersions v0, correlation 1, null client ID.
        client
            .write_all(&[0, 0, 0, 10, 0, 18, 0, 0, 0, 0, 0, 1, 255, 255])
            .await?;
        let size = client.read_i32().await?;
        ensure!(
            (6..=MAX_FRAME as i32).contains(&size),
            "invalid healthcheck reply"
        );
        let mut frame = vec![0; size as usize];
        client.read_exact(&mut frame).await?;
        ensure!(
            Bytes::from(frame).starts_with(&[0, 0, 0, 1, 0, 0]),
            "Kafka healthcheck failed"
        );
        Ok(())
    })
    .await?
}
