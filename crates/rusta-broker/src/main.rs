#![forbid(unsafe_code)]

mod catalog;
mod partition;
mod protocol;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use rusta_kafka::{MAX_FRAME, Request, decode, encode, protocol::StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Instant, timeout, timeout_at};

use crate::catalog::Catalog;
use crate::protocol::Endpoint;

const MAX_CONNECTIONS: usize = 128;
const IN_FLIGHT_REQUESTS: usize = 6;
const WAITING_FETCHES: usize = 3;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

struct Server {
    catalog: Mutex<Catalog>,
    connections: Arc<Semaphore>,
    requests: Semaphore,
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
                .lock()
                .map_err(|_| anyhow::anyhow!("broker state lock failed"))?;
            work(&mut catalog)
        })
        .await?
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let addr = std::env::var("RUSTA_BIND")
            .unwrap_or_else(|_| "0.0.0.0:9092".into())
            .parse::<SocketAddr>()?;
        return check_health(addr).await;
    }

    let path = std::env::var_os("RUSTA_DATA").map_or_else(|| PathBuf::from("data"), PathBuf::from);
    let host = Listener::from_env(
        "RUSTA_BIND",
        "RUSTA_ADVERTISED",
        Some(("0.0.0.0:9092", "127.0.0.1:9092")),
    )?;
    let host = host.context("missing host listener")?;
    let internal = Listener::from_env("RUSTA_INTERNAL_BIND", "RUSTA_INTERNAL_ADVERTISED", None)?;
    let catalog = Catalog::open(&path).with_context(|| format!("recover {}", path.display()))?;
    let state = Arc::new(Server {
        catalog: Mutex::new(catalog),
        connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        requests: Semaphore::new(IN_FLIGHT_REQUESTS),
        fetch_waiters: Semaphore::new(WAITING_FETCHES),
        worker: Arc::new(Semaphore::new(1)),
        data_available: Notify::new(),
    });
    let listener = host.bind().await?;
    let internal_listener = match internal {
        Some(internal) => Some(internal.bind().await?),
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
    worker
        .work(|catalog| {
            catalog.maintenance(false);
            Ok(())
        })
        .await
}

async fn maintenance(state: Arc<Server>) -> Result<()> {
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

    async fn bind(self) -> Result<(TcpListener, Endpoint)> {
        let listener = TcpListener::bind(self.bind).await?;
        eprintln!(
            "Kafka listener {} advertises {}:{}",
            listener.local_addr()?,
            self.endpoint.host.as_str(),
            self.endpoint.port
        );
        Ok((listener, self.endpoint))
    }
}

async fn accept((listener, endpoint): (TcpListener, Endpoint), state: Arc<Server>) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let Ok(permit) = state.connections.clone().try_acquire_owned() else {
            continue;
        };
        let state = state.clone();
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = connection(socket, &endpoint, state).await {
                // Unexpected protocol messages and connection resets are scoped to the client.
                if std::env::var("RUSTA_DEBUG_CONNECTIONS").as_deref() == Ok("1") {
                    eprintln!("Kafka connection closed: {error:#}");
                }
            }
        });
    }
}

async fn connection(mut socket: TcpStream, endpoint: &Endpoint, state: Arc<Server>) -> Result<()> {
    socket.set_nodelay(true)?;
    loop {
        let size = timeout(IDLE_TIMEOUT, socket.read_i32()).await??;
        ensure!(
            (8..=MAX_FRAME as i32).contains(&size),
            "invalid Kafka frame length"
        );
        let _request_slot = timeout(IO_TIMEOUT, state.requests.acquire()).await??;
        let mut frame = vec![0; size as usize];
        timeout(IO_TIMEOUT, socket.read_exact(&mut frame)).await??;
        let (head, request) = decode(frame.into())?;

        let response = if let Request::Fetch(request) = request {
            let version = head.version;
            let deadline =
                Instant::now() + Duration::from_millis(request.max_wait_ms.clamp(0, 2_000) as u64);
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
        } else {
            let notify = matches!(request, Request::Produce(_));
            let endpoint = endpoint.clone();
            let result = state
                .work(move |catalog| catalog.request(&head, request, &endpoint))
                .await?;
            if notify {
                state.data_available.notify_waiters();
            }
            result
        };
        if let Some(response) = response {
            timeout(IO_TIMEOUT, socket.write_all(&response)).await??;
        }
    }
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
