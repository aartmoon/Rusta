use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use protobuf::Message as ProtoMessage;
use raft::eraftpb::Message;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::{sleep, timeout},
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    rustls::{
        ClientConfig, RootCertStore, ServerConfig,
        pki_types::{CertificateDer, ServerName},
        server::WebPkiClientVerifier,
    },
};
use uuid::Uuid;

use super::{
    Cluster, Peer, Settings,
    engine::{GroupId, Movement},
};

pub enum Packet {
    Raft(GroupId, Message),
    Leaders(Bytes),
    Join,
    Drain,
    Moved(Movement),
}

pub struct Queued {
    packet: Packet,
    _bytes: OwnedSemaphorePermit,
}

impl Queued {
    pub fn send(queue: &mpsc::Sender<Self>, budget: &Arc<Semaphore>, packet: Packet) {
        let size = match &packet {
            Packet::Raft(_, message) => message.compute_size() as usize + GROUP,
            Packet::Leaders(report) => report.len(),
            Packet::Join | Packet::Drain => 0,
            Packet::Moved(_) => MOVE,
        } + HEADER;
        if size <= MAX_PACKET
            && let Ok(permit) = budget.clone().try_acquire_many_owned(size as u32)
        {
            let _ = queue.try_send(Self {
                packet,
                _bytes: permit,
            });
        }
    }
}

pub type Senders = HashMap<u64, mpsc::Sender<Queued>>;
type Receivers = HashMap<u64, mpsc::Receiver<Queued>>;
const MAX_PACKET: usize = 2 * 1024 * 1024 + 65536;
const HEADER: usize = 49;
const GROUP: usize = 20;
const MOVE: usize = GROUP + 16;
const REPORT_ITEM: usize = GROUP + 4;
const DEADLINE: Duration = Duration::from_secs(2);

pub struct Tls {
    connect: TlsConnector,
    accept: TlsAcceptor,
    identity: Arc<HashMap<u64, CertificateDer<'static>>>,
}

impl Tls {
    pub fn load(settings: &Settings) -> Result<Self> {
        let path = &settings.certificates;
        let mut roots = RootCertStore::empty();
        for cert in certificates(&path.join("ca.pem"))? {
            roots.add(cert)?;
        }
        let mut identity = HashMap::new();
        for &id in settings.peers.keys() {
            let leaf = certificates(&path.join(format!("node-{id}.pem")))?
                .into_iter()
                .next()
                .context("empty peer certificate")?;
            ensure!(
                !identity.values().any(|existing| existing == &leaf),
                "peer certificates must be unique"
            );
            identity.insert(id, leaf);
        }
        let own = vec![identity[&settings.id].clone()];
        let key =
            rustls_pemfile::private_key(&mut BufReader::new(File::open(path.join("key.pem"))?))?
                .context("missing node private key")?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone())).build()?;
        let accept = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(own.clone(), key.clone_key())?;
        let connect = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(own, key)?;
        Ok(Self {
            connect: TlsConnector::from(Arc::new(connect)),
            accept: TlsAcceptor::from(Arc::new(accept)),
            identity: Arc::new(identity),
        })
    }
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut BufReader::new(File::open(path)?))
        .collect::<std::io::Result<_>>()
        .with_context(|| format!("read certificates in {}", path.display()))
}

pub fn routes(settings: &Settings) -> (Senders, Receivers) {
    let mut outbound = Senders::new();
    let mut receiver = Receivers::new();
    for &id in settings.peers.keys().filter(|id| **id != settings.id) {
        let (sender, messages) = mpsc::channel(16);
        outbound.insert(id, sender);
        receiver.insert(id, messages);
    }
    (outbound, receiver)
}

pub async fn start(
    settings: Settings,
    tls: Tls,
    receivers: Receivers,
    cluster: Arc<Cluster>,
) -> Result<()> {
    let listener = TcpListener::bind(settings.bind).await?;
    for (id, messages) in receivers {
        tokio::spawn(outbound(
            settings.cluster,
            settings.voter_mask(),
            settings.peers[&id].clone(),
            tls.identity[&id].clone(),
            tls.connect.clone(),
            messages,
        ));
    }
    tokio::spawn(async move {
        let capacity = Arc::new(Semaphore::new(settings.peers.len() + 4));
        let bytes = Arc::new(Semaphore::new(16 * 1024 * 1024));
        while let Ok((socket, _)) = listener.accept().await {
            let Ok(permit) = capacity.clone().try_acquire_owned() else {
                continue;
            };
            let certificates = tls.identity.clone();
            let tls = tls.accept.clone();
            let cluster = cluster.clone();
            let peer_identity = (settings.cluster, settings.voter_mask(), settings.id);
            let bytes = bytes.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = inbound(cluster, peer_identity, tls, certificates, socket, bytes).await;
            });
        }
    });
    Ok(())
}

async fn outbound(
    cluster: Uuid,
    voter_mask: [u8; 32],
    peer: Peer,
    pinned: CertificateDer<'static>,
    tls: TlsConnector,
    mut messages: mpsc::Receiver<Queued>,
) {
    loop {
        let Some(first) = messages.recv().await else {
            return;
        };
        let result = async {
            let socket = timeout(DEADLINE, TcpStream::connect(&peer.raft)).await??;
            socket.set_nodelay(true)?;
            let name = ServerName::try_from(peer.dns.clone())?;
            let mut socket = timeout(DEADLINE, tls.connect(name, socket)).await??;
            let leaf = socket
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first());
            ensure!(leaf == Some(&pinned), "unexpected remote node certificate");
            let mut queued = first;
            loop {
                let (kind, group, data): (_, _, Bytes) = match &queued.packet {
                    Packet::Raft(key, message) => (2, Some(*key), message.write_to_bytes()?.into()),
                    Packet::Leaders(data) => (3, None, data.clone()),
                    Packet::Join => (4, None, Bytes::new()),
                    Packet::Drain => (5, None, Bytes::new()),
                    Packet::Moved(movement) => {
                        let mut body = BytesMut::with_capacity(MOVE);
                        body.extend_from_slice(movement.key.topic.as_bytes());
                        body.put_u32(movement.key.partition);
                        body.put_u64(movement.from);
                        body.put_u64(movement.to);
                        (6, None, body.freeze())
                    }
                };
                let size = HEADER + data.len() + group.map_or(0, |_| GROUP);
                ensure!(size <= MAX_PACKET, "Raft frame too large");
                timeout(DEADLINE, async {
                    socket.write_u32(size as u32).await?;
                    socket.write_u8(kind).await?;
                    socket.write_all(cluster.as_bytes()).await?;
                    socket.write_all(&voter_mask).await?;
                    if let Some(key) = group {
                        socket.write_all(key.topic.as_bytes()).await?;
                        socket.write_u32(key.partition).await?;
                    }
                    socket.write_all(&data).await?;
                    socket.flush().await
                })
                .await??;
                drop(queued);
                let Some(next) = messages.recv().await else {
                    return Ok::<_, anyhow::Error>(());
                };
                queued = next;
            }
        }
        .await;
        if result.is_ok() {
            return;
        }
        if messages.is_closed() {
            return;
        }
        sleep(Duration::from_millis(75)).await;
    }
}

async fn inbound(
    cluster: Arc<Cluster>,
    (cluster_id, voter_mask, local): (Uuid, [u8; 32], u64),
    tls: TlsAcceptor,
    pins: Arc<HashMap<u64, CertificateDer<'static>>>,
    socket: TcpStream,
    bytes: Arc<Semaphore>,
) -> Result<()> {
    socket.set_nodelay(true)?;
    let mut socket = timeout(DEADLINE, tls.accept(socket)).await??;
    let leaf = socket
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|chain| chain.first())
        .context("missing peer certificate")?;
    let source = pins
        .iter()
        .find(|(id, pinned)| **id != local && *pinned == leaf)
        .map(|(&id, _)| id)
        .context("unregistered node certificate")?;
    loop {
        let frame = timeout(Duration::from_secs(30), socket.read_u32()).await?? as usize;
        ensure!(
            (HEADER..=MAX_PACKET).contains(&frame),
            "invalid Raft frame size"
        );
        let _permit = timeout(DEADLINE, bytes.acquire_many(frame as u32)).await??;
        let mut data = vec![0; frame];
        timeout(DEADLINE, socket.read_exact(&mut data)).await??;
        ensure!(
            &data[1..17] == cluster_id.as_bytes() && data[17..HEADER] == voter_mask,
            "wrong cluster identity or voter roster"
        );
        let body = &data[HEADER..];
        match data[0] {
            2 => {
                ensure!(body.len() >= GROUP, "missing Raft group identity");
                let key = group_id(body)?;
                let message: Message = ProtoMessage::parse_from_bytes(&body[GROUP..])?;
                ensure!(
                    message.from == source && message.to == local,
                    "wrong authenticated Raft node ID"
                );
                cluster
                    .call(move |engine| engine.receive(key, message))
                    .await?;
            }
            3 => {
                let (items, tail) = body.as_chunks::<REPORT_ITEM>();
                ensure!(
                    tail.is_empty() && items.len() <= 128,
                    "invalid leader report"
                );
                let entries = items
                    .iter()
                    .map(|item| {
                        Ok((
                            group_id(item)?,
                            i32::from_be_bytes(item[GROUP..].try_into()?),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                cluster
                    .call(move |engine| engine.receive_leaders(source, entries))
                    .await?;
            }
            4 => {
                ensure!(body.is_empty(), "invalid cluster registration");
                cluster
                    .call(move |engine| engine.receive_join(source))
                    .await?;
            }
            5 => {
                ensure!(body.is_empty(), "invalid decommission request");
                cluster
                    .call(move |engine| engine.receive_drain(source))
                    .await?;
            }
            6 => {
                ensure!(body.len() == MOVE, "invalid replica movement report");
                let movement = Movement {
                    key: group_id(body)?,
                    from: u64::from_be_bytes(body[GROUP..GROUP + 8].try_into()?),
                    to: u64::from_be_bytes(body[GROUP + 8..].try_into()?),
                };
                cluster
                    .call(move |engine| engine.receive_move(source, movement))
                    .await?;
            }
            _ => anyhow::bail!("unsupported peer protocol"),
        }
    }
}

fn group_id(bytes: &[u8]) -> Result<GroupId> {
    Ok(GroupId {
        topic: Uuid::from_slice(&bytes[..16])?,
        partition: u32::from_be_bytes(bytes[16..GROUP].try_into()?),
    })
}
