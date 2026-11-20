use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use protobuf::Message as ProtoMessage;
use raft::eraftpb::{Message, MessageType};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::{sleep, timeout},
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    client::TlsStream,
    rustls::{
        ClientConfig, RootCertStore, ServerConfig,
        client::{WebPkiServerVerifier, danger::ServerCertVerifier},
        pki_types::{CertificateDer, ServerName, UnixTime},
        server::{WebPkiClientVerifier, danger::ClientCertVerifier},
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
    Moved(Uuid, Movement),
    PeerReady(Bytes),
    RecoveryRequest(GroupId, Uuid),
    RecoveryStart(GroupId, Uuid, u64, u64),
}

pub struct Queued {
    packet: Packet,
    _bytes: OwnedSemaphorePermit,
}

impl Queued {
    #[cfg(test)]
    pub(super) fn into_packet(self) -> Packet {
        self.packet
    }

    pub fn send(queue: &mpsc::Sender<Self>, budget: &Arc<Semaphore>, packet: Packet) {
        let size = match &packet {
            Packet::Raft(_, message) => message.compute_size() as usize + GROUP,
            Packet::Leaders(report) | Packet::PeerReady(report) => report.len(),
            Packet::Join | Packet::Drain => 0,
            Packet::Moved(..) => MOVE,
            Packet::RecoveryRequest(..) => GROUP + 16,
            Packet::RecoveryStart(..) => GROUP + 32,
        } + HEADER;
        let limit = if matches!(&packet, Packet::Raft(_, message) if message.get_msg_type() == MessageType::MsgSnapshot)
        {
            MAX_SNAPSHOT_FRAME
        } else {
            MAX_PACKET
        };
        if size <= limit
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
type Pins = HashMap<u64, CertificateDer<'static>>;
const MAX_PACKET: usize = 2 * 1024 * 1024 + 65536;
const HEADER: usize = 49;
const GROUP: usize = 20;
const LEGACY_MOVE: usize = GROUP + 16;
const MOVE: usize = LEGACY_MOVE + 16;
const REPORT_ITEM: usize = GROUP + 4;
const DEADLINE: Duration = Duration::from_secs(2);
const READY_ITEM: usize = 33;
const SNAPSHOT_FRAME: u8 = 8;
const MAX_SNAPSHOT_FRAME: usize = super::checkpoint::MAX_SNAPSHOT as usize + 16 * 1024;
const SNAPSHOT_CHUNK: usize = 256 * 1024;
const SNAPSHOT_RATE: f64 = (8 * 1024 * 1024) as f64;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(15);

pub fn peer_fingerprint(pin: &CertificateDer<'static>, spec: &str) -> [u8; 32] {
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(spec.as_bytes());
    digest.update(&[0]);
    digest.update(pin.as_ref());
    digest.finish().as_ref().try_into().expect("SHA256 digest")
}

pub struct Tls {
    connect: TlsConnector,
    accept: TlsAcceptor,
    identity: Pins,
    validators: Validators,
}

struct Validators {
    client_cert: Arc<dyn ClientCertVerifier>,
    server_cert: Arc<WebPkiServerVerifier>,
}

impl Validators {
    fn verify(&self, peer: &Peer, certificate: &CertificateDer<'static>) -> Result<()> {
        let time = UnixTime::now();
        self.client_cert
            .verify_client_cert(certificate, &[], time)?;
        self.server_cert.verify_server_cert(
            certificate,
            &[],
            &ServerName::try_from(peer.dns.as_str())?,
            &[],
            time,
        )?;
        Ok(())
    }
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
            let leaf = load_pin(path, id)?;
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
        let roots = Arc::new(roots);
        let validators = Validators {
            client_cert: WebPkiClientVerifier::builder(roots.clone()).build()?,
            server_cert: WebPkiServerVerifier::builder(roots.clone()).build()?,
        };
        let accept = ServerConfig::builder()
            .with_client_cert_verifier(validators.client_cert.clone())
            .with_single_cert(own.clone(), key.clone_key())?;
        let connect = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(own, key)?;
        Ok(Self {
            connect: TlsConnector::from(Arc::new(connect)),
            accept: TlsAcceptor::from(Arc::new(accept)),
            identity,
            validators,
        })
    }

    pub fn validate_inventory(&self, settings: &Settings) -> Result<()> {
        for (&id, peer) in &settings.peers {
            self.validators
                .verify(peer, &self.identity[&id])
                .with_context(|| format!("validate certificate for node {id}"))?;
        }
        Ok(())
    }
}

pub fn load_pin(path: &Path, id: u64) -> Result<CertificateDer<'static>> {
    certificates(&path.join(format!("node-{id}.pem")))?
        .into_iter()
        .next()
        .context("empty peer certificate")
}

pub fn validate_name(name: &str) -> Result<()> {
    ServerName::try_from(name)?;
    Ok(())
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut BufReader::new(File::open(path)?))
        .collect::<std::io::Result<_>>()
        .with_context(|| format!("read certificates in {}", path.display()))
}

pub struct Routes {
    connect: TlsConnector,
    accept: TlsAcceptor,
    identity: Arc<RwLock<Pins>>,
    capacity: Arc<Semaphore>,
    cluster: Uuid,
    voter_mask: [u8; 32],
    validators: Validators,
}

impl Routes {
    pub fn new(settings: &Settings, tls: Tls) -> Self {
        Self {
            connect: tls.connect,
            accept: tls.accept,
            identity: Arc::new(RwLock::new(tls.identity)),
            capacity: Arc::new(Semaphore::new(settings.peers.len() + 4)),
            cluster: settings.cluster,
            voter_mask: settings.voter_mask(),
            validators: tls.validators,
        }
    }

    pub fn connect_all(&self, settings: &Settings) -> Senders {
        let pins = self.identity.read().expect("peer pins");
        let runtime = Handle::current();
        settings
            .peers
            .iter()
            .filter(|(id, _)| **id != settings.id)
            .map(|(&id, peer)| {
                (
                    id,
                    self.connect_peer(peer.clone(), pins[&id].clone(), &runtime),
                )
            })
            .collect()
    }

    pub fn check_pin(&self, id: u64, peer: &Peer, pinned: &CertificateDer<'static>) -> Result<()> {
        self.validators
            .verify(peer, pinned)
            .context("validate peer certificate")?;
        let pins = self.identity.read().expect("peer pins");
        ensure!(
            pins.get(&id).is_none_or(|existing| existing == pinned),
            "configured peer certificate has changed"
        );
        ensure!(
            !pins
                .iter()
                .any(|(&member, existing)| member != id && existing == pinned),
            "peer certificates must be unique"
        );
        Ok(())
    }

    pub fn add(
        &self,
        id: u64,
        peer: Peer,
        pinned: CertificateDer<'static>,
        runtime: &Handle,
    ) -> mpsc::Sender<Queued> {
        self.identity
            .write()
            .expect("peer pins")
            .insert(id, pinned.clone());
        self.capacity.add_permits(1);
        self.connect_peer(peer, pinned, runtime)
    }

    fn connect_peer(
        &self,
        peer: Peer,
        pinned: CertificateDer<'static>,
        runtime: &Handle,
    ) -> mpsc::Sender<Queued> {
        let (sender, messages) = mpsc::channel(16);
        runtime.spawn(outbound(
            self.cluster,
            self.voter_mask,
            peer,
            pinned,
            self.connect.clone(),
            messages,
        ));
        sender
    }

    pub async fn listen(&self, settings: &Settings, cluster: Arc<Cluster>) -> Result<()> {
        let listener = TcpListener::bind(settings.bind).await?;
        let peer_identity = (self.cluster, self.voter_mask, settings.id);
        let capacity = self.capacity.clone();
        let pins = self.identity.clone();
        let accept = self.accept.clone();
        tokio::spawn(async move {
            let bytes = Arc::new(Semaphore::new(16 * 1024 * 1024));
            while let Ok((socket, _)) = listener.accept().await {
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    continue;
                };
                let tls = accept.clone();
                let pins = pins.clone();
                let cluster = cluster.clone();
                let bytes = bytes.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = inbound(cluster, peer_identity, tls, pins, socket, bytes).await;
                });
            }
        });
        Ok(())
    }
}

struct Remote {
    cluster: Uuid,
    voter_mask: [u8; 32],
    peer: Peer,
    pinned: CertificateDer<'static>,
    tls: TlsConnector,
}

impl Remote {
    async fn connect(&self) -> Result<TlsStream<TcpStream>> {
        let socket = timeout(DEADLINE, TcpStream::connect(&self.peer.raft)).await??;
        socket.set_nodelay(true)?;
        let name = ServerName::try_from(self.peer.dns.clone())?;
        let socket = timeout(DEADLINE, self.tls.connect(name, socket)).await??;
        let leaf = socket
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|chain| chain.first());
        ensure!(
            leaf == Some(&self.pinned),
            "unexpected remote node certificate"
        );
        Ok(socket)
    }

    fn frame_header(
        &self,
        kind: u8,
        group: Option<GroupId>,
        len: usize,
    ) -> Result<([u8; 4 + HEADER + GROUP], usize)> {
        let header_size = HEADER + group.map_or(0, |_| GROUP);
        let size = header_size + len;
        let limit = if kind == SNAPSHOT_FRAME {
            MAX_SNAPSHOT_FRAME
        } else {
            MAX_PACKET
        };
        ensure!(size <= limit, "Raft frame too large");
        let mut header = [0; 4 + HEADER + GROUP];
        let mut fields = &mut header[..];
        fields.put_u32(size as u32);
        fields.put_u8(kind);
        fields.put_slice(self.cluster.as_bytes());
        fields.put_slice(&self.voter_mask);
        if let Some(key) = group {
            fields.put_slice(key.topic.as_bytes());
            fields.put_u32(key.partition);
        }
        Ok((header, 4 + header_size))
    }

    async fn send_snapshot(&self, key: GroupId, message: &Message) -> Result<()> {
        let mut socket = self.connect().await?;
        let data = message.write_to_bytes()?;
        let (header, len) = self.frame_header(SNAPSHOT_FRAME, Some(key), data.len())?;
        socket.write_all(&header[..len]).await?;
        for chunk in data.chunks(SNAPSHOT_CHUNK) {
            timeout(DEADLINE, socket.write_all(chunk)).await??;
            socket.flush().await?;
            sleep(Duration::from_secs_f64(chunk.len() as f64 / SNAPSHOT_RATE)).await;
        }
        Ok(())
    }
}

async fn outbound(
    cluster: Uuid,
    voter_mask: [u8; 32],
    peer: Peer,
    pinned: CertificateDer<'static>,
    tls: TlsConnector,
    mut messages: mpsc::Receiver<Queued>,
) {
    let remote = Arc::new(Remote {
        cluster,
        voter_mask,
        peer,
        pinned,
        tls,
    });
    let snapshots = Arc::new(Semaphore::new(1));
    let mut socket = None;
    while let Some(queued) = messages.recv().await {
        if matches!(&queued.packet, Packet::Raft(_, message) if message.get_msg_type() == MessageType::MsgSnapshot)
        {
            if let Ok(slot) = snapshots.clone().try_acquire_owned() {
                let remote = remote.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    let Packet::Raft(key, message) = &queued.packet else {
                        unreachable!()
                    };
                    let _ = timeout(SNAPSHOT_TIMEOUT, remote.send_snapshot(*key, message)).await;
                });
            }
            continue;
        }
        if socket.is_none() {
            match remote.connect().await {
                Ok(connection) => socket = Some(connection),
                Err(_) => {
                    sleep(Duration::from_millis(75)).await;
                    continue;
                }
            }
        }
        let connection = socket.as_mut().expect("connected");
        let result = timeout(DEADLINE, async {
            let (kind, group, data): (_, _, Bytes) = match &queued.packet {
                Packet::Raft(key, message) => (2, Some(*key), message.write_to_bytes()?.into()),
                Packet::Leaders(data) => (3, None, data.clone()),
                Packet::Join => (4, None, Bytes::new()),
                Packet::Drain => (5, None, Bytes::new()),
                Packet::PeerReady(data) => (7, None, data.clone()),
                Packet::RecoveryRequest(key, token) => {
                    (9, Some(*key), Bytes::copy_from_slice(token.as_bytes()))
                }
                Packet::RecoveryStart(key, token, term, committed) => {
                    let mut data = BytesMut::with_capacity(32);
                    data.extend_from_slice(token.as_bytes());
                    data.put_u64(*term);
                    data.put_u64(*committed);
                    (10, Some(*key), data.freeze())
                }
                Packet::Moved(id, movement) => {
                    let mut body = BytesMut::with_capacity(MOVE);
                    body.extend_from_slice(movement.key.topic.as_bytes());
                    body.put_u32(movement.key.partition);
                    body.put_u64(movement.from);
                    body.put_u64(movement.to);
                    body.extend_from_slice(id.as_bytes());
                    (6, None, body.freeze())
                }
            };
            let (header, len) = remote.frame_header(kind, group, data.len())?;
            connection
                .write_all_buf(&mut Buf::chain(&header[..len], data))
                .await?;
            connection.flush().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            socket = None;
            sleep(Duration::from_millis(75)).await;
        }
    }
}

async fn inbound(
    cluster: Arc<Cluster>,
    (cluster_id, voter_mask, local): (Uuid, [u8; 32], u64),
    tls: TlsAcceptor,
    pins: Arc<RwLock<Pins>>,
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
        .read()
        .expect("peer pins")
        .iter()
        .find(|(id, pinned)| **id != local && *pinned == leaf)
        .map(|(&id, _)| id)
        .context("unregistered node certificate")?;
    loop {
        let frame = timeout(Duration::from_secs(30), socket.read_u32()).await?? as usize;
        ensure!(
            (HEADER..=MAX_SNAPSHOT_FRAME).contains(&frame),
            "invalid Raft frame size"
        );
        let mut header = [0; HEADER];
        timeout(DEADLINE, socket.read_exact(&mut header)).await??;
        let kind = header[0];
        ensure!(
            &header[1..17] == cluster_id.as_bytes() && header[17..HEADER] == voter_mask,
            "wrong cluster identity or voter roster"
        );
        ensure!(
            frame <= MAX_PACKET || kind == SNAPSHOT_FRAME,
            "invalid Raft frame size"
        );
        let _permit = timeout(DEADLINE, bytes.acquire_many(frame as u32)).await??;
        let mut data = vec![0; frame - HEADER];
        if kind == SNAPSHOT_FRAME {
            timeout(SNAPSHOT_TIMEOUT, read_snapshot_body(&mut socket, &mut data)).await??;
        } else {
            timeout(DEADLINE, socket.read_exact(&mut data)).await??;
        }
        let body = data.as_slice();
        match kind {
            2 | SNAPSHOT_FRAME => {
                ensure!(body.len() >= GROUP, "missing Raft group identity");
                let key = group_id(body)?;
                let message: Message = ProtoMessage::parse_from_bytes(&body[GROUP..])?;
                ensure!(
                    message.from == source && message.to == local,
                    "wrong authenticated Raft node ID"
                );
                ensure!(
                    kind != SNAPSHOT_FRAME || message.get_msg_type() == MessageType::MsgSnapshot,
                    "invalid snapshot frame"
                );
                cluster
                    .call(move |engine| engine.receive(key, message))
                    .await?;
                if kind == SNAPSHOT_FRAME {
                    return Ok(());
                }
            }
            3 => {
                let (items, tail) = body.as_chunks::<REPORT_ITEM>();
                ensure!(
                    tail.is_empty() && items.len() <= 132,
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
                ensure!(
                    body.len() == MOVE || body.len() == LEGACY_MOVE,
                    "invalid replica movement report"
                );
                let movement = Movement {
                    key: group_id(body)?,
                    from: u64::from_be_bytes(body[GROUP..GROUP + 8].try_into()?),
                    to: u64::from_be_bytes(body[GROUP + 8..LEGACY_MOVE].try_into()?),
                };
                let id = if body.len() == MOVE {
                    Uuid::from_slice(&body[LEGACY_MOVE..])?
                } else {
                    Uuid::nil()
                };
                cluster
                    .call(move |engine| engine.receive_move(source, id, movement))
                    .await?;
            }
            7 => {
                let (items, tail) = body.as_chunks::<READY_ITEM>();
                ensure!(
                    tail.is_empty() && (1..=255).contains(&items.len()),
                    "invalid peer readiness report"
                );
                let ready = items
                    .iter()
                    .map(|item| {
                        let (&id, fingerprint) = item.split_first().expect("peer readiness item");
                        (
                            u64::from(id),
                            fingerprint.try_into().expect("peer fingerprint"),
                        )
                    })
                    .collect();
                cluster
                    .call(move |engine| {
                        engine.receive_peer_ready(source, ready);
                        Ok(())
                    })
                    .await?;
            }
            9 => {
                ensure!(body.len() == GROUP + 16, "invalid recovery request");
                let key = group_id(body)?;
                let token = Uuid::from_slice(&body[GROUP..])?;
                cluster
                    .call(move |engine| engine.request_cache_recovery(source, key, token))
                    .await?;
            }
            10 => {
                ensure!(body.len() == GROUP + 32, "invalid recovery offer");
                let key = group_id(body)?;
                let token = Uuid::from_slice(&body[GROUP..GROUP + 16])?;
                let term = u64::from_be_bytes(body[GROUP + 16..GROUP + 24].try_into()?);
                let committed = u64::from_be_bytes(body[GROUP + 24..].try_into()?);
                cluster
                    .call(move |engine| {
                        engine.accept_cache_recovery(source, key, token, term, committed);
                        Ok(())
                    })
                    .await?;
            }
            _ => anyhow::bail!("unsupported peer protocol"),
        }
    }
}

async fn read_snapshot_body(
    reader: &mut (impl AsyncRead + Unpin),
    bytes: &mut [u8],
) -> std::io::Result<()> {
    for chunk in bytes.chunks_mut(SNAPSHOT_CHUNK) {
        reader.read_exact(chunk).await?;
    }
    Ok(())
}

fn group_id(bytes: &[u8]) -> Result<GroupId> {
    Ok(GroupId {
        topic: Uuid::from_slice(&bytes[..16])?,
        partition: u32::from_be_bytes(bytes[16..GROUP].try_into()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discarding_an_overflowing_frame_releases_its_byte_budget() {
        let (sender, mut receiver) = mpsc::channel(1);
        let budget = Arc::new(Semaphore::new(2 * HEADER));
        Queued::send(&sender, &budget, Packet::Join);
        Queued::send(&sender, &budget, Packet::Drain);
        assert_eq!(budget.available_permits(), HEADER);
        assert!(matches!(
            receiver.try_recv().unwrap().into_packet(),
            Packet::Join
        ));
        assert_eq!(budget.available_permits(), 2 * HEADER);
        assert!(receiver.try_recv().is_err());

        let small_budget = Arc::new(Semaphore::new(HEADER - 1));
        Queued::send(&sender, &small_budget, Packet::Join);
        assert_eq!(small_budget.available_permits(), HEADER - 1);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn interrupted_snapshot_stream_requires_a_complete_retry() {
        let payload = vec![0x3c; MAX_PACKET + SNAPSHOT_CHUNK];
        let mut received = vec![0; payload.len()];
        let (mut writer, mut reader) = tokio::io::duplex(SNAPSHOT_CHUNK);
        let truncated = &payload[..SNAPSHOT_CHUNK + 100];
        let (_, result) = tokio::join!(
            async move { writer.write_all(truncated).await.unwrap() },
            read_snapshot_body(&mut reader, &mut received),
        );
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        let (mut writer, mut reader) = tokio::io::duplex(SNAPSHOT_CHUNK);
        let (_, result) = tokio::join!(
            async { writer.write_all(&payload).await.unwrap() },
            read_snapshot_body(&mut reader, &mut received),
        );
        result.unwrap();
        assert_eq!(received, payload);
    }
}
