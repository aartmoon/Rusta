mod checkpoint;
mod store;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use rusta_kafka::{
    messages::{self, join_group_response::JoinGroupResponseMember},
    protocol::StrBytes,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::protocol::Error;
pub use store::{Completion, Event, Key, Offset, OffsetBudget};

use store::{JOURNAL_RETENTION, LocalStore, Offsets, Store};

pub const MAX_GROUPS: usize = 256;
const MAX_MEMBERS: usize = 32;
const MAX_NODE_MEMBERS: usize = 128;
const MAX_MEMBER_BYTES: usize = 16 * 1024;
const MAX_ASSIGNMENT_BYTES: usize = 128 * 1024;
const INITIAL_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const PROTOCOL: &str = "consumer";

pub struct Groups {
    pub entries: HashMap<String, Group>,
    store: Store,
    retention_ms: u64,
    max_groups: usize,
    max_members: usize,
}

#[derive(Default)]
pub struct Group {
    pub generation: i32,
    pub offsets: Offsets,
    pub empty_since: Option<u64>,
    pub members: BTreeMap<String, Member>,
    pub leader: String,
    pub protocol: Option<String>,
    pub phase: Phase,
    pending: HashMap<String, Instant>,
}

#[derive(Default)]
pub enum Phase {
    #[default]
    Empty,
    Preparing {
        not_before: Instant,
        deadline: Instant,
    },
    Completing {
        deadline: Instant,
    },
    Stable,
}

pub struct Member {
    pub client_id: String,
    pub client_host: String,
    protocols: Vec<(String, Bytes)>,
    pub assignment: Bytes,
    session: Duration,
    rebalance: Duration,
    expires: Instant,
    join: Option<oneshot::Sender<messages::JoinGroupResponse>>,
    sync: Option<oneshot::Sender<messages::SyncGroupResponse>>,
}

impl Member {
    pub fn metadata(&self, protocol: &str) -> Option<&Bytes> {
        self.protocols
            .iter()
            .find(|(name, _)| name == protocol)
            .map(|(_, bytes)| bytes)
    }
}

pub fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 249
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn str_bytes(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}

pub fn join_error(error: Error) -> messages::JoinGroupResponse {
    messages::JoinGroupResponse::default()
        .with_error_code(error as i16)
        .with_generation_id(-1)
}

pub fn sync_error(error: Error) -> messages::SyncGroupResponse {
    messages::SyncGroupResponse::default().with_error_code(error as i16)
}

fn send<T>(sender: oneshot::Sender<T>, value: T) {
    let _ = sender.send(value);
}

impl Groups {
    pub fn open(path: &Path) -> Result<Self> {
        let retention_ms = retention_ms()?;
        let (store, entries) = LocalStore::open(path)?;
        Ok(Self {
            entries,
            store: Store::Local(store),
            retention_ms,
            max_groups: MAX_GROUPS,
            max_members: MAX_NODE_MEMBERS,
        })
    }

    pub fn replicated(
        shards: usize,
        retention_ms: u64,
        committed: &HashMap<String, Group>,
    ) -> Self {
        Self {
            entries: committed
                .iter()
                .map(|(id, group)| {
                    (
                        id.clone(),
                        Group {
                            generation: group.generation,
                            offsets: group.offsets.clone(),
                            empty_since: group.empty_since,
                            ..Group::default()
                        },
                    )
                })
                .collect(),
            store: Store::Replicated {
                events: Vec::new(),
                completions: Vec::new(),
            },
            retention_ms,
            max_groups: MAX_GROUPS / shards,
            max_members: MAX_NODE_MEMBERS / shards,
        }
    }

    pub fn take_events(&mut self) -> (Vec<Event>, Vec<Completion>) {
        let Store::Replicated {
            events,
            completions,
        } = &mut self.store
        else {
            unreachable!("cluster events require a replicated store");
        };
        (std::mem::take(events), std::mem::take(completions))
    }

    pub fn join(
        &mut self,
        request: messages::JoinGroupRequest,
        client_id: String,
        client_host: String,
    ) -> oneshot::Receiver<messages::JoinGroupResponse> {
        let (sender, receiver) = oneshot::channel();
        match self.admit(&request) {
            Ok(group) => group.join(request, client_id, client_host, sender),
            Err(error) => send(sender, join_error(error)),
        }
        receiver
    }

    fn admit(&mut self, request: &messages::JoinGroupRequest) -> Result<&mut Group, Error> {
        let id = request.group_id.0.as_str();
        if !valid_id(id) {
            return Err(Error::InvalidGroupId);
        }
        if request.group_instance_id.is_some() {
            return Err(Error::UnsupportedVersion);
        }
        if !(1000..=300_000).contains(&request.session_timeout_ms)
            || !(1000..=300_000).contains(&request.rebalance_timeout_ms)
        {
            return Err(Error::InvalidSessionTimeout);
        }
        if request.protocol_type.as_str() != PROTOCOL
            || request.protocols.is_empty()
            || request.protocols.len() > 8
        {
            return Err(Error::InconsistentGroupProtocol);
        }
        let mut size = 0;
        let mut names = std::collections::HashSet::new();
        for protocol in &request.protocols {
            if !valid_id(protocol.name.as_str()) || !names.insert(protocol.name.as_str()) {
                return Err(Error::InconsistentGroupProtocol);
            }
            size += protocol.name.len() + protocol.metadata.len();
        }
        if size > MAX_MEMBER_BYTES {
            return Err(Error::InvalidRequest);
        }

        let known = self.entries.get(id);
        if request.member_id.is_empty() {
            if (known.is_none() && self.entries.len() >= self.max_groups)
                || known
                    .is_some_and(|group| group.members.len() + group.pending.len() >= MAX_MEMBERS)
                || self
                    .entries
                    .values()
                    .map(|group| group.members.len() + group.pending.len())
                    .sum::<usize>()
                    >= self.max_members
            {
                return Err(Error::GroupMaxSizeReached);
            }
        } else if !known.is_some_and(|group| {
            group.members.contains_key(request.member_id.as_str())
                || group
                    .pending
                    .get(request.member_id.as_str())
                    .is_some_and(|expiry| *expiry > Instant::now())
        }) {
            return Err(Error::UnknownMember);
        }
        Ok(self.entries.entry(id.to_owned()).or_default())
    }

    pub fn sync(
        &mut self,
        request: messages::SyncGroupRequest,
    ) -> oneshot::Receiver<messages::SyncGroupResponse> {
        let (sender, receiver) = oneshot::channel();
        let id = request.group_id.0.as_str().to_owned();
        if let Some(group) = self.entries.get_mut(&id) {
            group.sync(&id, &mut self.store, request, sender);
        } else {
            send(sender, sync_error(Error::UnknownMember));
        }
        receiver
    }

    pub fn heartbeat(&mut self, request: messages::HeartbeatRequest) -> Result<(), Error> {
        let group = self
            .entries
            .get_mut(request.group_id.0.as_str())
            .ok_or(Error::UnknownMember)?;
        group.validate(
            request.member_id.as_str(),
            request.generation_id,
            request.group_instance_id.is_some(),
        )?;
        let member = group
            .members
            .get_mut(request.member_id.as_str())
            .ok_or(Error::UnknownMember)?;
        member.expires = Instant::now() + member.session;
        if matches!(group.phase, Phase::Preparing { .. }) {
            return Err(Error::RebalanceInProgress);
        }
        Ok(())
    }

    pub fn leave(&mut self, id: &str, member: &str, static_member: bool) -> Result<(), Error> {
        if static_member {
            return Err(Error::UnsupportedVersion);
        }
        let group = self.entries.get_mut(id).ok_or(Error::UnknownMember)?;
        if group.pending.remove(member).is_some() {
            return Ok(());
        }
        if !group.remove_member(member, Error::UnknownMember) {
            return Err(Error::UnknownMember);
        }
        if group.members.is_empty() {
            group
                .make_empty(id, &mut self.store)
                .map_err(group_storage)?;
        } else {
            group.rebalance(Instant::now(), false);
        }
        Ok(())
    }

    pub fn validate_commit(
        &self,
        id: &str,
        member: &str,
        generation: i32,
        static_member: bool,
    ) -> Result<(), Error> {
        if !valid_id(id) {
            return Err(Error::InvalidGroupId);
        }
        if static_member {
            return Err(Error::UnsupportedVersion);
        }
        let group = self.entries.get(id);
        if generation == -1
            && member.is_empty()
            && group.is_none_or(|group| group.members.is_empty())
        {
            if group.is_none() && self.entries.len() >= self.max_groups {
                return Err(Error::GroupMaxSizeReached);
            }
            return Ok(());
        }
        let group = group.ok_or(Error::UnknownMember)?;
        group.validate(member, generation, false)?;
        if matches!(group.phase, Phase::Completing { .. }) {
            return Err(Error::RebalanceInProgress);
        }
        Ok(())
    }

    pub fn commit(&mut self, id: &str, offsets: Vec<(Key, Offset)>) -> Result<(), Error> {
        if offsets.is_empty() {
            return Ok(());
        }
        let active = self
            .entries
            .get(id)
            .is_some_and(|group| !group.members.is_empty());
        let empty_since = (!active).then(now_ms);
        self.store
            .append(Event::Commit {
                group: id.to_owned(),
                offsets: offsets.clone(),
                empty_since,
            })
            .map_err(group_storage)?;
        let group = self.entries.entry(id.to_owned()).or_default();
        group.offsets.extend(offsets);
        group.empty_since = empty_since;
        Ok(())
    }

    pub fn maintain(&mut self, retain_offsets: bool) {
        let now = Instant::now();
        let time = now_ms();
        let replicated = matches!(self.store, Store::Replicated { .. });
        let mut expired = Vec::new();
        for (id, group) in &mut self.entries {
            if let Err(error) = group.advance(id, &mut self.store, now) {
                group.fail_waiters(group_storage(error));
            }
            if group.members.is_empty() && group.pending.is_empty() {
                let transient = group.generation == 0 && group.offsets.is_empty();
                if replicated
                    && !transient
                    && group.empty_since.is_none()
                    && let Err(error) = group.make_empty(id, &mut self.store)
                {
                    group.fail_waiters(group_storage(error));
                }
                let eligible = retain_offsets
                    && group
                        .empty_since
                        .is_some_and(|since| time.saturating_sub(since) >= self.retention_ms);
                if transient
                    || (eligible
                        && self
                            .store
                            .append(Event::Expire { group: id.clone() })
                            .map_err(group_storage)
                            .is_ok())
                {
                    expired.push(id.clone());
                }
            }
            if self.store.has_events() {
                break;
            }
        }
        for group in expired {
            self.entries.remove(&group);
        }
        if retain_offsets && let Err(error) = self.store.compact(&self.entries, JOURNAL_RETENTION) {
            eprintln!("group checkpoint: {error:#}");
        }
    }
}

pub fn retention_ms() -> Result<u64> {
    match std::env::var("RUSTA_GROUP_OFFSET_RETENTION_MS") {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_RETENTION_MS),
        Err(error) => Err(error.into()),
    }
}

pub fn apply_event(entries: &mut HashMap<String, Group>, event: Event) -> std::io::Result<()> {
    store::apply(entries, event)
}

pub fn group_storage(error: anyhow::Error) -> Error {
    eprintln!("group journal: {error:#}");
    Error::CoordinatorNotAvailable
}

impl Group {
    pub fn state_name(&self) -> &'static str {
        match self.phase {
            Phase::Empty => "Empty",
            Phase::Preparing { .. } => "PreparingRebalance",
            Phase::Completing { .. } => "CompletingRebalance",
            Phase::Stable => "Stable",
        }
    }

    pub fn protocol_type(&self) -> &'static str {
        if self.generation == 0 { "" } else { PROTOCOL }
    }

    fn validate(&self, member: &str, generation: i32, static_member: bool) -> Result<(), Error> {
        if static_member {
            return Err(Error::UnsupportedVersion);
        }
        if !self.members.contains_key(member) {
            return Err(Error::UnknownMember);
        }
        if generation != self.generation {
            return Err(Error::IllegalGeneration);
        }
        Ok(())
    }

    fn join(
        &mut self,
        request: messages::JoinGroupRequest,
        client_id: String,
        client_host: String,
        sender: oneshot::Sender<messages::JoinGroupResponse>,
    ) {
        let now = Instant::now();
        let session = Duration::from_millis(request.session_timeout_ms as u64);
        let rebalance = Duration::from_millis(request.rebalance_timeout_ms as u64);
        if request.member_id.is_empty() {
            let id = Uuid::new_v4().to_string();
            self.pending.insert(id.clone(), now + session);
            send(
                sender,
                join_error(Error::MemberIdRequired).with_member_id(StrBytes::from_string(id)),
            );
            return;
        }

        let id = request.member_id.as_str();
        let protocols: Vec<_> = request
            .protocols
            .into_iter()
            .map(|protocol| {
                (
                    protocol.name.as_str().to_owned(),
                    Bytes::copy_from_slice(&protocol.metadata),
                )
            })
            .collect();
        // A candidate must overlap with every current member on at least one single protocol.
        if !protocols.iter().any(|(name, _)| {
            self.members
                .iter()
                .all(|(member_id, member)| member_id == id || member.metadata(name).is_some())
        }) {
            self.pending.remove(id);
            send(sender, join_error(Error::InconsistentGroupProtocol));
            return;
        }
        if matches!(self.phase, Phase::Completing { .. })
            && self
                .members
                .get(id)
                .is_some_and(|member| member.protocols == protocols)
        {
            send(sender, self.join_response(id));
            return;
        }

        let initial = self.members.is_empty();
        self.pending.remove(id);
        if let Some(mut previous) = self.members.remove(id) {
            if let Some(join) = previous.join.take() {
                send(join, join_error(Error::RebalanceInProgress));
            }
            if let Some(sync) = previous.sync.take() {
                send(sync, sync_error(Error::RebalanceInProgress));
            }
        }
        self.members.insert(
            id.to_owned(),
            Member {
                client_id,
                client_host,
                protocols,
                assignment: Bytes::new(),
                session,
                rebalance,
                expires: now + session,
                join: Some(sender),
                sync: None,
            },
        );
        if !matches!(self.phase, Phase::Preparing { .. }) {
            self.rebalance(now, initial);
        }
    }

    fn rebalance(&mut self, now: Instant, initial: bool) {
        let timeout = self
            .members
            .values()
            .map(|member| member.rebalance)
            .max()
            .unwrap_or(INITIAL_DELAY);
        for member in self.members.values_mut() {
            if let Some(waiter) = member.sync.take() {
                send(waiter, sync_error(Error::RebalanceInProgress));
            }
        }
        let not_before = if initial { now + INITIAL_DELAY } else { now };
        let deadline = match self.phase {
            Phase::Preparing { deadline, .. } => deadline,
            _ => now + timeout,
        };
        self.phase = Phase::Preparing {
            not_before,
            deadline,
        };
    }

    fn remove_member(&mut self, id: &str, error: Error) -> bool {
        let Some(member) = self.members.remove(id) else {
            return false;
        };
        if let Some(waiter) = member.join {
            send(waiter, join_error(error));
        }
        if let Some(waiter) = member.sync {
            send(waiter, sync_error(error));
        }
        true
    }

    fn fail_waiters(&mut self, error: Error) {
        for member in self.members.values_mut() {
            if let Some(waiter) = member.join.take() {
                send(waiter, join_error(error));
            }
            if let Some(waiter) = member.sync.take() {
                send(waiter, sync_error(error));
            }
        }
    }

    fn make_empty(&mut self, id: &str, store: &mut Store) -> Result<()> {
        self.phase = Phase::Empty;
        self.leader.clear();
        self.protocol = None;
        let since = now_ms();
        if self.generation != 0 || !self.offsets.is_empty() {
            store.append(Event::Empty {
                group: id.to_owned(),
                since,
            })?;
        }
        self.empty_since = Some(since);
        Ok(())
    }

    fn advance(&mut self, id: &str, store: &mut Store, now: Instant) -> Result<()> {
        self.pending.retain(|_, expires| *expires > now);

        let expired: Vec<_> = self
            .members
            .iter()
            .filter(|(_, member)| match self.phase {
                Phase::Preparing { deadline, .. } => {
                    member.join.is_none() && (member.expires <= now || deadline <= now)
                }
                Phase::Completing { deadline } => deadline <= now && member.sync.is_none(),
                Phase::Stable => member.expires <= now,
                Phase::Empty => false,
            })
            .map(|(id, _)| id.clone())
            .collect();

        if !expired.is_empty() {
            for member in expired {
                self.remove_member(&member, Error::UnknownMember);
            }
            if self.members.is_empty() {
                return self.make_empty(id, store);
            }
            if !matches!(self.phase, Phase::Preparing { .. }) {
                self.rebalance(now, false);
            }
        }
        if matches!(self.phase, Phase::Preparing { not_before, .. } if now >= not_before)
            && !self.members.is_empty()
            && self.members.values().all(|member| member.join.is_some())
        {
            self.complete_join(id, store, now)?;
        }
        Ok(())
    }

    fn complete_join(&mut self, id: &str, store: &mut Store, now: Instant) -> Result<()> {
        let mut votes = BTreeMap::<&str, usize>::new();
        for member in self.members.values() {
            if let Some((choice, _)) = member.protocols.iter().find(|(name, _)| {
                self.members
                    .values()
                    .all(|candidate| candidate.metadata(name).is_some())
            }) {
                *votes.entry(choice).or_default() += 1;
            }
        }
        let Some((protocol, _)) = votes.into_iter().max_by_key(|(_, votes)| *votes) else {
            self.fail_waiters(Error::InconsistentGroupProtocol);
            return Ok(());
        };
        let protocol = protocol.to_owned();
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("group generation exhausted"))?;
        store.append(Event::Generation {
            group: id.to_owned(),
            id: generation,
        })?;
        self.generation = generation;
        self.empty_since = None;
        self.protocol = Some(protocol);
        if !self.members.contains_key(&self.leader) {
            self.leader = self
                .members
                .keys()
                .next()
                .expect("joined group must have a member")
                .clone();
        }
        let timeout = self
            .members
            .values()
            .map(|member| member.rebalance)
            .max()
            .expect("members exist");
        self.phase = Phase::Completing {
            deadline: now + timeout,
        };
        let waiters: Vec<_> = self
            .members
            .iter_mut()
            .filter_map(|(id, member)| member.join.take().map(|waiter| (id.clone(), waiter)))
            .collect();
        for (id, waiter) in waiters {
            store.after_commit(
                waiter,
                self.join_response(&id),
                join_error(Error::NotCoordinator),
            );
        }
        Ok(())
    }

    fn join_response(&self, id: &str) -> messages::JoinGroupResponse {
        let protocol = self
            .protocol
            .as_deref()
            .expect("joined group has a protocol");
        messages::JoinGroupResponse::default()
            .with_generation_id(self.generation)
            .with_protocol_type(Some(str_bytes(PROTOCOL)))
            .with_protocol_name(Some(str_bytes(protocol)))
            .with_leader(str_bytes(&self.leader))
            .with_member_id(str_bytes(id))
            .with_members(if id == self.leader {
                self.members
                    .iter()
                    .map(|(id, member)| {
                        JoinGroupResponseMember::default()
                            .with_member_id(str_bytes(id))
                            .with_metadata(
                                member
                                    .metadata(protocol)
                                    .expect("selected common protocol")
                                    .clone(),
                            )
                    })
                    .collect()
            } else {
                Vec::new()
            })
    }

    fn sync(
        &mut self,
        id: &str,
        store: &mut Store,
        request: messages::SyncGroupRequest,
        sender: oneshot::Sender<messages::SyncGroupResponse>,
    ) {
        let member = request.member_id.as_str();
        if let Err(error) = self.validate(
            member,
            request.generation_id,
            request.group_instance_id.is_some(),
        ) {
            send(sender, sync_error(error));
            return;
        }
        if request
            .protocol_type
            .as_ref()
            .is_some_and(|kind| kind.as_str() != PROTOCOL)
            || request
                .protocol_name
                .as_ref()
                .is_some_and(|name| Some(name.as_str()) != self.protocol.as_deref())
        {
            send(sender, sync_error(Error::InconsistentGroupProtocol));
            return;
        }
        match self.phase {
            Phase::Stable => {
                let response = self.sync_response(member);
                let state = self.members.get_mut(member).expect("validated member");
                state.expires = Instant::now() + state.session;
                send(sender, response);
            }
            Phase::Completing { .. } if member == self.leader => {
                let result = self.complete_sync(id, store, request.assignments);
                if let Err(error) = result {
                    send(sender, sync_error(error));
                    return;
                }
                store.after_commit(
                    sender,
                    self.sync_response(member),
                    sync_error(Error::NotCoordinator),
                );
            }
            Phase::Completing { .. } if request.assignments.is_empty() => {
                let state = self.members.get_mut(member).expect("validated member");
                if let Some(old) = state.sync.replace(sender) {
                    send(old, sync_error(Error::RebalanceInProgress));
                }
            }
            Phase::Completing { .. } => send(sender, sync_error(Error::InvalidRequest)),
            _ => send(sender, sync_error(Error::RebalanceInProgress)),
        }
    }

    fn complete_sync(
        &mut self,
        id: &str,
        store: &mut Store,
        assignments: Vec<messages::sync_group_request::SyncGroupRequestAssignment>,
    ) -> Result<(), Error> {
        let mut valid = HashMap::new();
        let mut total_bytes = 0;
        for item in assignments {
            if item.assignment.len() > MAX_MEMBER_BYTES
                || !self.members.contains_key(item.member_id.as_str())
            {
                return Err(Error::InvalidRequest);
            }
            total_bytes += item.assignment.len();
            if total_bytes > MAX_ASSIGNMENT_BYTES
                || valid
                    .insert(item.member_id.as_str().to_owned(), item.assignment)
                    .is_some()
            {
                return Err(Error::InvalidRequest);
            }
        }
        let assignments = self
            .members
            .keys()
            .map(|member| {
                let payload = valid
                    .get(member)
                    .map_or_else(Vec::new, |bytes| bytes.to_vec());
                (member.clone(), payload)
            })
            .collect();
        store
            .append(Event::Stable {
                group: id.to_owned(),
                id: self.generation,
                assignments,
            })
            .map_err(group_storage)?;
        let now = Instant::now();
        self.phase = Phase::Stable;
        let mut waiters = Vec::new();
        for (id, member) in &mut self.members {
            member.assignment = valid
                .remove(id)
                .map_or_else(Bytes::new, |bytes| Bytes::copy_from_slice(&bytes));
            member.expires = now + member.session;
            if let Some(waiter) = member.sync.take() {
                waiters.push((id.clone(), waiter));
            }
        }
        for (id, waiter) in waiters {
            store.after_commit(
                waiter,
                self.sync_response(&id),
                sync_error(Error::NotCoordinator),
            );
        }
        Ok(())
    }

    fn sync_response(&self, id: &str) -> messages::SyncGroupResponse {
        messages::SyncGroupResponse::default()
            .with_protocol_type(Some(str_bytes(PROTOCOL)))
            .with_protocol_name(self.protocol.as_deref().map(str_bytes))
            .with_assignment(self.members[id].assignment.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use messages::{
        GroupId, JoinGroupRequest, SyncGroupRequest, join_group_request::JoinGroupRequestProtocol,
        sync_group_request::SyncGroupRequestAssignment,
    };

    const ID: &str = "consumers";

    fn group_id() -> GroupId {
        GroupId(str_bytes(ID))
    }

    fn join(groups: &mut Groups) -> (String, oneshot::Receiver<messages::JoinGroupResponse>) {
        let request = JoinGroupRequest::default()
            .with_group_id(group_id())
            .with_session_timeout_ms(1000)
            .with_rebalance_timeout_ms(4000)
            .with_protocol_type(str_bytes(PROTOCOL))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default().with_name(str_bytes("range")),
            ]);
        let pending = groups
            .join(request.clone(), "client".into(), "127.0.0.1".into())
            .try_recv()
            .unwrap();
        assert_eq!(pending.error_code, Error::MemberIdRequired as i16);
        let member = pending.member_id.as_str().to_owned();
        let completion = groups.join(
            request.with_member_id(pending.member_id),
            "client".into(),
            "127.0.0.1".into(),
        );
        (member, completion)
    }

    fn sync(generation_id: i32, member: &str) -> SyncGroupRequest {
        SyncGroupRequest::default()
            .with_group_id(group_id())
            .with_generation_id(generation_id)
            .with_member_id(str_bytes(member))
    }

    #[test]
    fn expires_unresponsive_member_and_fences_its_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut groups = Groups::open(dir.path()).unwrap();
        let (member, mut pending) = join(&mut groups);
        let now = Instant::now() + INITIAL_DELAY;
        groups
            .entries
            .get_mut(ID)
            .unwrap()
            .advance(ID, &mut groups.store, now)
            .unwrap();
        let joined = pending.try_recv().unwrap();
        assert_eq!(joined.generation_id, 1);
        let synced = groups
            .sync(sync(1, &member).with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(str_bytes(&member))
                        .with_assignment(Bytes::from_static(b"one")),
                ]))
            .try_recv()
            .unwrap();
        assert_eq!(synced.error_code, 0);
        assert_eq!(&synced.assignment[..], b"one");
        assert!(groups.validate_commit(ID, &member, 1, false).is_ok());
        assert!(matches!(
            groups.validate_commit(ID, &member, 0, false),
            Err(Error::IllegalGeneration)
        ));

        let future = Instant::now() + Duration::from_secs(2);
        groups
            .entries
            .get_mut(ID)
            .unwrap()
            .advance(ID, &mut groups.store, future)
            .unwrap();
        assert!(matches!(groups.entries[ID].phase, Phase::Empty));
        assert!(matches!(
            groups.validate_commit(ID, &member, 1, false),
            Err(Error::UnknownMember)
        ));

        drop(groups);
        let reopened = Groups::open(dir.path()).unwrap();
        assert_eq!(reopened.entries[ID].generation, 1);
        assert!(reopened.entries[ID].members.is_empty());
    }

    #[test]
    fn recovery_fences_previous_sessions_but_keeps_offsets_and_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut groups = Groups::open(dir.path()).unwrap();
        let (old_member, mut pending) = join(&mut groups);
        let now = Instant::now() + INITIAL_DELAY;
        groups
            .entries
            .get_mut(ID)
            .unwrap()
            .advance(ID, &mut groups.store, now)
            .unwrap();
        assert_eq!(pending.try_recv().unwrap().generation_id, 1);
        let synced = groups
            .sync(sync(1, &old_member).with_assignments(vec![
                SyncGroupRequestAssignment::default().with_member_id(str_bytes(&old_member)),
            ]))
            .try_recv()
            .unwrap();
        assert_eq!(synced.error_code, 0);
        assert!(groups.validate_commit(ID, &old_member, 1, false).is_ok());

        let key = Key {
            topic: Uuid::new_v4(),
            partition: 0,
        };
        groups
            .commit(
                ID,
                vec![(
                    key,
                    Offset {
                        value: 42,
                        leader_epoch: 0,
                        metadata: None,
                    },
                )],
            )
            .unwrap();
        assert!(groups.entries[ID].empty_since.is_none());
        drop(groups);

        let mut groups = Groups::open(dir.path()).unwrap();
        assert_eq!(groups.entries[ID].offsets[&key].value, 42);
        assert!(groups.entries[ID].empty_since.is_some());
        assert!(matches!(
            groups.validate_commit(ID, &old_member, 1, false),
            Err(Error::UnknownMember)
        ));
        let (_, mut pending) = join(&mut groups);
        let now = Instant::now() + INITIAL_DELAY;
        groups
            .entries
            .get_mut(ID)
            .unwrap()
            .advance(ID, &mut groups.store, now)
            .unwrap();
        assert_eq!(pending.try_recv().unwrap().generation_id, 2);
    }

    #[test]
    fn offset_expiration_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let mut groups = Groups::open(dir.path()).unwrap();
        let key = Key {
            topic: Uuid::new_v4(),
            partition: 0,
        };
        groups
            .commit(
                ID,
                vec![(
                    key,
                    Offset {
                        value: 9,
                        leader_epoch: 0,
                        metadata: Some("processed".into()),
                    },
                )],
            )
            .unwrap();
        let retention = rusta_log::Retention {
            max_bytes: None,
            keep_since_ms: Some(i64::MAX),
        };
        assert_eq!(groups.store.compact(&groups.entries, retention).unwrap(), 1);
        // New commits after the checkpoint must be replayed on top of it.
        let later = Key {
            partition: 1,
            ..key
        };
        groups
            .commit(
                ID,
                vec![(
                    later,
                    Offset {
                        value: 10,
                        leader_epoch: 0,
                        metadata: Some("next".into()),
                    },
                )],
            )
            .unwrap();
        drop(groups);

        let mut groups = Groups::open(dir.path()).unwrap();
        assert_eq!(groups.entries[ID].offsets[&key].value, 9);
        assert_eq!(groups.entries[ID].offsets[&later].value, 10);
        groups.retention_ms = 0;
        groups.maintain(true);
        assert!(!groups.entries.contains_key(ID));
        drop(groups);
        assert!(!Groups::open(dir.path()).unwrap().entries.contains_key(ID));
    }
}
