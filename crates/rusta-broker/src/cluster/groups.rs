use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rusta_kafka::messages::{self, BrokerId};
use tokio::time::{Instant, timeout, timeout_at};

use crate::{
    group_protocol,
    groups::{join_error, sync_error, valid_id},
    protocol::Error,
};

use super::{
    Cluster,
    engine::{Engine, GroupId, coordinator, group_error},
    kafka::{DEADLINE, reply},
};

impl Cluster {
    pub async fn find_coordinator(
        self: &Arc<Self>,
        request: messages::FindCoordinatorRequest,
        version: i16,
        internal: bool,
    ) -> Result<messages::FindCoordinatorResponse> {
        self.call(move |engine| {
            Ok(group_protocol::find_coordinator(request, version, |id| {
                let (leader, _) = engine.leader_view(coordinator(id));
                let peer = engine
                    .settings
                    .peers
                    .get(&leader)
                    .ok_or(Error::CoordinatorNotAvailable)?;
                Ok((
                    BrokerId(leader as i32),
                    if internal { &peer.internal } else { &peer.host },
                ))
            }))
        })
        .await
    }

    async fn group_term(self: &Arc<Self>, id: &str) -> Result<Result<(GroupId, u64), Error>> {
        if !valid_id(id) {
            return Ok(Err(Error::InvalidGroupId));
        }
        let key = coordinator(id);
        let result = match self.call(move |engine| engine.group_barrier(key)).await? {
            Ok((term, receiver)) => reply(receiver)
                .await
                .map(|()| (key, term))
                .map_err(group_error),
            Err(error) => Err(error),
        };
        Ok(result)
    }

    pub async fn join_group(
        self: &Arc<Self>,
        request: messages::JoinGroupRequest,
        client: String,
        host: String,
    ) -> Result<messages::JoinGroupResponse> {
        let (key, term) = match self.group_term(request.group_id.0.as_str()).await? {
            Ok(location) => location,
            Err(error) => return Ok(join_error(error)),
        };
        let wait = Duration::from_millis(request.rebalance_timeout_ms.clamp(1000, 300_000) as u64)
            + DEADLINE;
        let result = self
            .call(move |engine| {
                engine.with_group(key, term, |groups, _| groups.join(request, client, host))
            })
            .await?;
        Ok(match result {
            Ok((receiver, _)) => match timeout(wait, receiver).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => join_error(Error::NotCoordinator),
                Err(_) => join_error(Error::CoordinatorLoadInProgress),
            },
            Err(error) => join_error(error),
        })
    }

    pub async fn sync_group(
        self: &Arc<Self>,
        request: messages::SyncGroupRequest,
    ) -> Result<messages::SyncGroupResponse> {
        let retryable = |error| {
            if matches!(error, Error::CoordinatorLoadInProgress) {
                Error::NotCoordinator
            } else {
                error
            }
        };
        let (key, term) = match self.group_term(request.group_id.0.as_str()).await? {
            Ok(location) => location,
            Err(error) => return Ok(sync_error(retryable(error))),
        };
        let deadline = Instant::now() + DEADLINE;
        let mut request = request;
        let result = loop {
            let progress = self.group_progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            let attempt = self
                .call(move |engine| {
                    if matches!(
                        engine.active_groups(key, term),
                        Err(Error::CoordinatorLoadInProgress)
                    ) {
                        return Ok(Err(request));
                    }
                    Ok(Ok(
                        engine.with_group(key, term, |groups, _| groups.sync(request))?
                    ))
                })
                .await?;
            match attempt {
                Ok(result) => break result,
                Err(waiting) => request = waiting,
            }
            // The preceding SyncGroup may still be committing the assignment.
            if timeout_at(deadline, progress).await.is_err() {
                return Ok(sync_error(Error::NotCoordinator));
            }
        };
        Ok(match result {
            Ok((receiver, _)) => match timeout(Duration::from_secs(300) + DEADLINE, receiver).await
            {
                Ok(Ok(response)) => response,
                _ => sync_error(Error::NotCoordinator),
            },
            Err(error) => sync_error(retryable(error)),
        })
    }

    pub async fn heartbeat(
        self: &Arc<Self>,
        request: messages::HeartbeatRequest,
    ) -> Result<messages::HeartbeatResponse> {
        let result = match self.group_term(request.group_id.0.as_str()).await? {
            Ok((key, term)) => self
                .call(move |engine| {
                    engine.with_group(key, term, |groups, _| groups.heartbeat(request))
                })
                .await?
                .and_then(|(result, _)| result),
            Err(error) => Err(error),
        };
        Ok(messages::HeartbeatResponse::default()
            .with_error_code(result.err().map_or(0, |error| error as i16)))
    }

    pub async fn leave_group(
        self: &Arc<Self>,
        request: messages::LeaveGroupRequest,
    ) -> Result<messages::LeaveGroupResponse> {
        let error_response =
            |error: Error| messages::LeaveGroupResponse::default().with_error_code(error as i16);
        let (key, term) = match self.group_term(request.group_id.0.as_str()).await? {
            Ok(location) => location,
            Err(error) => return Ok(error_response(error)),
        };
        let result = self
            .call(move |engine| {
                engine.with_group(key, term, |groups, _| {
                    group_protocol::leave_group(groups, request)
                })
            })
            .await?;
        Ok(match result {
            Ok((response, pending)) => {
                if let Some(receiver) = pending
                    && let Err(error) = reply(receiver).await
                {
                    return Ok(error_response(group_error(error)));
                }
                response
            }
            Err(error) => error_response(error),
        })
    }

    pub async fn offset_commit(
        self: &Arc<Self>,
        request: messages::OffsetCommitRequest,
    ) -> Result<messages::OffsetCommitResponse> {
        let (key, term) = match self.group_term(request.group_id.0.as_str()).await? {
            Ok(location) => location,
            Err(error) => return Ok(group_protocol::commit_error(request, error)),
        };
        let fallback = request.clone();
        let result = self
            .call(move |engine| {
                engine.with_group(key, term, |groups, topics| {
                    group_protocol::offset_commit(groups, request, |name| {
                        topics.get(name).map(|topic| (topic.id, topic.partitions))
                    })
                })
            })
            .await?;
        Ok(match result {
            Ok((mut response, pending)) => {
                if let Some(receiver) = pending
                    && let Err(error) = reply(receiver).await
                {
                    group_protocol::reject_uncommitted(&mut response, group_error(error));
                }
                response
            }
            Err(error) => group_protocol::commit_error(fallback, error),
        })
    }

    async fn group_barriers(
        self: &Arc<Self>,
        keys: impl FnOnce(&Engine) -> BTreeSet<GroupId> + Send + 'static,
    ) -> Result<BTreeMap<GroupId, Result<u64, Error>>> {
        let barriers = self
            .call(move |engine| {
                keys(engine)
                    .into_iter()
                    .map(|key| Ok((key, engine.group_barrier(key)?)))
                    .collect::<Result<Vec<_>>>()
            })
            .await?;
        let deadline = Instant::now() + DEADLINE;
        let mut terms = BTreeMap::new();
        for (key, barrier) in barriers {
            let result = match barrier {
                Ok((term, receiver)) => match timeout_at(deadline, receiver).await {
                    Ok(Ok(result)) => result.map(|()| term).map_err(group_error),
                    Ok(Err(_)) => Err(Error::NotCoordinator),
                    Err(_) => Err(Error::CoordinatorLoadInProgress),
                },
                Err(error) => Err(error),
            };
            terms.insert(key, result);
        }
        Ok(terms)
    }

    pub async fn offset_fetch(
        self: &Arc<Self>,
        request: messages::OffsetFetchRequest,
        version: i16,
    ) -> Result<messages::OffsetFetchResponse> {
        let keys: BTreeSet<_> = if version <= 7 {
            valid_id(request.group_id.0.as_str())
                .then(|| coordinator(request.group_id.0.as_str()))
                .into_iter()
                .collect()
        } else {
            request
                .groups
                .iter()
                .map(|group| group.group_id.0.as_str())
                .filter(|id| valid_id(id))
                .map(coordinator)
                .collect()
        };
        let terms = self.group_barriers(move |_| keys).await?;
        self.call(move |engine| {
            Ok(group_protocol::offset_fetch(
                request,
                version,
                |id| {
                    let key = coordinator(id);
                    let term = terms[&key]?;
                    Ok(engine.group_entries(key, term)?.get(id))
                },
                |name| {
                    engine
                        .topics
                        .get(name)
                        .map(|topic| (topic.id, topic.partitions))
                },
                |id| engine.names.get(id).map(String::as_str),
            ))
        })
        .await
    }

    pub async fn describe_groups(
        self: &Arc<Self>,
        request: messages::DescribeGroupsRequest,
        version: i16,
    ) -> Result<messages::DescribeGroupsResponse> {
        let keys = request
            .groups
            .iter()
            .map(|group| group.0.as_str())
            .filter(|id| valid_id(id))
            .map(coordinator)
            .collect();
        let terms = self.group_barriers(move |_| keys).await?;
        self.call(move |engine| {
            Ok(group_protocol::describe_groups(request, version, |id| {
                let key = coordinator(id);
                let term = terms[&key]?;
                Ok(engine.active_groups(key, term)?.entries.get(id))
            }))
        })
        .await
    }

    pub async fn list_groups(
        self: &Arc<Self>,
        request: messages::ListGroupsRequest,
    ) -> Result<messages::ListGroupsResponse> {
        let terms = self.group_barriers(Engine::local_group_shards).await?;
        self.call(move |engine| {
            let mut groups = Vec::new();
            for (key, term) in terms {
                match term.and_then(|term| engine.active_groups(key, term)) {
                    Ok(shard) => groups.push(shard),
                    Err(error) => {
                        return Ok(
                            messages::ListGroupsResponse::default().with_error_code(error as i16)
                        );
                    }
                }
            }
            Ok(group_protocol::list_groups(
                request,
                groups.into_iter().flat_map(|shard| &shard.entries),
            ))
        })
        .await
    }
}
