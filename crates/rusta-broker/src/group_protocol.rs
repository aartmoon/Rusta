use std::collections::{BTreeMap, HashSet};

use rusta_kafka::{
    messages::{self, GroupId, TopicName},
    protocol::StrBytes,
};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::groups::{Group, Groups, Key, Offset, OffsetBudget, Phase, valid_id};
use crate::protocol::{BROKER, Endpoint, Error};

const FETCH_OFFSET_BUDGET: usize = 3 * 1024 * 1024;

fn code(result: Result<(), Error>) -> i16 {
    result.err().map_or(0, |error| error as i16)
}

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}

impl Catalog {
    pub fn find_coordinator(
        request: messages::FindCoordinatorRequest,
        version: i16,
        endpoint: &Endpoint,
    ) -> messages::FindCoordinatorResponse {
        find_coordinator(request, version, |_| Ok((BROKER, endpoint)))
    }

    pub fn heartbeat(
        &mut self,
        request: messages::HeartbeatRequest,
    ) -> messages::HeartbeatResponse {
        messages::HeartbeatResponse::default().with_error_code(code(self.groups.heartbeat(request)))
    }

    pub fn leave_group(
        &mut self,
        request: messages::LeaveGroupRequest,
    ) -> messages::LeaveGroupResponse {
        leave_group(&mut self.groups, request)
    }

    pub fn offset_commit(
        &mut self,
        request: messages::OffsetCommitRequest,
    ) -> messages::OffsetCommitResponse {
        let topics = &self.topics;
        offset_commit(&mut self.groups, request, |name| {
            topics
                .get(name)
                .map(|topic| (topic.config.id, topic.config.partitions))
        })
    }

    pub fn offset_fetch(
        &self,
        request: messages::OffsetFetchRequest,
        version: i16,
    ) -> messages::OffsetFetchResponse {
        offset_fetch(
            request,
            version,
            |id| Ok(self.groups.entries.get(id)),
            |name| {
                self.topics
                    .get(name)
                    .map(|topic| (topic.config.id, topic.config.partitions))
            },
            |id| self.names_by_id.get(id).map(String::as_str),
        )
    }

    pub fn describe_groups(
        &self,
        request: messages::DescribeGroupsRequest,
        version: i16,
    ) -> messages::DescribeGroupsResponse {
        describe_groups(request, version, |id| Ok(self.groups.entries.get(id)))
    }

    pub fn list_groups(
        &self,
        request: messages::ListGroupsRequest,
    ) -> messages::ListGroupsResponse {
        list_groups(request, self.groups.entries.iter())
    }
}

pub fn find_coordinator<'a>(
    request: messages::FindCoordinatorRequest,
    version: i16,
    resolve_endpoint: impl Fn(&str) -> Result<(messages::BrokerId, &'a Endpoint), Error>,
) -> messages::FindCoordinatorResponse {
    use messages::find_coordinator_response::Coordinator;
    let resolve = |key: &StrBytes| {
        if request.key_type != 0 {
            return Err(Error::UnsupportedVersion);
        }
        if !valid_id(key.as_str()) {
            return Err(Error::InvalidGroupId);
        }
        resolve_endpoint(key.as_str())
    };
    let mut response = messages::FindCoordinatorResponse::default();
    if version == 3 {
        match resolve(&request.key) {
            Ok((node, endpoint)) => {
                response.node_id = node;
                response.host = endpoint.host.clone();
                response.port = endpoint.port;
            }
            Err(error) => response.error_code = error as i16,
        }
    } else {
        response.coordinators = request
            .coordinator_keys
            .into_iter()
            .map(|key| {
                let result = resolve(&key);
                let mut coordinator = Coordinator::default().with_key(key);
                match result {
                    Ok((node, endpoint)) => {
                        coordinator.node_id = node;
                        coordinator.host = endpoint.host.clone();
                        coordinator.port = endpoint.port;
                    }
                    Err(error) => coordinator.error_code = error as i16,
                }
                coordinator
            })
            .collect();
    }
    response
}

pub fn leave_group(
    groups: &mut Groups,
    request: messages::LeaveGroupRequest,
) -> messages::LeaveGroupResponse {
    use messages::leave_group_response::MemberResponse;
    let id = request.group_id.0.as_str();
    if !valid_id(id) {
        return messages::LeaveGroupResponse::default()
            .with_error_code(Error::InvalidGroupId as i16);
    }
    let members = request
        .members
        .into_iter()
        .map(|member| {
            let result = groups.leave(
                id,
                member.member_id.as_str(),
                member.group_instance_id.is_some(),
            );
            MemberResponse::default()
                .with_member_id(member.member_id)
                .with_group_instance_id(member.group_instance_id)
                .with_error_code(code(result))
        })
        .collect();
    messages::LeaveGroupResponse::default().with_members(members)
}

pub fn offset_commit(
    groups: &mut Groups,
    request: messages::OffsetCommitRequest,
    topic_lookup: impl Fn(&str) -> Option<(Uuid, usize)>,
) -> messages::OffsetCommitResponse {
    use messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    let id = request.group_id.0.as_str();
    let validation = groups.validate_commit(
        id,
        request.member_id.as_str(),
        request.generation_id_or_member_epoch,
        request.group_instance_id.is_some(),
    );
    let empty = Default::default();
    let mut budget = OffsetBudget::new(
        groups
            .entries
            .get(id)
            .map_or(&empty, |group| &group.offsets),
    );
    let mut accepted = Vec::new();
    let mut topics: Vec<_> = request
        .topics
        .into_iter()
        .map(|topic| {
            let resolved = topic_lookup(topic.name.0.as_str());
            let partitions = topic
                .partitions
                .into_iter()
                .map(|partition| {
                    let result = (|| {
                        validation?;
                        let topic = resolved.ok_or(Error::UnknownTopic)?;
                        if !(0..topic.1 as i32).contains(&partition.partition_index) {
                            return Err(Error::UnknownTopic);
                        }
                        let key = Key {
                            topic: topic.0,
                            partition: partition.partition_index,
                        };
                        let offset = Offset {
                            value: partition.committed_offset,
                            leader_epoch: partition.committed_leader_epoch,
                            metadata: partition
                                .committed_metadata
                                .map(|metadata| metadata.as_str().to_owned()),
                        };
                        budget.admit(key, &offset)?;
                        accepted.push((key, offset));
                        Ok(())
                    })();
                    OffsetCommitResponsePartition::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(code(result))
                })
                .collect();
            OffsetCommitResponseTopic::default()
                .with_name(topic.name)
                .with_partitions(partitions)
        })
        .collect();
    if let Err(error) = groups.commit(id, accepted) {
        for partition in topics.iter_mut().flat_map(|topic| &mut topic.partitions) {
            if partition.error_code == 0 {
                partition.error_code = error as i16;
            }
        }
    }
    messages::OffsetCommitResponse::default().with_topics(topics)
}

pub fn offset_fetch<'a>(
    request: messages::OffsetFetchRequest,
    version: i16,
    lookup_group: impl Fn(&str) -> Result<Option<&'a Group>, Error>,
    lookup_topic: impl Fn(&str) -> Option<(Uuid, usize)>,
    lookup_name: impl Fn(&Uuid) -> Option<&'a str>,
) -> messages::OffsetFetchResponse {
    use messages::offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    let mut budget = FETCH_OFFSET_BUDGET;
    let mut seen = HashSet::new();
    if version <= 7 {
        let filter = request.topics.map(|topics| {
            topics
                .into_iter()
                .map(|topic| (topic.name, topic.partition_indexes))
                .collect()
        });
        return match fetch_group_offsets(
            &request.group_id,
            filter,
            &mut budget,
            &lookup_group,
            &lookup_topic,
            &lookup_name,
        ) {
            Ok(results) => messages::OffsetFetchResponse::default().with_topics(
                results
                    .into_iter()
                    .map(|topic| {
                        let partitions = topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                OffsetFetchResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_committed_offset(partition.committed_offset)
                                    .with_committed_leader_epoch(partition.committed_leader_epoch)
                                    .with_metadata(partition.metadata)
                                    .with_error_code(partition.error_code)
                            })
                            .collect();
                        OffsetFetchResponseTopic::default()
                            .with_name(topic.name)
                            .with_partitions(partitions)
                    })
                    .collect(),
            ),
            Err(error) => messages::OffsetFetchResponse::default().with_error_code(error as i16),
        };
    }
    let groups = request
        .groups
        .into_iter()
        .map(|group| {
            let result = if seen.insert(group.group_id.clone()) {
                let filter = group.topics.map(|topics| {
                    topics
                        .into_iter()
                        .map(|topic| (topic.name, topic.partition_indexes))
                        .collect()
                });
                fetch_group_offsets(
                    &group.group_id,
                    filter,
                    &mut budget,
                    &lookup_group,
                    &lookup_topic,
                    &lookup_name,
                )
            } else {
                Err(Error::InvalidRequest)
            };
            let response = OffsetFetchResponseGroup::default().with_group_id(group.group_id);
            match result {
                Ok(topics) => response.with_topics(topics),
                Err(error) => response.with_error_code(error as i16),
            }
        })
        .collect();
    messages::OffsetFetchResponse::default().with_groups(groups)
}

fn fetch_group_offsets<'a>(
    id: &GroupId,
    filter: Option<Vec<(TopicName, Vec<i32>)>>,
    response_budget: &mut usize,
    lookup_group: &impl Fn(&str) -> Result<Option<&'a Group>, Error>,
    lookup_topic: &impl Fn(&str) -> Option<(Uuid, usize)>,
    lookup_name: &impl Fn(&Uuid) -> Option<&'a str>,
) -> Result<Vec<messages::offset_fetch_response::OffsetFetchResponseTopics>, Error> {
    use messages::offset_fetch_response::{
        OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
    };
    if !valid_id(id.0.as_str()) {
        return Err(Error::InvalidGroupId);
    }
    let stored = lookup_group(id.0.as_str())?.map(|group| &group.offsets);
    let filter = filter.unwrap_or_else(|| {
        let mut topics = BTreeMap::<&str, Vec<i32>>::new();
        if let Some(stored) = stored {
            for key in stored.keys() {
                if let Some(name) = lookup_name(&key.topic) {
                    topics.entry(name).or_default().push(key.partition);
                }
            }
        }
        topics
            .into_iter()
            .map(|(name, partitions)| (TopicName(text(name)), partitions))
            .collect()
    });
    let mut budget = *response_budget;
    let topics = filter
        .into_iter()
        .map(|(name, indexes)| {
            budget = budget
                .checked_sub(name.0.len() + 16)
                .ok_or(Error::PolicyViolation)?;
            let topic = lookup_topic(name.0.as_str());
            let partitions = indexes
                .into_iter()
                .map(|index| {
                    budget = budget.checked_sub(32).ok_or(Error::PolicyViolation)?;
                    let partition =
                        OffsetFetchResponsePartitions::default().with_partition_index(index);
                    let Some(topic) = topic.filter(|topic| (0..topic.1 as i32).contains(&index))
                    else {
                        return Ok(partition.with_error_code(Error::UnknownTopic as i16));
                    };
                    if let Some(offset) = stored.and_then(|offsets| {
                        offsets.get(&Key {
                            topic: topic.0,
                            partition: index,
                        })
                    }) {
                        budget = budget
                            .checked_sub(offset.metadata.as_ref().map_or(0, String::len))
                            .ok_or(Error::PolicyViolation)?;
                        return Ok(partition
                            .with_committed_offset(offset.value)
                            .with_committed_leader_epoch(offset.leader_epoch)
                            .with_metadata(offset.metadata.as_deref().map(text)));
                    }
                    Ok(partition)
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(OffsetFetchResponseTopics::default()
                .with_name(name)
                .with_partitions(partitions))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    *response_budget = budget;
    Ok(topics)
}

pub fn describe_groups<'a>(
    request: messages::DescribeGroupsRequest,
    version: i16,
    lookup_group: impl Fn(&str) -> Result<Option<&'a Group>, Error>,
) -> messages::DescribeGroupsResponse {
    use messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
    let mut seen = HashSet::new();
    let groups = request
        .groups
        .into_iter()
        .map(|id| {
            let mut response = DescribedGroup::default().with_group_id(id.clone());
            if !valid_id(id.0.as_str()) || !seen.insert(id.clone()) {
                return response.with_error_code(Error::InvalidGroupId as i16);
            }
            let group = match lookup_group(id.0.as_str()) {
                Ok(group) => group,
                Err(error) => return response.with_error_code(error as i16),
            };
            if let Some(group) = group {
                response.group_state = text(group.state_name());
                response.protocol_type = text(group.protocol_type());
                if matches!(group.phase, Phase::Stable) {
                    let selected = group.protocol.as_deref().unwrap_or_default();
                    response.protocol_data = text(selected);
                    response.members = group
                        .members
                        .iter()
                        .map(|(id, member)| {
                            DescribedGroupMember::default()
                                .with_member_id(text(id))
                                .with_client_id(text(&member.client_id))
                                .with_client_host(text(&member.client_host))
                                .with_member_metadata(
                                    member.metadata(selected).cloned().unwrap_or_default(),
                                )
                                .with_member_assignment(member.assignment.clone())
                        })
                        .collect();
                }
            } else {
                response.group_state = text("Dead");
                if version >= 6 {
                    response.error_code = Error::GroupIdNotFound as i16;
                }
            }
            response
        })
        .collect();
    messages::DescribeGroupsResponse::default().with_groups(groups)
}

pub fn list_groups<'a>(
    request: messages::ListGroupsRequest,
    entries: impl Iterator<Item = (&'a String, &'a Group)>,
) -> messages::ListGroupsResponse {
    use messages::list_groups_response::ListedGroup;
    if !request.types_filter.is_empty()
        && !request
            .types_filter
            .iter()
            .any(|kind| kind.as_str().eq_ignore_ascii_case("Classic"))
    {
        return messages::ListGroupsResponse::default();
    }
    let groups = entries
        .filter_map(|(id, group)| {
            if group.generation == 0 && group.offsets.is_empty() {
                return None;
            }
            let state = group.state_name();
            if !request.states_filter.is_empty()
                && !request
                    .states_filter
                    .iter()
                    .any(|filter| filter.as_str().eq_ignore_ascii_case(state))
            {
                return None;
            }
            Some(
                ListedGroup::default()
                    .with_group_id(GroupId(text(id)))
                    .with_protocol_type(text(group.protocol_type()))
                    .with_group_state(text(state))
                    .with_group_type(text("Classic")),
            )
        })
        .collect();
    messages::ListGroupsResponse::default().with_groups(groups)
}

pub fn commit_error(
    request: messages::OffsetCommitRequest,
    error: Error,
) -> messages::OffsetCommitResponse {
    use messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    messages::OffsetCommitResponse::default().with_topics(
        request
            .topics
            .into_iter()
            .map(|topic| {
                OffsetCommitResponseTopic::default()
                    .with_name(topic.name)
                    .with_partitions(
                        topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_error_code(error as i16)
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

pub fn reject_uncommitted(response: &mut messages::OffsetCommitResponse, error: Error) {
    for partition in response
        .topics
        .iter_mut()
        .flat_map(|topic| &mut topic.partitions)
    {
        if partition.error_code == 0 {
            partition.error_code = error as i16;
        }
    }
}
