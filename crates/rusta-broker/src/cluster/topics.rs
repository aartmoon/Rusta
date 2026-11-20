use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rusta_kafka::messages;
use tokio::time::{Instant, timeout_at};

use crate::{protocol::Error, topic_admin};

use super::{
    Cluster,
    kafka::{DEADLINE, reply},
};

impl Cluster {
    pub async fn delete_topics(
        self: &Arc<Self>,
        request: messages::DeleteTopicsRequest,
        version: i16,
    ) -> Result<messages::DeleteTopicsResponse> {
        let deadline =
            Instant::now() + DEADLINE.min(Duration::from_millis(request.timeout_ms.max(0) as u64));
        let mut responses = Vec::new();
        for target in topic_admin::deletes(request) {
            let proposed = self
                .call(move |engine| {
                    let mut target = target;
                    let result = if !engine.controller() {
                        Err(Error::NotController)
                    } else if Instant::now() >= deadline {
                        Err(Error::RequestTimedOut)
                    } else {
                        topic_admin::resolve_delete(
                            &target,
                            |name| engine.topics.get(name),
                            |id| {
                                engine
                                    .names
                                    .get(&id)
                                    .and_then(|name| engine.topics.get(name))
                            },
                        )
                        .cloned()
                    };
                    let proposal = match result {
                        Ok(topic) => {
                            topic_admin::confirm_delete(&mut target, &topic, version);
                            engine.delete_topic(topic)?
                        }
                        Err(error) => Err(error),
                    };
                    Ok((target, proposal))
                })
                .await?;
            let (mut target, proposed) = proposed;
            let outcome = match proposed {
                Ok(receiver) => timeout_at(deadline, reply(receiver))
                    .await
                    .unwrap_or(Err(Error::RequestTimedOut))
                    .map(|_| ()),
                Err(error) => Err(error),
            };
            if let Err(error) = outcome {
                target.error_code = controller_error(error) as i16;
            }
            responses.push(target);
        }
        Ok(messages::DeleteTopicsResponse::default().with_responses(responses))
    }

    pub async fn describe_configs(
        self: &Arc<Self>,
        request: messages::DescribeConfigsRequest,
        version: i16,
    ) -> Result<messages::DescribeConfigsResponse> {
        self.call(move |engine| {
            Ok(topic_admin::describe(request, version, -1, |name| {
                engine.topics.get(name)
            }))
        })
        .await
    }

    pub async fn alter_configs(
        self: &Arc<Self>,
        request: messages::IncrementalAlterConfigsRequest,
    ) -> Result<messages::IncrementalAlterConfigsResponse> {
        let validate = request.validate_only;
        let deadline = Instant::now() + DEADLINE;
        let mut responses = Vec::new();
        for resource in request.resources {
            let mut response = topic_admin::alter_result(&resource);
            let proposed = self
                .call(move |engine| {
                    if !engine.controller() {
                        return Ok(Err(Error::NotController));
                    }
                    if Instant::now() >= deadline {
                        return Ok(Err(Error::RequestTimedOut));
                    }
                    match topic_admin::alter(
                        &resource,
                        engine.topics.get(resource.resource_name.as_str()),
                    ) {
                        Ok(config) if !validate => {
                            engine.alter_topic(config).map(|result| result.map(Some))
                        }
                        Ok(_) => Ok(Ok(None)),
                        Err(error) => Ok(Err(error)),
                    }
                })
                .await?;
            let outcome = match proposed {
                Ok(Some(receiver)) => timeout_at(deadline, reply(receiver))
                    .await
                    .unwrap_or(Err(Error::RequestTimedOut))
                    .map(|_| ()),
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(error) = outcome {
                response.error_code = controller_error(error) as i16;
            }
            responses.push(response);
        }
        Ok(messages::IncrementalAlterConfigsResponse::default().with_responses(responses))
    }
}

fn controller_error(error: Error) -> Error {
    match error {
        Error::NotLeader => Error::NotController,
        other => other,
    }
}
