use std::collections::BTreeSet;

use rusta_kafka::{
    messages::{
        self, TopicName,
        delete_topics_response::DeletableTopicResult,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
        },
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
    protocol::StrBytes,
};
use uuid::Uuid;

use crate::{catalog::TopicConfig, protocol::Error};

const TOPIC: i8 = 2;
const TOPIC_CONFIG: i8 = 1;
const DEFAULT_CONFIG: i8 = 5;

pub fn deletes(
    request: messages::DeleteTopicsRequest,
) -> impl Iterator<Item = DeletableTopicResult> {
    request
        .topic_names
        .into_iter()
        .map(|name| DeletableTopicResult::default().with_name(Some(name)))
        .chain(request.topics.into_iter().map(|topic| {
            DeletableTopicResult::default()
                .with_name(topic.name)
                .with_topic_id(topic.topic_id)
        }))
}

pub fn resolve_delete<'a>(
    target: &DeletableTopicResult,
    by_name: impl FnOnce(&str) -> Option<&'a TopicConfig>,
    by_id: impl FnOnce(Uuid) -> Option<&'a TopicConfig>,
) -> Result<&'a TopicConfig, Error> {
    match (&target.name, target.topic_id.is_nil()) {
        (Some(name), true) => by_name(name.0.as_str()).ok_or(Error::UnknownTopic),
        (None, false) => by_id(target.topic_id).ok_or(Error::UnknownTopicId),
        _ => Err(Error::InvalidRequest),
    }
}

pub fn confirm_delete(target: &mut DeletableTopicResult, topic: &TopicConfig, version: i16) {
    target.name = Some(TopicName(text(&topic.name)));
    if version >= 6 {
        target.topic_id = topic.id;
    }
}

pub fn describe<'a>(
    request: messages::DescribeConfigsRequest,
    version: i16,
    default_retention: i64,
    lookup: impl Fn(&str) -> Option<&'a TopicConfig>,
) -> messages::DescribeConfigsResponse {
    let mut results = Vec::with_capacity(request.resources.len());
    for resource in request.resources {
        let mut response = DescribeConfigsResult::default()
            .with_resource_type(resource.resource_type)
            .with_resource_name(resource.resource_name.clone());
        let topic = topic_resource(
            resource.resource_type,
            lookup(resource.resource_name.as_str()),
        );
        let configs = topic.and_then(|topic| {
            let settings = [
                Setting {
                    name: "retention.ms",
                    value: topic.retention_ms.map(|limit| limit.to_string()),
                    default: default_retention.to_string(),
                    kind: 5,
                    read_only: false,
                    documentation: "Maximum retained age in milliseconds. -1 disables time retention.",
                },
                Setting {
                    name: "retention.bytes",
                    value: topic.retention_bytes.map(|limit| limit.to_string()),
                    default: "-1".into(),
                    kind: 5,
                    read_only: false,
                    documentation: "Retained bytes per partition. -1 disables size retention.",
                },
                Setting {
                    name: "cleanup.policy",
                    value: None,
                    default: "delete".into(),
                    kind: 7,
                    read_only: true,
                    documentation: "Retire complete segments according to retention settings.",
                },
                Setting {
                    name: "rusta.allow.weak.acks",
                    value: topic.allow_weak_acks.then(|| "true".to_owned()),
                    default: "false".into(),
                    kind: 1,
                    read_only: true,
                    documentation: "Permit weaker acknowledgements on a singleton topic.",
                },
            ];
            if let Some(keys) = &resource.configuration_keys {
                for key in keys {
                    if !settings.iter().any(|setting| setting.name == key.as_str()) {
                        return Err(Error::InvalidConfig);
                    }
                }
            }
            Ok(settings
                .into_iter()
                .filter(|setting| resource.configuration_keys.as_ref().is_none_or(|keys| {
                    keys.iter().any(|key| key.as_str() == setting.name)
                }))
                .map(|setting| setting.describe(version, request.include_synonyms, request.include_documentation))
                .collect())
        });
        match configs {
            Ok(configs) => response.configs = configs,
            Err(error) => response.error_code = error as i16,
        }
        results.push(response);
    }
    messages::DescribeConfigsResponse::default().with_results(results)
}

struct Setting {
    name: &'static str,
    value: Option<String>,
    default: String,
    kind: i8,
    read_only: bool,
    documentation: &'static str,
}

impl Setting {
    fn describe(
        self,
        version: i16,
        synonyms: bool,
        documentation: bool,
    ) -> DescribeConfigsResourceResult {
        let source = if self.value.is_some() {
            TOPIC_CONFIG
        } else {
            DEFAULT_CONFIG
        };
        let mut result = DescribeConfigsResourceResult::default()
            .with_name(text(self.name))
            .with_value(Some(text(self.value.as_deref().unwrap_or(&self.default))))
            .with_config_source(source)
            .with_config_type(if version >= 3 { self.kind } else { 0 })
            .with_read_only(self.read_only);
        if version >= 3 && documentation {
            result.documentation = Some(text(self.documentation));
        }
        if synonyms {
            if let Some(value) = &self.value {
                result.synonyms.push(
                    DescribeConfigsSynonym::default()
                        .with_name(text(self.name))
                        .with_value(Some(text(value)))
                        .with_source(TOPIC_CONFIG),
                );
            }
            result.synonyms.push(
                DescribeConfigsSynonym::default()
                    .with_name(text(self.name))
                    .with_value(Some(text(&self.default)))
                    .with_source(DEFAULT_CONFIG),
            );
        }
        result
    }
}

pub fn create_response(
    mut response: messages::CreateTopicsResponse,
    version: i16,
) -> messages::CreateTopicsResponse {
    for topic in &mut response.topics {
        if version < 7 {
            topic.topic_id = Uuid::nil();
        }
        if version < 5 {
            topic.num_partitions = -1;
            topic.replication_factor = -1;
            topic.configs = None;
        }
    }
    response
}

pub fn alter(
    resource: &AlterConfigsResource,
    topic: Option<&TopicConfig>,
) -> Result<TopicConfig, Error> {
    let mut topic = topic_resource(resource.resource_type, topic)?.clone();
    let mut names = BTreeSet::new();
    for config in &resource.configs {
        let key = config.name.as_str();
        if !names.insert(key) {
            return Err(Error::InvalidConfig);
        }
        let value = match (config.config_operation, config.value.as_ref()) {
            (0, Some(value)) => Some(retention_value(value.as_str())?),
            (1, None) => None,
            _ => return Err(Error::InvalidConfig),
        };
        match key {
            "retention.ms" => topic.retention_ms = value,
            "retention.bytes" => topic.retention_bytes = value,
            _ => return Err(Error::InvalidConfig),
        }
    }
    Ok(topic)
}

pub fn alter_result(resource: &AlterConfigsResource) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse::default()
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
}

pub fn retention_value(value: &str) -> Result<i64, Error> {
    value
        .parse::<i64>()
        .ok()
        .filter(|&v| v >= -1)
        .ok_or(Error::InvalidConfig)
}

fn topic_resource(kind: i8, topic: Option<&TopicConfig>) -> Result<&TopicConfig, Error> {
    if kind != TOPIC {
        return Err(Error::InvalidRequest);
    }
    topic.ok_or(Error::UnknownTopic)
}

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}
