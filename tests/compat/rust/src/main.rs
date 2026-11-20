use std::{error::Error, path::Path, time::Duration};

use rdkafka::{
    Message,
    admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::{KafkaError, RDKafkaErrorCode},
    producer::{FutureProducer, FutureRecord},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const WAIT: Duration = Duration::from_secs(20);

fn settings(seeds: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", seeds);
    config
}

async fn consume(seeds: &str, topic: &str, expected: i64, value: &str) -> Result<()> {
    let consumer: StreamConsumer = settings(seeds)
        .set("group.id", topic)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[topic])?;
    let message = tokio::time::timeout(WAIT, async {
        loop {
            match consumer.recv().await {
                Err(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::Resolve
                    | RDKafkaErrorCode::BrokerTransportFailure
                    | RDKafkaErrorCode::AllBrokersDown,
                )) => continue,
                result => return result,
            }
        }
    })
    .await??;
    assert_eq!(message.offset(), expected);
    assert_eq!(message.payload(), Some(value.as_bytes()));
    consumer.commit_message(&message, CommitMode::Sync)?;
    Ok(())
}

async fn write(producer: &FutureProducer, topic: &str, value: &str, expected: i64) -> Result<()> {
    let record = FutureRecord::<str, str>::to(topic).payload(value);
    let delivery = producer
        .send(record, WAIT)
        .await
        .map_err(|(error, _)| error)?;
    assert_eq!(delivery.offset, expected);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let seeds = &args[1];
    let topic = &args[2];
    let shared = Path::new(&args[3]);
    let admin: AdminClient<DefaultClientContext> = settings(seeds).create()?;
    let results = admin
        .create_topics(
            [NewTopic::new(topic, 1, TopicReplication::Fixed(3))].iter(),
            &AdminOptions::new().operation_timeout(Some(WAIT)),
        )
        .await?;
    for result in results {
        result.map_err(|(name, code)| format!("create {name}: {code:?}"))?;
    }
    let metadata = admin.inner().fetch_metadata(None, WAIT)?;
    let described = metadata
        .topics()
        .iter()
        .find(|item| item.name() == topic)
        .expect("topic listed");
    assert_eq!(described.partitions().len(), 1);
    assert_eq!(described.partitions()[0].replicas().len(), 3);
    for config in admin
        .describe_configs(
            [ResourceSpecifier::Topic(topic)].iter(),
            &AdminOptions::new(),
        )
        .await?
    {
        let config = config.map_err(|error| format!("describe {topic}: {error:?}"))?;
        let retention = config
            .entries
            .iter()
            .find(|entry| entry.name == "retention.ms")
            .expect("retention config");
        assert_eq!(retention.value.as_deref(), Some("-1"));
    }

    let producer: FutureProducer = settings(seeds)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "20000")
        .create()?;
    write(&producer, topic, "first", 0).await?;
    consume(seeds, topic, 0, "first").await?;
    std::fs::write(shared.join(format!("{topic}.ready")), b"ready\n")?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !shared.join(format!("{topic}.go")).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    write(&producer, topic, "second", 1).await?;
    consume(seeds, topic, 1, "second").await?;
    for result in admin
        .delete_topics(&[topic], &AdminOptions::new().operation_timeout(Some(WAIT)))
        .await?
    {
        result.map_err(|(name, code)| format!("delete {name}: {code:?}"))?;
    }
    println!(
        "rust-rdkafka 0.38.0 librdkafka {} create/list/describe/delete, idempotent produce across failover, fetch and group offset verified",
        rdkafka::util::get_rdkafka_version().1
    );
    Ok(())
}
