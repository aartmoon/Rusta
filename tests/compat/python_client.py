import sys
import time
from pathlib import Path

import confluent_kafka
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType


def consume(seeds, topic, offset, value):
    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": seeds,
        "group.id": topic,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    try:
        consumer.subscribe([topic])
        message = consumer.poll(20)
        assert message is not None, "fetch timed out"
        assert message.error() is None, message.error()
        assert (message.offset(), message.value()) == (offset, value), "incorrect resume offset or payload"
        consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


def main(seeds, topic, shared):
    admin = AdminClient({"bootstrap.servers": seeds})
    admin.create_topics([NewTopic(topic, 1, 3)])[topic].result(20)
    topic_info = admin.list_topics(timeout=10).topics[topic]
    assert topic_info.error is None and len(topic_info.partitions[0].replicas) == 3
    resource = ConfigResource(ResourceType.TOPIC, topic)
    configs = admin.describe_configs([resource])[resource].result(20)
    assert configs["retention.ms"].value == "-1"
    producer = confluent_kafka.Producer({
        "bootstrap.servers": seeds,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 20000,
    })

    def write(value, offset):
        outcome = []
        producer.produce(topic, value, on_delivery=lambda error, msg: outcome.append((error, msg)))
        assert producer.flush(25) == 0 and len(outcome) == 1
        error, message = outcome[0]
        assert error is None, error
        assert message.offset() == offset, message.offset()

    write(b"first", 0)
    consume(seeds, topic, 0, b"first")
    Path(shared, topic + ".ready").touch()
    deadline = time.monotonic() + 30
    while not Path(shared, topic + ".go").exists():
        assert time.monotonic() < deadline, "waited for broker failure"
        time.sleep(0.02)
    write(b"second", 1)
    consume(seeds, topic, 1, b"second")
    admin.delete_topics([topic])[topic].result(20)
    print("confluent-kafka", confluent_kafka.__version__, "librdkafka", confluent_kafka.libversion()[0],
          "create/list/describe/delete, idempotent produce across failover, fetch and group offset verified")


if __name__ == "__main__":
    if sys.argv[1] == "leader":
        seeds, topic = sys.argv[2:]
        metadata = AdminClient({"bootstrap.servers": seeds}).list_topics(topic, timeout=10)
        print(metadata.topics[topic].partitions[0].leader)
    else:
        main(*sys.argv[1:])
