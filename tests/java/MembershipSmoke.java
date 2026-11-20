import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

public final class MembershipSmoke {
    static final String ORIGINAL = "before-node-four";
    static final String EXPANDED = "after-node-four";
    static final String GROUP = "membership-rebalance";
    static final int PARTITIONS = 8;
    static final int BACKLOG_RECORDS = 7;
    static final String BACKLOG_VALUE = "b".repeat(700 * 1024);

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String endpoint = args[1];
        switch (mode) {
            case "setup" -> {
                waitForNodes(endpoint, Set.of(1, 2, 3));
                create(endpoint, ORIGINAL, PARTITIONS, 3);
                try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
                    for (int partition = 0; partition < PARTITIONS; partition++) {
                        var record = new ProducerRecord<String, String>(ORIGINAL, partition, null, "before-" + partition);
                        GroupSmoke.check(producer.send(record).get(10, TimeUnit.SECONDS).offset() == 0,
                            "unexpected offset before expansion");
                    }
                    for (int offset = 1; offset < BACKLOG_RECORDS; offset++) {
                        var record = new ProducerRecord<String, String>(ORIGINAL, 1, null, BACKLOG_VALUE);
                        GroupSmoke.check(producer.send(record).get(10, TimeUnit.SECONDS).offset() == offset,
                            "unexpected offset in relocation backlog");
                    }
                }
                var partition = new TopicPartition(ORIGINAL, 0);
                try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(endpoint, GROUP))) {
                    consumer.commitSync(Map.of(partition, new OffsetAndMetadata(1, "before-rebalance")),
                        GroupSmoke.TIMEOUT);
                }
                var topicId = describe(endpoint).data().topics().find(ORIGINAL).topicId();
                Files.writeString(Path.of(args[2]).resolve("original-id"), topicId.toString());
            }
            case "traffic" -> {
                var scratch = Path.of(args[2]);
                try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
                    long deadline = System.nanoTime() + Duration.ofSeconds(85).toNanos();
                    int offset = 1;
                    while (!Files.exists(scratch.resolve("stop")) && System.nanoTime() < deadline) {
                        var record = new ProducerRecord<String, String>(ORIGINAL, 0, null, "during-" + offset);
                        GroupSmoke.check(producer.send(record).get(8, TimeUnit.SECONDS).offset() == offset, "traffic interrupted");
                        if (offset++ == 1) Files.writeString(scratch.resolve("ready"), "ready");
                        Thread.sleep(75);
                    }
                    GroupSmoke.check(offset > 2 && Files.exists(scratch.resolve("stop")), "missing concurrent traffic");
                    Files.writeString(scratch.resolve("committed"), Integer.toString(offset));
                }
            }
            case "expanded" -> {
                waitForNodes(endpoint, Set.of(1, 2, 3, 4));
                create(endpoint, EXPANDED, 1, 4);
                write(endpoint, EXPANDED, "after", 0);
                try (var admin = Admin.create(GroupSmoke.base(endpoint))) {
                    var oldTopic = admin.describeTopics(List.of(ORIGINAL)).allTopicNames().get(10, TimeUnit.SECONDS).get(ORIGINAL);
                    GroupSmoke.check(oldTopic.partitions().get(0).replicas().size() == 3, "old replication factor changed");
                }
            }
            case "controller" -> {
                var scratch = Path.of(args[2]);
                long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
                int controller;
                do {
                    controller = describe(endpoint).data().controllerId();
                    if (controller > 0) break;
                    Thread.sleep(100);
                } while (System.nanoTime() < deadline);
                GroupSmoke.check(controller > 0, "controller unavailable");
                Files.writeString(scratch.resolve("controller"), Integer.toString(controller));
            }
            case "leadership" -> {
                var scratch = Path.of(args[2]);
                var partition = describe(endpoint).data().topics().find(ORIGINAL).partitions().getFirst();
                int leader = partition.leaderId();
                int target = partition.replicaNodes().stream().filter(id -> id != leader).findFirst().orElseThrow();
                GroupSmoke.check(leader > 0, "leader unavailable");
                Files.write(scratch.resolve("transfer"), List.of(Integer.toString(leader), Integer.toString(target)));
            }
            case "transferred" -> {
                var lines = Files.readAllLines(Path.of(args[2]).resolve("transfer"));
                int target = Integer.parseInt(lines.get(1));
                long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
                do {
                    var partition = describe(endpoint).data().topics().find(ORIGINAL).partitions().getFirst();
                    if (partition.leaderId() == target) {
                        System.out.println("Manual leadership transfer passed");
                        return;
                    }
                    Thread.sleep(100);
                } while (System.nanoTime() < deadline);
                throw new AssertionError("requested replica did not become leader");
            }
            case "verify" -> {
                var scratch = Path.of(args[2]);
                waitForNodes(endpoint, Set.of(1, 2, 3, 4));
                int end = Integer.parseInt(Files.readString(scratch.resolve("committed")));
                var data = describe(endpoint).data().topics().find(ORIGINAL);
                GroupSmoke.check(data.topicId().toString().equals(Files.readString(scratch.resolve("original-id"))),
                    "topic ID changed during relocation");
                GroupSmoke.check(data.partitions().stream().allMatch(p -> p.replicaNodes().size() == 3)
                    && data.partitions().stream().anyMatch(p -> p.replicaNodes().contains(4)),
                    "existing partitions did not rebalance with RF3");
                verifyOffsets(endpoint, ORIGINAL, 0, end);
                for (int partition = 1; partition < PARTITIONS; partition++) {
                    verifyOffsets(endpoint, ORIGINAL, partition, partition == 1 ? BACKLOG_RECORDS : 1);
                }
                var partition = new TopicPartition(ORIGINAL, 0);
                try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(endpoint, GROUP))) {
                    var offset = consumer.committed(Set.of(partition), GroupSmoke.TIMEOUT).get(partition);
                    GroupSmoke.check(offset != null && offset.offset() == 1
                        && "before-rebalance".equals(offset.metadata()), "group offset was lost");
                }
                write(endpoint, EXPANDED, "after-restart", 1);
                verifyOffsets(endpoint, EXPANDED, 0, 2);
            }
            default -> throw new IllegalArgumentException(mode);
        }
        System.out.println("Hot membership " + mode + " passed");
    }

    private static void create(String endpoint, String topic, int partitions, int replicas) throws Exception {
        try (var admin = Admin.create(GroupSmoke.base(endpoint))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) replicas))).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void write(String endpoint, String topic, String value, long offset) throws Exception {
        try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
            var record = new ProducerRecord<String, String>(topic, 0, null, value);
            GroupSmoke.check(producer.send(record).get(10, TimeUnit.SECONDS).offset() == offset, "unexpected offset");
        }
    }

    private static void waitForNodes(String endpoint, Set<Integer> expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            var response = describe(endpoint);
            var nodes = response.data().brokers().stream().map(broker -> broker.nodeId())
                .collect(java.util.stream.Collectors.toSet());
            if (nodes.equals(expected) && response.data().controllerId() > 0) return;
            Thread.sleep(100);
        }
        throw new AssertionError("broker did not learn committed membership " + expected);
    }

    private static MetadataResponse describe(String endpoint) throws Exception {
        return (MetadataResponse) Smoke.exchange(endpoint, MetadataRequest.Builder.allTopics().build((short) 12), 1);
    }

    private static void verifyOffsets(String endpoint, String topic, int number, int expected) {
        var properties = GroupSmoke.consumer(endpoint, "unused");
        properties.remove(ConsumerConfig.GROUP_ID_CONFIG);
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            var partition = new TopicPartition(topic, number);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var offsets = new java.util.ArrayList<Long>();
            GroupSmoke.until(() -> offsets.size() >= expected, () -> consumer.poll(Duration.ofMillis(100))
                .forEach(record -> {
                    long offset = record.offset();
                    String value;
                    if (topic.equals(EXPANDED)) value = offset == 0 ? "after" : "after-restart";
                    else if (offset == 0) value = "before-" + number;
                    else if (number == 0) value = "during-" + offset;
                    else value = BACKLOG_VALUE;
                    GroupSmoke.check(value.equals(record.value()), "record changed during relocation");
                    offsets.add(offset);
                }));
            GroupSmoke.check(offsets.size() == expected, "wrong record count");
            for (int i = 0; i < expected; i++) {
                GroupSmoke.check(offsets.get(i) == i, "offset gap after membership change");
            }
        }
    }
}
