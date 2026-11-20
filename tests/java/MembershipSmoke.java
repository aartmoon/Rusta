import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

public final class MembershipSmoke {
    static final String ORIGINAL = "before-node-four";
    static final String EXPANDED = "after-node-four";

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String endpoint = args[1];
        switch (mode) {
            case "setup" -> {
                waitForNodes(endpoint, Set.of(1, 2, 3));
                create(endpoint, ORIGINAL, 3);
                write(endpoint, ORIGINAL, "before", 0);
            }
            case "traffic" -> {
                var scratch = Path.of(args[2]);
                try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
                    long deadline = System.nanoTime() + Duration.ofSeconds(50).toNanos();
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
                create(endpoint, EXPANDED, 4);
                write(endpoint, EXPANDED, "after", 0);
                try (var admin = Admin.create(GroupSmoke.base(endpoint))) {
                    var oldTopic = admin.describeTopics(List.of(ORIGINAL)).allTopicNames().get(10, TimeUnit.SECONDS).get(ORIGINAL);
                    GroupSmoke.check(oldTopic.partitions().get(0).replicas().size() == 3, "old replication factor changed");
                }
            }
            case "verify" -> {
                var scratch = Path.of(args[2]);
                waitForNodes(endpoint, Set.of(1, 2, 3, 4));
                int end = Integer.parseInt(Files.readString(scratch.resolve("committed")));
                verifyOffsets(endpoint, ORIGINAL, end);
                write(endpoint, EXPANDED, "after-restart", 1);
                verifyOffsets(endpoint, EXPANDED, 2);
            }
            default -> throw new IllegalArgumentException(mode);
        }
        System.out.println("Hot membership " + mode + " passed");
    }

    private static void create(String endpoint, String topic, int replicas) throws Exception {
        try (var admin = Admin.create(GroupSmoke.base(endpoint))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) replicas))).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void write(String endpoint, String topic, String value, long offset) throws Exception {
        try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
            var record = new ProducerRecord<String, String>(topic, 0, null, value);
            GroupSmoke.check(producer.send(record).get(10, TimeUnit.SECONDS).offset() == offset, "unexpected offset");
        }
    }

    private static void waitForNodes(String endpoint, Set<Integer> expected) throws Exception {
        var request = MetadataRequest.Builder.allTopics().build((short) 12);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            var response = (MetadataResponse) Smoke.exchange(endpoint, request, 1);
            var nodes = response.data().brokers().stream().map(broker -> broker.nodeId())
                .collect(java.util.stream.Collectors.toSet());
            if (nodes.equals(expected) && response.data().controllerId() > 0) return;
            Thread.sleep(100);
        }
        throw new AssertionError("broker did not learn committed membership " + expected);
    }

    private static void verifyOffsets(String endpoint, String topic, int expected) {
        var properties = GroupSmoke.consumer(endpoint, "unused");
        properties.remove(ConsumerConfig.GROUP_ID_CONFIG);
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var offsets = new java.util.ArrayList<Long>();
            GroupSmoke.until(() -> offsets.size() >= expected, () -> consumer.poll(Duration.ofMillis(100))
                .forEach(record -> offsets.add(record.offset())));
            GroupSmoke.check(offsets.size() == expected, "wrong record count");
            for (int i = 0; i < expected; i++) {
                GroupSmoke.check(offsets.get(i) == i, "offset gap after membership change");
            }
        }
    }
}
