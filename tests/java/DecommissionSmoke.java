import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

public final class DecommissionSmoke {
    private static final String TOPIC = "drain-traffic";
    private static final int PARTITIONS = 8;
    private static final Set<Integer> INITIAL = Set.of(1, 2, 3);

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String endpoint = args[1];
        Path scratch = Path.of(args[2]);
        if (mode.equals("setup")) {
            try (var admin = Admin.create(GroupSmoke.base(endpoint));
                 var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 3))).all().get(15, TimeUnit.SECONDS);
                for (int partition = 0; partition < PARTITIONS; partition++) {
                    send(producer, partition, 0);
                }
            }
            try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(endpoint, "drain-group"))) {
                consumer.commitSync(Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1, "relocate")),
                    GroupSmoke.TIMEOUT);
            }
            var topic = waitForPlacement(endpoint, INITIAL, 0);
            Files.writeString(scratch.resolve("topic-id"), topic.topicId().toString());
            var metadata = (MetadataResponse) Smoke.exchange(endpoint, MetadataRequest.Builder.allTopics().build((short) 12), 1);
            Files.writeString(scratch.resolve("initial-controller"), Integer.toString(metadata.data().controllerId()));
        } else if (mode.equals("traffic")) {
            try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint));
                 var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(endpoint, "drain-readback"))) {
                long deadline = System.nanoTime() + Duration.ofSeconds(80).toNanos();
                int next = 1;
                long read = 0;
                var partition = new TopicPartition(TOPIC, 0);
                consumer.assign(List.of(partition));
                consumer.seek(partition, 0);
                while (!Files.exists(scratch.resolve("stop")) && System.nanoTime() < deadline) {
                    send(producer, 0, next++);
                    for (var record : consumer.poll(Duration.ofMillis(5))) {
                        GroupSmoke.check(record.offset() == read && value(0, read).equals(record.value()),
                            "concurrent Fetch returned a gap or different record");
                        read++;
                    }
                    if (next == 2) Files.writeString(scratch.resolve("ready"), "ready");
                }
                GroupSmoke.check(next > 2 && Files.exists(scratch.resolve("stop")), "missing concurrent writes");
                GroupSmoke.check(read >= 3, "missing concurrent reads");
                Files.writeString(scratch.resolve("committed"), Integer.toString(next));
            }
        } else if (mode.equals("before-decommission")) {
            var topic = waitForPlacement(endpoint, INITIAL, 0);
            checkTopicId(topic, scratch);
            checkCommit(endpoint);
            int leader = topic.partitions().stream().filter(p -> p.partitionIndex() == 0).findFirst().orElseThrow().leaderId();
            Files.writeString(scratch.resolve("source"), Integer.toString(leader));
        } else if (mode.equals("pending")) {
            var request = MetadataRequest.Builder.allTopics().build((short) 12);
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            do {
                var data = ((MetadataResponse) Smoke.exchange(endpoint, request, 1)).data();
                GroupSmoke.check(brokers(data).equals(INITIAL), "broker removed before replacement was available");
                Thread.sleep(100);
            } while (System.nanoTime() < deadline);
        } else if (mode.equals("partly-moved")) {
            int source = Integer.parseInt(Files.readString(scratch.resolve("source")));
            var request = MetadataRequest.Builder.allTopics().build((short) 12);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                var data = ((MetadataResponse) Smoke.exchange(endpoint, request, 1)).data();
                var topic = topic(data);
                if (data.controllerId() > 0 && brokers(data).contains(source) && topic != null
                        && topic.partitions().stream().anyMatch(p -> p.replicaNodes().contains(4))
                        && topic.partitions().stream().anyMatch(p -> p.replicaNodes().contains(source))) {
                    Files.writeString(scratch.resolve("controller"), Integer.toString(data.controllerId()));
                    System.out.println("Decommission partly-moved passed");
                    return;
                }
                Thread.sleep(75);
            }
            throw new AssertionError("could not observe an in-progress move");
        } else {
            var survivors = new HashSet<>(INITIAL);
            survivors.remove(Integer.parseInt(Files.readString(scratch.resolve("source"))));
            survivors.add(4);
            int stopped = mode.equals("quorum") ? Integer.parseInt(Files.readString(scratch.resolve("stopped")).trim()) : 0;
            checkTopicId(waitForPlacement(endpoint, survivors, stopped), scratch);
            if (!mode.equals("placement")) {
                checkCommit(endpoint);
                int end = Integer.parseInt(Files.readString(scratch.resolve("committed")));
                verify(endpoint, 0, end);
                for (int partition = 1; partition < PARTITIONS; partition++) {
                    verify(endpoint, partition, 1);
                }
                if (mode.equals("quorum")) {
                    try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
                        send(producer, 0, end);
                    }
                    verify(endpoint, 0, end + 1);
                    Files.writeString(scratch.resolve("committed"), Integer.toString(end + 1));
                }
            }
        }
        System.out.println("Decommission " + mode + " passed");
    }

    private static void checkTopicId(MetadataResponseTopic topic, Path scratch) throws Exception {
        GroupSmoke.check(topic.topicId().toString().equals(Files.readString(scratch.resolve("topic-id"))),
            "topic ID changed during membership change");
    }

    private static void checkCommit(String endpoint) {
        var partition = new TopicPartition(TOPIC, 0);
        try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(endpoint, "drain-group"))) {
            var offset = consumer.committed(Set.of(partition), GroupSmoke.TIMEOUT).get(partition);
            GroupSmoke.check(offset != null && offset.offset() == 1 && "relocate".equals(offset.metadata()),
                "consumer commit was lost during membership change");
        }
    }

    private static void send(KafkaProducer<String, String> producer, int partition, long offset) throws Exception {
        var record = new ProducerRecord<String, String>(TOPIC, partition, null, value(partition, offset));
        GroupSmoke.check(producer.send(record).get(25, TimeUnit.SECONDS).offset() == offset, "wrong acknowledged offset");
    }

    private static String value(int partition, long offset) {
        return "partition-" + partition + "-value-" + offset;
    }

    private static MetadataResponseTopic waitForPlacement(
            String endpoint, Set<Integer> expected, int stopped) throws Exception {
        var request = MetadataRequest.Builder.allTopics().build((short) 12);
        long deadline = System.nanoTime() + Duration.ofSeconds(50).toNanos();
        while (System.nanoTime() < deadline) {
            var data = ((MetadataResponse) Smoke.exchange(endpoint, request, 1)).data();
            var topic = topic(data);
            if (brokers(data).equals(expected) && expected.contains(data.controllerId()) && data.controllerId() != stopped && topic != null
                    && topic.partitions().size() == PARTITIONS
                    && topic.partitions().stream().allMatch(p -> p.errorCode() == 0
                        && expected.contains(p.leaderId()) && p.leaderId() != stopped
                        && p.replicaNodes().size() == 3
                        && expected.containsAll(p.replicaNodes()))) {
                return topic;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("metadata did not converge to brokers " + expected);
    }

    private static Set<Integer> brokers(MetadataResponseData data) {
        return data.brokers().stream().map(broker -> broker.nodeId()).collect(Collectors.toSet());
    }

    private static MetadataResponseTopic topic(MetadataResponseData data) {
        return data.topics().valuesList().stream().filter(t -> TOPIC.equals(t.name())).findFirst().orElse(null);
    }

    private static void verify(String endpoint, int partition, int expectedCount) {
        var config = GroupSmoke.consumer(endpoint, "unused");
        config.remove(ConsumerConfig.GROUP_ID_CONFIG);
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            var assignment = List.of(new TopicPartition(TOPIC, partition));
            consumer.assign(assignment);
            consumer.seekToBeginning(assignment);
            var values = new ArrayList<String>();
            GroupSmoke.until(() -> values.size() >= expectedCount, () -> consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                GroupSmoke.check(record.offset() == values.size(), "gap or duplicate offset");
                values.add(record.value());
            }));
            GroupSmoke.check(values.size() == expectedCount, "wrong number of recovered records");
            for (int offset = 0; offset < expectedCount; offset++) {
                GroupSmoke.check(values.get(offset).equals(value(partition, offset)), "record changed after relocation");
            }
        }
    }
}
