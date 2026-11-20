import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

public final class TopologySmoke {
    private static final String SMALL = "topology-rf3";
    private static final String LARGE = "topology-rf5";

    public static void main(String[] args) throws Exception {
        var bootstrap = args[1];
        switch (args[0]) {
            case "setup" -> {
                try (var admin = Admin.create(GroupSmoke.base(bootstrap))) {
                    GroupSmoke.check(admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size() == 5, "broker count");
                    admin.createTopics(List.of(new NewTopic(SMALL, Optional.of(5), Optional.empty())))
                        .all().get(10, TimeUnit.SECONDS);
                    admin.createTopics(List.of(new NewTopic(LARGE, 1, (short) 5)))
                        .all().get(10, TimeUnit.SECONDS);
                    try {
                        admin.createTopics(List.of(new NewTopic("rf-out-of-range", 1, (short) 6)))
                            .all().get(10, TimeUnit.SECONDS);
                        throw new AssertionError("RF above node count was accepted");
                    } catch (ExecutionException error) {
                        GroupSmoke.check(error.getCause() instanceof InvalidReplicationFactorException, "wrong RF error");
                    }
                    var partitions = admin.describeTopics(List.of(SMALL)).allTopicNames().get(10, TimeUnit.SECONDS).get(SMALL).partitions();
                    GroupSmoke.check(partitions.stream().allMatch(p -> p.replicas().size() == 3), "default RF");
                    GroupSmoke.check(partitions.stream().flatMap(p -> p.replicas().stream()).distinct().count() == 5,
                        "placements did not use all brokers");
                }
                verifyNonReplica(args[2]);
                write(bootstrap, SMALL, 0);
                write(bootstrap, LARGE, 0);
            }
            case "majority" -> {
                write(bootstrap, LARGE, 1);
                read(bootstrap, LARGE, 2);
            }
            case "independent" -> {
                write(bootstrap, SMALL, 1);
                read(bootstrap, SMALL, 2);
                expectNoCommit(args[2]);
            }
            default -> throw new IllegalArgumentException(args[0]);
        }
        System.out.println("Five-broker topology " + args[0] + " passed");
    }

    private static void verifyNonReplica(String endpoint) throws Exception {
        var request = MetadataRequest.Builder.forTopicNames(List.of(SMALL), false).build((short) 12);
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            var reply = (MetadataResponse) Smoke.exchange(endpoint, request, 10);
            var topics = reply.data().topics();
            if (!topics.isEmpty() && !topics.iterator().next().partitions().isEmpty()) {
                var part = topics.iterator().next().partitions().get(0);
                GroupSmoke.check(Set.copyOf(part.replicaNodes()).equals(Set.of(1, 2, 3)), "wrong RF3 placement");
                if (part.errorCode() == 0 && part.replicaNodes().contains(part.leaderId())) return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("broker 5 did not learn the leader of partition 0");
    }

    private static void write(String endpoint, String topic, long expected) throws Exception {
        try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
            var sent = producer.send(new ProducerRecord<String, String>(topic, 0, null, "record-" + expected));
            GroupSmoke.check(sent.get(12, TimeUnit.SECONDS).offset() == expected, "unexpected write offset for " + topic);
        }
    }

    private static void read(String endpoint, String topic, int expected) {
        var props = GroupSmoke.consumer(endpoint, "unused");
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var records = new ArrayList<String>();
            GroupSmoke.until(() -> records.size() >= expected, () -> consumer.poll(Duration.ofMillis(100))
                .forEach(record -> records.add(record.value())));
            GroupSmoke.check(records.equals(List.of("record-0", "record-1")), "committed records differ for " + topic);
        }
    }

    private static void expectNoCommit(String endpoint) throws Exception {
        var records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("without-majority".getBytes(StandardCharsets.UTF_8)));
        var part = new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records);
        var topic = new ProduceRequestData.TopicProduceData().setName(LARGE).setPartitionData(List.of(part));
        var request = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(2000);
        request.topicData().add(topic);
        var response = (ProduceResponse) Smoke.exchange(endpoint, ProduceRequest.builder(request).build((short) 11), 12);
        short code = response.data().responses().iterator().next().partitionResponses().get(0).errorCode();
        GroupSmoke.check(code == 6 || code == 7, "RF5 write unexpectedly succeeded or returned wrong error: " + code);
    }
}
