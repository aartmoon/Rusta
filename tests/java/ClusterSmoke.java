import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.*;

public final class ClusterSmoke {
    private static final String TOPIC = "rusta-cluster-smoke";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final String RETRY = "retry-across-leaders";
    private static final long TIMESTAMP = 1_700_000_000_000L;

    public static void main(String[] args) throws Exception {
        var nodes = args[1].split(",");
        var run = Path.of(args[2]);
        var endpoints = args[3].split(",");
        switch (args[0]) {
            case "write" -> initial(nodes, run);
            case "failover" -> failover(nodes, endpoints, run, true);
            case "rejoined" -> failover(nodes, endpoints, run, false);
            case "minority" -> minority(nodes[0], run);
            case "leader" -> {
                String bootstrap = String.join(",", nodes);
                read(bootstrap, 7);
                Files.writeString(run.resolve("leader"), Integer.toString(waitLeader(bootstrap, -1)));
            }
            default -> throw new IllegalArgumentException(args[0]);
        }
        System.out.println("Kafka Java 4.1.2 cluster " + args[0] + " passed");
    }

    private static void initial(String[] nodes, Path run) throws Exception {
        String bootstrap = String.join(",", nodes);
        try (Admin admin = Admin.create(GroupSmoke.base(bootstrap))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3))).all().get(20, TimeUnit.SECONDS);
        }
        int leader = waitLeader(bootstrap, -1);
        try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(bootstrap))) {
            for (int i = 0; i < 5; i++) {
                var result = producer.send(new ProducerRecord<String, String>(TOPIC, 0, null, "message-" + i));
                GroupSmoke.check(result.get(10, TimeUnit.SECONDS).offset() == i, "unexpected cluster append offset");
            }
        }
        var init = new InitProducerIdRequest.Builder(new InitProducerIdRequestData().setTransactionalId(null)
            .setTransactionTimeoutMs(10_000)).build((short) 4);
        var producer = ((InitProducerIdResponse) Smoke.exchange(nodes[leader - 1], init, 1)).data();
        GroupSmoke.check(producer.errorCode() == 0, "could not allocate producer ID");
        GroupSmoke.check(retry(nodes[leader - 1], producer.producerId(), producer.producerEpoch()) == 5, "raw append mismatch");
        Files.write(run.resolve("producer"), List.of(Long.toString(producer.producerId()), Short.toString(producer.producerEpoch())));
        Files.writeString(run.resolve("leader"), Integer.toString(leader));
        read(bootstrap, 6);
    }

    private static void failover(String[] nodes, String[] endpoints, Path run, boolean append) throws Exception {
        String bootstrap = String.join(",", nodes);
        int previous = Integer.parseInt(Files.readString(run.resolve("leader")));
        int leader = waitLeader(bootstrap, previous);
        read(bootstrap, append ? 6 : 7);
        var producer = Files.readAllLines(run.resolve("producer"));
        GroupSmoke.check(retry(endpoints[leader - 1], Long.parseLong(producer.get(0)), Short.parseShort(producer.get(1))) == 5,
            "new leader forgot the producer sequence or wrote a duplicate");
        if (append) {
            try (var writer = new KafkaProducer<String, String>(GroupSmoke.producer(bootstrap))) {
                var result = writer.send(new ProducerRecord<String, String>(TOPIC, 0, null, "after-failover"));
                GroupSmoke.check(result.get(10, TimeUnit.SECONDS).offset() == 6, "wrong offset after failover");
            }
            read(bootstrap, 7);
        }
        Files.writeString(run.resolve("leader"), Integer.toString(leader));
    }

    private static void read(String bootstrap, long count) {
        var props = GroupSmoke.consumer(bootstrap, "unused");
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.assign(List.of(PARTITION));
            consumer.seekToBeginning(List.of(PARTITION));
            var found = new ArrayList<String>();
            GroupSmoke.until(() -> found.size() >= count, () -> consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                GroupSmoke.check(record.offset() == found.size(), "nonconsecutive cluster offset");
                found.add(record.value());
            }));
            List<String> expected = new ArrayList<>(List.of("message-0", "message-1", "message-2", "message-3", "message-4", RETRY));
            if (count == 7) expected.add("after-failover");
            GroupSmoke.check(found.equals(expected), "broker lost or duplicated a committed record " + found);
            GroupSmoke.check(consumer.endOffsets(List.of(PARTITION)).get(PARTITION) == count, "wrong high watermark");
        }
    }

    private static int waitLeader(String bootstrap, int previous) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        var request = MetadataRequest.Builder.allTopics().build((short) 12);
        while (System.nanoTime() < deadline) {
            for (var endpoint : bootstrap.split(",")) {
                try {
                    var metadata = ((MetadataResponse) Smoke.exchange(endpoint, request, 1)).data();
                    var topic = metadata.topics().find(TOPIC);
                    if (topic == null || topic.partitions().isEmpty()) continue;
                    var partition = topic.partitions().getFirst();
                    GroupSmoke.check(partition.replicaNodes().size() == 3, "partition was not assigned RF3");
                    if (partition.leaderId() > 0 && partition.leaderId() != previous) {
                        return partition.leaderId();
                    }
                } catch (java.io.IOException disconnected) {
                    // A seed can be down while the surviving brokers hold an election.
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no replacement leader for node " + previous);
    }

    private static long retry(String endpoint, long producerId, short epoch) throws Exception {
        var batch = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, epoch, 0,
            new SimpleRecord(TIMESTAMP, RETRY.getBytes(StandardCharsets.UTF_8)));
        var response = produce(endpoint, batch);
        GroupSmoke.check(response.errorCode() == 0, "idempotent Produce returned " + response.errorCode());
        return response.baseOffset();
    }

    private static ProduceRequestData request(MemoryRecords batch) {
        var part = new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(batch);
        var topic = new ProduceRequestData.TopicProduceData().setName(TOPIC).setPartitionData(List.of(part));
        var request = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(2000);
        request.topicData().add(topic);
        return request;
    }

    private static org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse produce(
        String endpoint, MemoryRecords batch
    ) throws Exception {
        var response = (ProduceResponse) Smoke.exchange(endpoint, ProduceRequest.builder(request(batch)).build((short) 11), 2);
        return response.data().responses().iterator().next().partitionResponses().get(0);
    }

    private static void minority(String endpoint, Path run) throws Exception {
        var batch = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(TIMESTAMP, "no-quorum".getBytes(StandardCharsets.UTF_8)));
        var error = produce(endpoint, batch).errorCode();
        GroupSmoke.check(error == 6 || error == 7, "minority attempt returned unexpected Produce code " + error);
        var saved = Files.readAllLines(run.resolve("producer"));
        var duplicate = MemoryRecords.withIdempotentRecords(Compression.NONE,
            Long.parseLong(saved.get(0)), Short.parseShort(saved.get(1)), 0,
            new SimpleRecord(TIMESTAMP, RETRY.getBytes(StandardCharsets.UTF_8)));
        var retryError = produce(endpoint, duplicate).errorCode();
        GroupSmoke.check(retryError == 6 || retryError == 7, "minority accepted an idempotent retry");
        var read = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1024 * 1024, Optional.empty());
        var fetch = FetchRequest.Builder.forConsumer((short) 12, 0, 0, Map.of(PARTITION, read))
            .setMaxBytes(1024 * 1024).build((short) 12);
        var reply = (FetchResponse) Smoke.exchange(endpoint, fetch, 3);
        var part = reply.data().responses().get(0).partitions().get(0);
        GroupSmoke.check(part.errorCode() == Errors.NOT_LEADER_OR_FOLLOWER.code()
            || part.errorCode() == Errors.OFFSET_NOT_AVAILABLE.code(),
            "minority attempt returned unexpected Fetch code " + part.errorCode());
        GroupSmoke.check(part.records() == null || part.records().sizeInBytes() == 0, "minority exposed records");
    }
}
