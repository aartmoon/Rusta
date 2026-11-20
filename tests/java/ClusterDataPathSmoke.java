import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.*;

public final class ClusterDataPathSmoke {
    private static final String RETAINED = "cluster-retention";
    private static final String POLL = "cluster-fetch-poll";
    private static final TopicPartition PARTITION = new TopicPartition(RETAINED, 0);
    private static final long RECORD_TIMESTAMP = 5_000_000_000_000L;

    private record Leader(int id, int epoch, String endpoint) {}

    public static void main(String[] args) throws Exception {
        String seeds = args[1];
        Path scratch = Path.of(args[2]);
        if (args[0].equals("write")) {
            try (var admin = Admin.create(GroupSmoke.base(seeds))) {
                admin.createTopics(List.of(
                    new NewTopic(RETAINED, 1, (short) 3).configs(Map.of("retention.ms", "2500")),
                    new NewTopic(POLL, 1, (short) 3))).all().get(15, TimeUnit.SECONDS);
            }
            checkLongPoll(seeds);
            var leader = leader(seeds, RETAINED, 0);
            var init = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
                .setTransactionalId(null).setTransactionTimeoutMs(10000)).build((short) 4);
            var producer = ((InitProducerIdResponse) Smoke.exchange(leader.endpoint(), init, 1)).data();
            GroupSmoke.check(producer.errorCode() == 0, "producer ID allocation failed");
            retry(leader.endpoint(), producer.producerId(), producer.producerEpoch());
            try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(seeds, "data-path"))) {
                var initial = consumer.offsetsForTimes(Map.of(PARTITION, 0L), GroupSmoke.TIMEOUT).get(PARTITION);
                GroupSmoke.check(initial != null && initial.offset() == 0
                    && initial.timestamp() == RECORD_TIMESTAMP, "initial timestamp lookup failed");
                GroupSmoke.until(() -> consumer.beginningOffsets(List.of(PARTITION), GroupSmoke.TIMEOUT)
                    .get(PARTITION) == 1L, ClusterDataPathSmoke::pause);
            }
            verify(seeds, leader, producer.producerId(), producer.producerEpoch(), leader.epoch());
            Files.write(scratch.resolve("retention-leader"), List.of(Integer.toString(leader.id()),
                Integer.toString(leader.epoch()), Long.toString(producer.producerId()),
                Short.toString(producer.producerEpoch())));
        } else {
            var saved = Files.readAllLines(scratch.resolve("retention-leader"));
            int previous = args[0].equals("failover") ? Integer.parseInt(saved.get(0)) : 0;
            var current = leader(seeds, RETAINED, previous);
            verify(seeds, current, Long.parseLong(saved.get(2)), Short.parseShort(saved.get(3)), Integer.parseInt(saved.get(1)));
        }
        System.out.println("Cluster data path " + args[0] + " passed");
    }

    private static void checkLongPoll(String seeds) throws Exception {
        var leader = leader(seeds, POLL, 0);
        long started = System.nanoTime();
        var timed = fetch(leader.endpoint(), POLL, 0, 1, 650, 4096);
        GroupSmoke.check(timed.errorCode() == 0 && timed.highWatermark() == 0
            && Duration.ofNanos(System.nanoTime() - started).toMillis() >= 450,
            "empty Fetch returned before its wait budget");

        try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(seeds))) {
            var waiting = CompletableFuture.supplyAsync(() -> {
                try {
                    return fetch(leader.endpoint(), POLL, 0, 1, 2000, 4096);
                } catch (Exception error) {
                    throw new java.util.concurrent.CompletionException(error);
                }
            });
            Thread.sleep(250);
            GroupSmoke.check(!waiting.isDone(), "Fetch completed before Produce");
            producer.send(new ProducerRecord<String, String>(POLL, 0, null, "first")).get(10, TimeUnit.SECONDS);
            var committed = waiting.get(1400, TimeUnit.MILLISECONDS);
            GroupSmoke.check(committed.errorCode() == 0 && committed.highWatermark() == 1
                && ((MemoryRecords) committed.records()).batches().iterator().hasNext(), "Fetch did not wake on durable append");

            producer.send(new ProducerRecord<String, String>(POLL, 0, null, "second")).get(10, TimeUnit.SECONDS);
            var capped = fetch(leader.endpoint(), POLL, 0, 0, 0, 1);
            var batches = ((MemoryRecords) capped.records()).batches().iterator();
            GroupSmoke.check(batches.next().baseOffset() == 0 && !batches.hasNext(),
                "Fetch byte cap did not preserve first-batch progress");
        }
    }

    private static void verify(String seeds, Leader leader, long producer, short epoch, int oldEpoch) throws Exception {
        try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(seeds, "data-path"))) {
            GroupSmoke.check(consumer.beginningOffsets(List.of(PARTITION), GroupSmoke.TIMEOUT).get(PARTITION) == 1L,
                "log start was lost");
            GroupSmoke.check(consumer.endOffsets(List.of(PARTITION), GroupSmoke.TIMEOUT).get(PARTITION) == 1L,
                "retention changed the high watermark");
            GroupSmoke.check(consumer.offsetsForTimes(Map.of(PARTITION, 0L), GroupSmoke.TIMEOUT).get(PARTITION) == null,
                "timestamp lookup exposed expired records");
        }
        var expired = fetch(leader.endpoint(), RETAINED, 0, 1, 1000, 4096);
        GroupSmoke.check(expired.errorCode() == Errors.OFFSET_OUT_OF_RANGE.code()
            && expired.logStartOffset() == 1 && expired.highWatermark() == 1,
            "Fetch returned wrong retained range or error");
        var epochRequest = new OffsetForLeaderEpochRequestData().setTopics(
            new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection(List.of(
                new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic(RETAINED)
                    .setPartitions(List.of(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartition(0).setCurrentLeaderEpoch(leader.epoch()).setLeaderEpoch(oldEpoch)))).iterator()));
        var response = (org.apache.kafka.common.message.OffsetForLeaderEpochResponseData)
            exchange(leader.endpoint(), epochRequest, 4);
        var end = response.topics().iterator().next().partitions().getFirst();
        GroupSmoke.check(end.errorCode() == 0 && end.leaderEpoch() == oldEpoch && end.endOffset() == 1,
            "leader epoch lookup did not preserve the retained boundary: " + end);
        retry(leader.endpoint(), producer, epoch);
    }

    private static void retry(String endpoint, long producerId, short epoch) throws Exception {
        var records = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, epoch, 0,
            new SimpleRecord(RECORD_TIMESTAMP, new byte[] {1, 2, 3}));
        var data = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(3000);
        data.topicData().add(new ProduceRequestData.TopicProduceData().setName(RETAINED)
            .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records))));
        var response = (ProduceResponse) Smoke.exchange(endpoint, ProduceRequest.builder(data).build((short) 11), 1);
        var partition = response.data().responses().iterator().next().partitionResponses().getFirst();
        GroupSmoke.check(partition.errorCode() == 0 && partition.baseOffset() == 0,
            "idempotent retry returned " + Errors.forCode(partition.errorCode()) + " at " + partition.baseOffset());
    }

    private static org.apache.kafka.common.message.FetchResponseData.PartitionData fetch(
            String endpoint, String topic, long offset, int min, int wait, int max) throws Exception {
        var request = new FetchRequestData().setReplicaId(-1).setMinBytes(min).setMaxWaitMs(wait).setMaxBytes(max)
            .setTopics(List.of(new FetchRequestData.FetchTopic().setTopic(topic).setPartitions(
                List.of(new FetchRequestData.FetchPartition().setPartition(0)
                    .setFetchOffset(offset).setPartitionMaxBytes(max)))));
        var response = (org.apache.kafka.common.message.FetchResponseData) exchange(endpoint, request, 12);
        return response.responses().getFirst().partitions().getFirst();
    }

    private static Leader leader(String seeds, String name, int excluded) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
        while (System.nanoTime() < deadline) {
            for (var seed : seeds.split(",")) {
                var response = (MetadataResponse) Smoke.exchange(seed, MetadataRequest.Builder.allTopics().build((short) 12), 1);
                var topic = response.data().topics().find(name);
                if (topic == null || topic.partitions().isEmpty()) continue;
                var partition = topic.partitions().getFirst();
                var broker = response.data().brokers().find(partition.leaderId());
                if (broker != null && broker.nodeId() != excluded) return new Leader(broker.nodeId(), partition.leaderEpoch(), broker.host() + ":" + broker.port());
            }
            pause();
        }
        throw new AssertionError("partition leader unavailable for " + name);
    }

    private static ApiMessage exchange(String endpoint, ApiMessage data, int version) throws Exception {
        var request = AbstractRequest.parseRequest(ApiKeys.forId(data.apiKey()), (short) version,
            MessageUtil.toByteBufferAccessor(data, (short) version)).request;
        return Smoke.exchange(endpoint, request, 1).data();
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
        }
    }
}
