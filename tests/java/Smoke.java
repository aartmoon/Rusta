import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Smoke {
    private static final String TOPIC = "rusta-smoke";
    private static final String EXPIRED = "rusta-expired";
    private static final long TIME = 1_700_000_000_000L;
    private static final List<String> CODECS = List.of("none", "gzip", "snappy", "lz4", "zstd");
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String endpoint = args[1];
        switch (mode) {
            case "write" -> write(endpoint, Path.of(args[2]));
            case "verify" -> verify(endpoint, Path.of(args[2]));
            case "internal" -> {
                try (Admin admin = Admin.create(base(endpoint))) {
                    check(admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS)
                        .get(TOPIC).partitions().size() == 2, "internal advertised endpoint");
                }
                checkReads(endpoint);
            }
            default -> throw new IllegalArgumentException(mode);
        }
        System.out.println("Kafka Java 4.1.2 " + mode + " passed");
    }

    private static void write(String endpoint, Path saved) throws Exception {
        Uuid topicId;
        try (Admin admin = Admin.create(base(endpoint))) {
            admin.createTopics(List.of(new NewTopic("validation-only", 1, (short) 1)),
                new CreateTopicsOptions().validateOnly(true)).all().get(10, TimeUnit.SECONDS);
            check(!admin.listTopics().names().get(10, TimeUnit.SECONDS).contains("validation-only"), "validateOnly changed the catalog");
            NewTopic topic = new NewTopic(TOPIC, 2, (short) 1).configs(Map.of("retention.ms", "-1"));
            admin.createTopics(List.of(topic, new NewTopic(EXPIRED, 1, (short) 1)
                .configs(Map.of("retention.ms", "0")))).all().get(10, TimeUnit.SECONDS);
            topicId = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC).topicId();
            check(!topicId.equals(Uuid.ZERO_UUID), "missing stable topic ID");
            try {
                admin.createTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
                throw new AssertionError("duplicate creation succeeded");
            } catch (ExecutionException error) {
                check(error.getCause() instanceof TopicExistsException, "unexpected duplicate topic error " + error);
            }
        }

        long[] next = {0, 0};
        for (int i = 0; i < CODECS.size(); i++) {
            String codec = CODECS.get(i);
            Properties props = base(endpoint);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 25);
            int partition = i % 2;
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                List<java.util.concurrent.Future<RecordMetadata>> sends = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    long timestamp = TIME + i * 100 + (j == 0 ? 5 : 0);
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, partition, timestamp, "key", value(codec, j));
                    record.headers().add("test", new byte[] {42});
                    record.headers().add("test", null);
                    sends.add(producer.send(record));
                }
                for (var send : sends) {
                    check(send.get(10, TimeUnit.SECONDS).offset() == next[partition]++, "unexpected append offset");
                }
            }
        }

        var init = new InitProducerIdRequest.Builder(new InitProducerIdRequestData().setTransactionalId(null).setTransactionTimeoutMs(10_000))
            .build((short) 4);
        var id = ((InitProducerIdResponse) exchange(endpoint, init, 1)).data();
        check(id.errorCode() == 0 && id.producerId() > 0, "InitProducerId failed");
        long offset = sendRetry(endpoint, TOPIC, id.producerId(), id.producerEpoch(), 2);
        check(offset == next[0], "unexpected raw append offset");
        check(sendRetry(endpoint, TOPIC, id.producerId(), id.producerEpoch(), 3) == offset, "retry allocated a new offset");
        check(sendRetry(endpoint, EXPIRED, id.producerId(), id.producerEpoch(), 5) == 0, "unexpected expired topic offset");
        checkExpired(endpoint);
        Files.write(saved, List.of(topicId.toString(), Long.toString(id.producerId()), Short.toString(id.producerEpoch())));
        checkReads(endpoint);
    }

    private static void verify(String endpoint, Path saved) throws Exception {
        var identity = Files.readAllLines(saved);
        try (Admin admin = Admin.create(base(endpoint))) {
            var info = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
            check(info.topicId().toString().equals(identity.get(0)), "topic ID changed after restart");
        }
        long producer = Long.parseLong(identity.get(1));
        short epoch = Short.parseShort(identity.get(2));
        check(sendRetry(endpoint, TOPIC, producer, epoch, 4) == 6, "restart lost durable producer retry state");
        check(sendRetry(endpoint, EXPIRED, producer, epoch, 6) == 0, "retention erased producer retry state");
        checkExpired(endpoint);
        checkReads(endpoint);
    }

    private static void checkExpired(String endpoint) throws InterruptedException {
        var props = base(endpoint);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var partition = new TopicPartition(EXPIRED, 0);
            var partitions = List.of(partition);
            consumer.assign(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (consumer.beginningOffsets(partitions).get(partition) == 0 && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            check(consumer.beginningOffsets(partitions).get(partition) == 1, "retention did not expire idle segment");
            check(consumer.endOffsets(partitions).get(partition) == 1, "retry appended after retention");
        }
    }

    private static void checkReads(String endpoint) {
        var props = base(endpoint);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 32);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 32);
        Map<TopicPartition, List<String>> expected = Map.of(P0, new ArrayList<>(), P1, new ArrayList<>());
        for (int i = 0; i < CODECS.size(); i++) {
            var values = expected.get(i % 2 == 0 ? P0 : P1);
            values.add(value(CODECS.get(i), 0));
            values.add(value(CODECS.get(i), 1));
        }
        expected.get(P0).add("raw replay");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var partitions = List.of(P0, P1);
            consumer.assign(partitions);
            var duration = Duration.ofSeconds(10);
            check(consumer.endOffsets(partitions, duration).equals(Map.of(P0, 7L, P1, 4L)), "duplicate or missing durable record");
            var timestamp = consumer.offsetsForTimes(Map.of(P0, TIME + 4), duration).get(P0);
            check(timestamp != null && timestamp.offset() == 0 && timestamp.timestamp() == TIME + 5, "timestamp lookup");
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, List<String>> actual = Map.of(P0, new ArrayList<>(), P1, new ArrayList<>());
            long deadline = System.nanoTime() + duration.toNanos();
            while (actual.values().stream().mapToInt(List::size).sum() < 11 && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(300))) {
                    var values = actual.get(new TopicPartition(record.topic(), record.partition()));
                    check(record.offset() == values.size(), "unexpected fetch offset");
                    if (!record.value().equals("raw replay")) {
                        check(record.key().equals("key"), "lost key");
                        var headers = record.headers().headers("test").iterator();
                        check(Arrays.equals(headers.next().value(), new byte[] {42}), "changed header");
                        check(headers.next().value() == null && !headers.hasNext(), "lost null or duplicate header");
                    }
                    values.add(record.value());
                }
            }
            check(actual.equals(expected), "unexpected records or order");
        }
    }

    private static long sendRetry(String endpoint, String topicName, long producer, short epoch, int correlation) throws Exception {
        var records = MemoryRecords.withIdempotentRecords(Compression.NONE, producer, epoch, 0,
            new SimpleRecord(TIME + 500, "raw replay".getBytes(StandardCharsets.UTF_8)));
        var partition = new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records);
        var topic = new ProduceRequestData.TopicProduceData().setName(topicName).setPartitionData(List.of(partition));
        var request = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(5000);
        request.topicData().add(topic);
        var response = (ProduceResponse) exchange(endpoint, ProduceRequest.builder(request).build((short) 11), correlation);
        var result = response.data().responses().iterator().next().partitionResponses().get(0);
        check(result.errorCode() == 0, "raw Produce error " + result.errorCode());
        return result.baseOffset();
    }

    static AbstractResponse exchange(String endpoint, AbstractRequest request, int correlation) throws Exception {
        var parts = endpoint.split(":");
        var header = new RequestHeader(request.apiKey(), request.version(), "rusta-smoke", correlation);
        var body = request.serializeWithHeader(header);
        byte[] requestBytes = new byte[body.remaining()];
        body.get(requestBytes);
        try (var socket = new Socket(parts[0], Integer.parseInt(parts[1]))) {
            socket.setSoTimeout(5000);
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            var input = new DataInputStream(socket.getInputStream());
            int size = input.readInt();
            check(size > 0 && size < 8 * 1024 * 1024, "invalid response length");
            byte[] response = new byte[size];
            input.readFully(response);
            return AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
        }
    }

    private static String value(String codec, int index) {
        return codec + "-" + index + "-" + "record payload ".repeat(128);
    }

    private static Properties base(String endpoint) {
        var props = new Properties();
        props.put("bootstrap.servers", endpoint);
        props.put("request.timeout.ms", 5000);
        return props;
    }

    private static void check(boolean result, String message) {
        if (!result) throw new AssertionError(message);
    }
}
