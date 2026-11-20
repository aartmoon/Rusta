import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

public final class CacheRecoverySmoke {
    private static final int RECORD_BYTES =
        Integer.parseInt(System.getenv().getOrDefault("RUSTA_CACHE_RECOVERY_RECORD_BYTES", "0"));

    private static String payload(int offset) {
        String prefix = "record-" + offset;
        return prefix + "x".repeat(Math.max(0, RECORD_BYTES - prefix.length()));
    }

    public static void main(String[] args) throws Exception {
        String topic = "cache-rejoin";
        var config = new HashMap<String, Object>();
        config.put("bootstrap.servers", args[1]);
        config.put("request.timeout.ms", 5000);
        config.put("default.api.timeout.ms", 5000);
        switch (args[0]) {
            case "create" -> {
                try (var admin = Admin.create(config)) {
                    admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)))
                        .all().get(15, TimeUnit.SECONDS);
                }
            }
            case "leader" -> {
                try (var admin = Admin.create(config)) {
                    var info = admin.describeTopics(List.of(topic)).allTopicNames().get(8, TimeUnit.SECONDS);
                    var leader = info.get(topic).partitions().getFirst().leader();
                    if (leader == null || leader.id() < 1) throw new AssertionError("leader missing");
                    System.out.println(leader.id());
                }
            }
            case "no-leader" -> {
                try (var admin = Admin.create(config)) {
                    try {
                        var info = admin.describeTopics(List.of(topic)).allTopicNames().get(8, TimeUnit.SECONDS);
                        var leader = info.get(topic).partitions().getFirst().leader();
                        if (leader != null && leader.id() > 0) throw new AssertionError("recovering replicas elected a leader");
                    } catch (java.util.concurrent.ExecutionException reported) {
                        if (!(reported.getCause() instanceof org.apache.kafka.common.errors.RetriableException))
                            throw reported;
                    }
                }
            }
            case "write" -> {
                config.put("acks", "all");
                config.put("enable.idempotence", true);
                try (var producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
                    int first = Integer.parseInt(args[2]);
                    int end = Integer.parseInt(args[3]);
                    for (int base = first; base < end; base += 128) {
                        var deliveries = new ArrayList<java.util.concurrent.Future<RecordMetadata>>();
                        for (int offset = base; offset < Math.min(base + 128, end); offset++) {
                            deliveries.add(producer.send(new ProducerRecord<>(topic, 0, null, payload(offset))));
                        }
                        for (int index = 0; index < deliveries.size(); index++) {
                            var result = deliveries.get(index).get(10, TimeUnit.SECONDS);
                            if (result.offset() != base + index)
                                throw new AssertionError("wrong Produce offset " + result.offset());
                        }
                    }
                }
            }
            case "read" -> {
                var partition = new TopicPartition(topic, 0);
                try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
                    var partitions = List.of(partition);
                    consumer.assign(partitions);
                    consumer.seekToBeginning(partitions);
                    int count = Integer.parseInt(args[2]);
                    int found = 0;
                    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                    while (found < count && System.nanoTime() < deadline) {
                        for (var record : consumer.poll(Duration.ofMillis(100))) {
                            if (record.offset() != found || !record.value().equals(payload(found)))
                                throw new AssertionError("wrong record at " + found);
                            found++;
                        }
                    }
                    if (found != count || consumer.endOffsets(partitions).get(partition) != count)
                        throw new AssertionError("wrong log length " + found);
                    System.out.println("Verified all " + count + " committed records");
                }
            }
        }
    }
}
