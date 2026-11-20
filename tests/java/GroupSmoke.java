import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class GroupSmoke {
    static final Duration TIMEOUT = Duration.ofSeconds(12);
    private static final String TOPIC = "rusta-group-data";
    private static final List<TopicPartition> PARTITIONS =
        java.util.stream.IntStream.range(0, 4).mapToObj(i -> new TopicPartition(TOPIC, i)).toList();

    public static void main(String[] args) throws Exception {
        String endpoint = args[1];
        short replication = args.length > 2 ? Short.parseShort(args[2]) : 1;
        if (args[0].equals("write")) {
            try (Admin admin = Admin.create(base(endpoint))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 4, replication))).all().get(10, TimeUnit.SECONDS);
            }
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producer(endpoint))) {
                for (var partition : PARTITIONS) {
                    for (int i = 0; i < 3; i++) {
                        var record = new ProducerRecord<String, String>(TOPIC, partition.partition(), null, "record-" + i);
                        check(producer.send(record).get(10, TimeUnit.SECONDS).offset() == i, "append offset");
                    }
                }
            }
            rebalance(endpoint, "rusta-eager", RangeAssignor.class);
            rebalance(endpoint, "rusta-cooperative", CooperativeStickyAssignor.class);
            manual(endpoint);
        } else {
            for (String group : List.of("rusta-eager", "rusta-cooperative")) {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumer(endpoint, group))) {
                    consumer.subscribe(List.of(TOPIC));
                    Map<Integer, Long> first = new HashMap<>();
                    until(() -> first.size() == 4, () -> consumer.poll(Duration.ofMillis(100)).forEach(
                        record -> first.putIfAbsent(record.partition(), record.offset())));
                    check(first.values().stream().allMatch(offset -> offset == 1L), "restart did not resume at committed offset " + first);
                }
            }
            try (KafkaConsumer<String, String> manual = new KafkaConsumer<>(consumer(endpoint, "rusta-manual"))) {
                var offset = manual.committed(Set.of(PARTITIONS.get(0)), TIMEOUT).get(PARTITIONS.get(0));
                check(offset != null && offset.offset() == 2 && offset.metadata().equals("manual"), "lost manual offset");
            }
        }
        System.out.println("Kafka Java 4.1.2 groups " + args[0] + " passed");
    }

    private static void rebalance(String endpoint, String group, Class<?> assignor) throws Exception {
        var properties = consumer(endpoint, group);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor.getName());
        var revokeCount = new java.util.concurrent.atomic.AtomicInteger();
        try (KafkaConsumer<String, String> first = new KafkaConsumer<>(properties)) {
            first.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    revokeCount.addAndGet(partitions.size());
                }
            });
            var epochs = new HashMap<Integer, Optional<Integer>>();
            until(() -> epochs.size() == 4, () -> first.poll(Duration.ofMillis(100)).forEach(
                record -> epochs.put(record.partition(), record.leaderEpoch())));
            var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (var partition : PARTITIONS) {
                offsets.put(partition, new OffsetAndMetadata(1, epochs.get(partition.partition()), "checkpoint"));
            }
            first.commitSync(offsets, TIMEOUT);

            try (KafkaConsumer<String, String> second = new KafkaConsumer<>(properties)) {
                second.subscribe(List.of(TOPIC));
                until(() -> first.assignment().size() == 2 && second.assignment().size() == 2, () -> {
                    first.poll(Duration.ofMillis(100));
                    second.poll(Duration.ofMillis(100));
                    check(Collections.disjoint(first.assignment(), second.assignment()), "partitions assigned to two members");
                });
                var combined = new HashSet<>(first.assignment());
                combined.addAll(second.assignment());
                check(combined.equals(Set.copyOf(PARTITIONS)), "partitions lost during rebalancing");
                check(revokeCount.get() > 0, "rebalance never revoked partitions");

                try (Admin admin = Admin.create(base(endpoint))) {
                    var description = admin.describeConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS).get(group);
                    check(description.members().size() == 2 && description.type() == GroupType.CLASSIC, "wrong group description");
                    check(admin.listGroups().all().get(10, TimeUnit.SECONDS).stream()
                        .anyMatch(listing -> listing.groupId().equals(group)), "missing group listing");
                    check(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                        .equals(offsets), "admin offsets differ from the committed values");
                }
            }
            until(() -> first.assignment().size() == 4, () -> first.poll(Duration.ofMillis(100)));
        }
    }

    private static void manual(String endpoint) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumer(endpoint, "rusta-manual"))) {
            var partition = PARTITIONS.get(0);
            consumer.assign(List.of(partition));
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(2, "manual")), TIMEOUT);
        }
    }

    static Map<String, Object> base(String endpoint) {
        var properties = new HashMap<String, Object>();
        properties.put("bootstrap.servers", endpoint);
        properties.put("request.timeout.ms", 5000);
        return properties;
    }

    static Map<String, Object> producer(String endpoint) {
        var properties = base(endpoint);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    static Map<String, Object> consumer(String endpoint, String group) {
        var properties = base(endpoint);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 5000);
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 300);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 8000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    static void until(BooleanSupplier condition, Runnable poll) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) poll.run();
        check(condition.getAsBoolean(), "condition not met within " + TIMEOUT);
    }

    static void check(boolean result, String detail) {
        if (!result) throw new AssertionError(detail);
    }
}
