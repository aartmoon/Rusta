import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

public final class SpringSmoke {
    private static final String TOPIC = "rusta-spring-data";
    private static final String GROUP = "rusta-spring-group";
    private static String endpoint;
    private static boolean cluster;

    public static void main(String[] args) throws Exception {
        endpoint = args[1];
        cluster = args[0].equals("cluster");
        if (cluster) {
            clusterFailover(Path.of(args[2]));
            return;
        }
        if (args[0].equals("verify-cluster")) {
            try (var admin = Admin.create(GroupSmoke.base(endpoint))) {
                verifyBothCommits(admin, 2);
            }
            System.out.println("Spring Kafka 4.0.8 offsets recovered after full cluster restart");
            return;
        }
        long expected = args[0].equals("write") ? 0 : 1;

        try (var context = new AnnotationConfigApplicationContext(Config.class);
             var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
            var listener = context.getBean(TestListener.class);
            String message = "spring-" + expected;
            var sent = producer.send(new ProducerRecord<String, String>(TOPIC, 0, null, message));
            GroupSmoke.check(sent.get(10, TimeUnit.SECONDS).offset() == expected, "unexpected Spring record offset");
            ConsumerRecord<String, String> received = listener.records.poll(12, TimeUnit.SECONDS);
            GroupSmoke.check(received != null && received.offset() == expected && received.value().equals(message),
                "Spring listener received wrong record " + received);
            try (Admin admin = Admin.create(GroupSmoke.base(endpoint))) {
                var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                var committed = offsets.get(new TopicPartition(TOPIC, 0));
                GroupSmoke.check(committed != null && committed.offset() == expected + 1, "Spring acknowledgement was not committed");
            }
        }
        System.out.println("Spring Kafka 4.0.8 " + args[0] + " passed");
    }

    private static void clusterFailover(Path scratch) throws Exception {
        try (var context = new AnnotationConfigApplicationContext(Config.class);
             var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint));
             var admin = Admin.create(GroupSmoke.base(endpoint))) {
            var listener = context.getBean(TestListener.class);
            var registry = context.getBean(KafkaListenerEndpointRegistry.class);
            var container = registry.getListenerContainer("rusta-spring-listener");
            GroupSmoke.until(() -> {
                var assigned = container.getAssignmentsByClientId();
                return assigned != null && assigned.size() == 2
                    && assigned.values().stream().allMatch(partitions -> partitions.size() == 1);
            }, SpringSmoke::pause);
            verifyBothPartitions(listener, producer, admin, 0);

            var before = admin.describeConsumerGroups(List.of(GROUP)).all()
                .get(10, TimeUnit.SECONDS).get(GROUP);
            GroupSmoke.check(before.members().size() == 2, "expected two Spring group members");
            Files.writeString(scratch.resolve("spring-coordinator"), Integer.toString(before.coordinator().id()));
            GroupSmoke.until(() -> Files.exists(scratch.resolve("spring-failover")), SpringSmoke::pause);

            var survivorEndpoints = Arrays.stream(endpoint.split(","))
                .filter(node -> !node.equals(before.coordinator().host() + ":" + before.coordinator().port()))
                .collect(java.util.stream.Collectors.joining(","));
            var elected = GroupWireSmoke.find(survivorEndpoints, GROUP, before.coordinator().id());
            var properties = GroupSmoke.base(elected.endpoint());
            properties.put("default.api.timeout.ms", 5000);
            var survivor = Admin.create(properties);
            try {
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                boolean rejoined = false;
                while (System.nanoTime() < deadline) {
                    var after = survivor.describeConsumerGroups(List.of(GROUP)).all()
                        .get(6, TimeUnit.SECONDS).get(GROUP);
                    if (after.coordinator().id() == elected.id() && after.members().size() == 2) {
                        rejoined = true;
                        break;
                    }
                    pause();
                }
                GroupSmoke.check(rejoined, "Spring consumers did not rejoin after coordinator failover");
                verifyBothPartitions(listener, producer, survivor, 1);
            } finally {
                survivor.close(Duration.ZERO);
            }
        }
        System.out.println("Spring Kafka 4.0.8 two-consumer coordinator failover passed");
    }

    private static void verifyBothPartitions(TestListener listener, KafkaProducer<String, String> producer,
                                             Admin admin, long expected) throws Exception {
        for (int partition = 0; partition < 2; partition++) {
            var sent = producer.send(new ProducerRecord<String, String>(TOPIC, partition, null, "spring-" + expected));
            GroupSmoke.check(sent.get(10, TimeUnit.SECONDS).offset() == expected, "unexpected Spring record offset");
        }
        var received = new HashSet<Integer>();
        GroupSmoke.until(() -> received.size() == 2, () -> {
            var record = listener.records.poll();
            if (record != null && record.offset() == expected) received.add(record.partition());
            pause();
        });
        verifyBothCommits(admin, expected + 1);
    }

    private static void verifyBothCommits(Admin admin, long expected) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        for (int partition = 0; partition < 2; partition++) {
            var committed = offsets.get(new TopicPartition(TOPIC, partition));
            GroupSmoke.check(committed != null && committed.offset() == expected,
                "Spring acknowledgement was not committed for partition " + partition);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(75);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    @Configuration
    @EnableKafka
    static class Config {
        @Bean KafkaAdmin kafkaAdmin() {
            var admin = new KafkaAdmin(GroupSmoke.base(endpoint));
            admin.setFatalIfBrokerNotAvailable(true);
            return admin;
        }

        @Bean NewTopic testTopic() {
            return new NewTopic(TOPIC, cluster ? 2 : 1, (short) (cluster ? 3 : 1));
        }

        @Bean ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
            if (cluster) factory.setConcurrency(2);
            factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(GroupSmoke.consumer(endpoint, GROUP)));
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            return factory;
        }

        @Bean TestListener testListener() {
            return new TestListener();
        }
    }

    static class TestListener {
        final BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        @KafkaListener(id = "rusta-spring-listener", topics = TOPIC, groupId = GROUP)
        void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
            acknowledgment.acknowledge();
            records.add(record);
        }
    }
}
