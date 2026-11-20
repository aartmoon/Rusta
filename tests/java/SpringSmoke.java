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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

public final class SpringSmoke {
    private static final String TOPIC = "rusta-spring-data";
    private static final String GROUP = "rusta-spring-group";
    private static String endpoint;

    public static void main(String[] args) throws Exception {
        endpoint = args[1];
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

    @Configuration
    @EnableKafka
    static class Config {
        @Bean KafkaAdmin kafkaAdmin() {
            var admin = new KafkaAdmin(GroupSmoke.base(endpoint));
            admin.setFatalIfBrokerNotAvailable(true);
            return admin;
        }

        @Bean NewTopic testTopic() {
            return new NewTopic(TOPIC, 1, (short) 1);
        }

        @Bean ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
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
