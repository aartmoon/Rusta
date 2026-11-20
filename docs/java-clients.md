# Connect Java and Spring Kafka

Use `127.0.0.1:9092` on the host or `broker:29092` from a container on the single-broker Compose network. For a cluster, supply several advertised seeds as listed in the [README](../README.md#run-a-local-cluster). The examples use Java 21, Kafka Java Client 4.1.2 and Spring Kafka 4.0.8. The [Kafka API contract](kafka-api.md) documents the supported request versions and features.

## Kafka Java Client

Create the topic before publishing. Put these imports and statements in a Java 21 application with `kafka-clients` on the classpath. The example uses one replica for a singleton. Use a replication factor of three for a three-broker cluster.

```java
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

// Inside main, which declares throws Exception.
var config = Map.<String, Object>of(
    "bootstrap.servers", "127.0.0.1:9092",
    "security.protocol", "PLAINTEXT"
);
try (var admin = Admin.create(config)) {
    admin.createTopics(List.of(new NewTopic("events", 1, (short) 1)))
        .all().get(10, TimeUnit.SECONDS);
}
var producerConfig = new HashMap<>(config);
producerConfig.put("acks", "all");
producerConfig.put("enable.idempotence", true);
try (var producer = new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer())) {
    producer.send(new ProducerRecord<>("events", "id-1", "hello")).get(10, TimeUnit.SECONDS);
}
var consumerConfig = new HashMap<>(config);
consumerConfig.put("group.id", "events-reader");
consumerConfig.put("group.protocol", "classic");
consumerConfig.put("auto.offset.reset", "earliest");
consumerConfig.put("enable.auto.commit", false);
try (var consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new StringDeserializer())) {
    consumer.subscribe(List.of("events"));
    while (true) {
        var records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) continue;
        records.forEach(record -> System.out.println(record.value()));
        consumer.commitSync(Duration.ofSeconds(10));
        break;
    }
}
```

The runnable Java tests cover [production and retrieval](../tests/java/Smoke.java) and [group rebalances and offset commits](../tests/java/GroupSmoke.java).

## Spring Kafka

For a Spring Boot app using Spring Kafka, configure string serializers, classic groups and record-level acknowledgment after a successful listener invocation.

```yaml
spring:
  kafka:
    bootstrap-servers: 127.0.0.1:9092
    properties:
      security.protocol: PLAINTEXT
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        enable.idempotence: true
    consumer:
      group-id: spring-events
      enable-auto-commit: false
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        group.protocol: classic
    listener:
      ack-mode: record
```

```java
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class EventsListener {
    @Bean
    NewTopic events() {
        return new NewTopic("spring-events", 1, (short) 1);
    }

    @KafkaListener(topics = "spring-events")
    void receive(String value) {
        System.out.println(value);
    }
}
```

Use `KafkaTemplate<String, String>.send("spring-events", "id-1", "hello")` and handle the returned future to observe the broker acknowledgment. The runnable [Spring Kafka smoke](../tests/java/SpringSmoke.java) verifies topic creation, annotated consumption and offset resume after broker replacement.

```sh
tests/java/smoke.sh
```
