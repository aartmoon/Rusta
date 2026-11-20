import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.protocol.Errors;

public final class AdminSmoke {
    private static final String LIVE = "rusta-admin-live";
    private static final String DELETED = "rusta-admin-delete";
    private static final ConfigResource CONFIG = new ConfigResource(ConfigResource.Type.TOPIC, LIVE);

    public static void main(String[] args) throws Exception {
        var endpoint = args[1];
        var scratch = Path.of(args[2]);
        short replication = Short.parseShort(args[3]);
        try (var admin = Admin.create(GroupSmoke.base(endpoint))) {
            if (args[0].equals("verify")) {
                checkConfig(admin, "retention.ms", replication == 1 ? "604800000" : "-1", ConfigEntry.ConfigSource.DEFAULT_CONFIG);
                checkConfig(admin, "retention.bytes", "1048576", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
                var names = get(admin.listTopics().names());
                GroupSmoke.check(names.contains(LIVE) && !names.contains(DELETED), "deleted topic returned after restart");
                GroupSmoke.check(delete(controller(admin), (short) 6, null, Uuid.fromString(Files.readString(scratch.resolve("deleted-id"))))
                    == Errors.UNKNOWN_TOPIC_ID.code(), "retry by deleted ID did not return UNKNOWN_TOPIC_ID");
                System.out.println("Admin state recovered after restart");
                return;
            }
            var topics = new ArrayList<>(List.of(new NewTopic(LIVE, 1, replication), new NewTopic(DELETED, 1, replication)));
            for (int version = 1; version <= 5; version++) topics.add(new NewTopic("rusta-admin-v" + version, 1, replication));
            get(admin.createTopics(topics).all());
            var id = get(admin.describeTopics(List.of(DELETED)).allTopicNames()).get(DELETED).topicId();
            Files.writeString(scratch.resolve("deleted-id"), id.toString());
            Files.writeString(scratch.resolve("deleted-directory"),
                new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits()).toString());
            checkConfig(admin, "retention.ms", replication == 1 ? "604800000" : "-1", ConfigEntry.ConfigSource.DEFAULT_CONFIG);
            Map<ConfigResource, Collection<AlterConfigOp>> changes =
                Map.of(CONFIG, List.of(set("retention.bytes", "1048576")));
            get(admin.incrementalAlterConfigs(changes, new AlterConfigsOptions().validateOnly(true)).all());
            checkConfig(admin, "retention.bytes", "-1", ConfigEntry.ConfigSource.DEFAULT_CONFIG);
            get(admin.incrementalAlterConfigs(changes).all());
            get(admin.incrementalAlterConfigs(changes).all());
            checkConfig(admin, "retention.bytes", "1048576", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
            for (short version = 1; version <= 4; version++) checkDescribeWire(controller(admin), version);
            try {
                get(admin.incrementalAlterConfigs(Map.of(CONFIG,
                    List.of(set("retention.bytes", "100"), set("min.insync.replicas", "2")))).all());
                throw new AssertionError("unsupported config accepted");
            } catch (ExecutionException e) {
                GroupSmoke.check(e.getCause() instanceof InvalidConfigurationException, e.toString());
            }
            checkConfig(admin, "retention.bytes", "1048576", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
            try {
                get(admin.createPartitions(Map.of(LIVE, NewPartitions.increaseTo(2))).all());
                throw new AssertionError("partition expansion succeeded");
            } catch (ExecutionException e) {
                GroupSmoke.check(e.getCause() instanceof UnsupportedVersionException, e.toString());
            }
            try (var producer = new KafkaProducer<String, String>(GroupSmoke.producer(endpoint))) {
                get(producer.send(new ProducerRecord<>(LIVE, 0, "key", "value")));
                get(producer.send(new ProducerRecord<>(DELETED, 0, "key", "value")));
            }
            get(admin.incrementalAlterConfigs(Map.of(CONFIG, List.of(set("retention.ms", "0")))).all());
            try (var consumer = new KafkaConsumer<String, String>(GroupSmoke.consumer(endpoint, "rusta-admin-read"))) {
                var partition = new TopicPartition(LIVE, 0);
                GroupSmoke.until(() -> consumer.beginningOffsets(List.of(partition), Duration.ofSeconds(5)).get(partition) == 1,
                    () -> pause());
            }
            var reset = new AlterConfigOp(new ConfigEntry("retention.ms", null), AlterConfigOp.OpType.DELETE);
            get(admin.incrementalAlterConfigs(Map.of(CONFIG, List.of(reset))).all());
            get(admin.incrementalAlterConfigs(Map.of(CONFIG, List.of(reset))).all());
            String controller = controller(admin);
            for (short version = 1; version <= 5; version++) {
                String name = "rusta-admin-v" + version;
                GroupSmoke.check(delete(controller, version, name, Uuid.ZERO_UUID) == 0, "DeleteTopics v" + version);
                GroupSmoke.check(delete(controller, version, name, Uuid.ZERO_UUID) == Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                    "DeleteTopics retry v" + version);
            }
            GroupSmoke.check(delete(controller, (short) 6, null, id) == 0, "DeleteTopics v6 by ID");
            GroupSmoke.check(delete(controller, (short) 6, null, id) == Errors.UNKNOWN_TOPIC_ID.code(), "stale delete by ID");
            GroupSmoke.check(delete(controller, (short) 6, DELETED, Uuid.ZERO_UUID) == Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                "repeat delete by name");
            get(admin.createTopics(List.of(new NewTopic(DELETED, 1, replication))).all());
            var recreated = get(admin.describeTopics(List.of(DELETED)).allTopicNames()).get(DELETED).topicId();
            GroupSmoke.check(!recreated.equals(id), "recreated topic reused ID");
            GroupSmoke.check(delete(controller, (short) 6, null, id) == Errors.UNKNOWN_TOPIC_ID.code(), "stale ID targeted recreation");
            get(admin.deleteTopics(List.of(DELETED)).all());
            System.out.println("Admin mutation and wire versions passed");
        }
    }

    private static AlterConfigOp set(String key, String value) {
        return new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET);
    }

    private static void checkConfig(Admin admin, String key, String value, ConfigEntry.ConfigSource source) {
        GroupSmoke.until(() -> {
            try {
                var result = get(admin.describeConfigs(List.of(CONFIG),
                    new DescribeConfigsOptions().includeSynonyms(true).includeDocumentation(true)).all()).get(CONFIG).get(key);
                return result != null && value.equals(result.value()) && source == result.source()
                    && result.documentation() != null && !result.synonyms().isEmpty();
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        }, AdminSmoke::pause);
    }

    private static String controller(Admin admin) throws Exception {
        var controller = get(admin.describeCluster().controller());
        return controller.host() + ":" + controller.port();
    }

    private static short delete(String endpoint, short version, String name, Uuid id) throws Exception {
        var data = new DeleteTopicsRequestData().setTimeoutMs(5000);
        if (version < 6) data.setTopicNames(List.of(name));
        else data.topics().add(new DeleteTopicsRequestData.DeleteTopicState().setName(name).setTopicId(id));
        var response = (DeleteTopicsResponse) Smoke.exchange(endpoint, new DeleteTopicsRequest.Builder(data).build(version), 4);
        return response.data().responses().iterator().next().errorCode();
    }

    private static void checkDescribeWire(String endpoint, short version) throws Exception {
        var resource = new DescribeConfigsRequestData.DescribeConfigsResource()
            .setResourceType(ConfigResource.Type.TOPIC.id())
            .setResourceName(LIVE)
            .setConfigurationKeys(List.of("retention.bytes"));
        var data = new DescribeConfigsRequestData().setResources(List.of(resource))
            .setIncludeSynonyms(true).setIncludeDocumentation(version >= 3);
        var response = (DescribeConfigsResponse) Smoke.exchange(endpoint,
            new DescribeConfigsRequest.Builder(data).build(version), 6);
        var result = response.data().results().getFirst();
        GroupSmoke.check(result.errorCode() == 0 && result.configs().size() == 1, "DescribeConfigs v" + version);
        var config = result.configs().getFirst();
        GroupSmoke.check("1048576".equals(config.value()) && config.synonyms().size() == 2
            && (version < 3 || config.documentation() != null), "DescribeConfigs fields v" + version);
    }

    private static <T> T get(Future<T> future) throws Exception {
        return future.get(20, TimeUnit.SECONDS);
    }

    private static <T> T get(org.apache.kafka.common.KafkaFuture<T> future) throws Exception {
        return future.get(20, TimeUnit.SECONDS);
    }

    private static void pause() {
        try { Thread.sleep(100); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
    }
}
