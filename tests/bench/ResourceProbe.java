import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

public final class ResourceProbe {
    private static final int DEFAULT_PARTITIONS = 6;
    private static Properties base(String seeds) {
        var p = new Properties();
        p.put("bootstrap.servers", seeds);
        p.put("request.timeout.ms", "10000");
        return p;
    }
    private static KafkaConsumer<byte[],byte[]> consumer(String seeds, String group, int fetchMaxWaitMs) {
        var p = base(seeds);
        p.put("group.id", group);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("fetch.max.wait.ms", fetchMaxWaitMs);
        p.put("session.timeout.ms", "6000");
        p.put("heartbeat.interval.ms", "1000");
        p.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        return new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    public static void main(String[] args) throws Exception {
        String seeds = args[1];
        String topic = args[2];
        if (args[0].equals("ready")) {
            try (var admin = Admin.create(base(seeds))) {
                System.out.println(admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).size());
            }
            return;
        }
        if (args[0].equals("prepare")) {
            short rf = Short.parseShort(args[3]);
            int partitions = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_PARTITIONS;
            var settings = new HashMap<String, String>();
            settings.put("retention.ms", "-1");
            if (args.length > 5 && !args[5].equals("-")) {
                var config = args[5].split("=", 2);
                settings.put(config[0], config[1]);
            }
            try (var admin = Admin.create(base(seeds))) {
                var topics = (topic.equals("all") ? List.of("write", "mix", "mix_large") : List.of(topic)).stream()
                    .map(name -> new NewTopic(name, partitions, rf).configs(settings)).toList();
                admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
                admin.describeTopics(topics.stream().map(NewTopic::name).toList()).allTopicNames().get(30, TimeUnit.SECONDS);
            }
            return;
        }
        boolean read = args[0].equals("mixed");
        int size = Integer.parseInt(args[3]);
        int rate = Integer.parseInt(args[4]);
        int duration = Integer.parseInt(args[5]);
        Path files = Path.of(args[6]);
        int lingerMs = Integer.parseInt(args[7]);
        int fetchMaxWaitMs = Integer.parseInt(args[8]);
        int partitions = args.length > 9 ? Integer.parseInt(args[9]) : DEFAULT_PARTITIONS;
        var readCount = new AtomicLong();
        var committed = new AtomicLong();
        var sent = new AtomicLong();
        var done = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var endToEnd = new ConcurrentLinkedQueue<Long>();
        long warmupRecords = 4L * rate;
        var assigned = new CountDownLatch(read ? 1 : 0);
        Thread reader = null;
        if (read) {
            reader = new Thread(() -> {
                try (var c = consumer(seeds, "resource-" + topic, fetchMaxWaitMs)) {
                    c.subscribe(List.of(topic));
                    var offsets = new long[partitions];
                    long lastSync = System.nanoTime();
                    while (!done.get() || readCount.get() < sent.get()) {
                        var records = c.poll(Duration.ofMillis(75));
                        if (c.assignment().size() == partitions) assigned.countDown();
                        long receivedAt = System.nanoTime();
                        for (var item : records) {
                            if (item.offset() != offsets[item.partition()]++ || item.value().length != size)
                                throw new IllegalStateException("unexpected record");
                            var payload = ByteBuffer.wrap(item.value());
                            long submittedAt = payload.getLong();
                            long sequence = payload.getLong();
                            if (sequence != item.offset() * partitions + item.partition())
                                throw new IllegalStateException("wrong message identity " + sequence);
                            if (sequence >= warmupRecords) endToEnd.add(receivedAt - submittedAt);
                            readCount.incrementAndGet();
                        }
                        if (readCount.get() > committed.get() && System.nanoTime() - lastSync >= 1_000_000_000L) {
                            c.commitSync(Duration.ofSeconds(10));
                            committed.set(readCount.get());
                            lastSync = System.nanoTime();
                        }
                    }
                    c.commitSync(Duration.ofSeconds(10));
                    committed.set(readCount.get());
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "consumer");
            reader.setDaemon(true);
            reader.start();
        }
        if (!assigned.await(25, TimeUnit.SECONDS)) throw new IllegalStateException("consumer assignment", failure.get());
        var properties = base(seeds);
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");
        properties.put("linger.ms", lingerMs);
        properties.put("batch.size", "32768");
        properties.put("compression.type", "none");
        properties.put("delivery.timeout.ms", "30000");
        properties.put("max.block.ms", "12000");
        byte[] body = new byte[size];
        new Random(23).nextBytes(body);
        var latencies = new ConcurrentLinkedQueue<Long>();
        var acks = new AtomicLong();
        long start = System.nanoTime();
        long measuredNanos = 0;
        long readStart = 0, readEnd = 0, ackStart = 0, ackEnd = 0;
        var sentDuring = 0L;
        try (var p = new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int slot = 0; slot < (duration + 4) * 100; slot++) {
                long remaining = start + slot * 10_000_000L - System.nanoTime();
                if (remaining > 0) LockSupport.parkNanos(remaining);
                if (slot == 400) {
                    measuredNanos = System.nanoTime();
                    readStart = readCount.get();
                    ackStart = acks.get();
                    Files.createFile(files.resolve("steady"));
                }
                boolean measured = slot >= 400;
                int tick = slot % 100;
                long slotCount = (tick + 1L) * rate / 100 - (long) tick * rate / 100;
                for (int i = 0; i < slotCount; i++) {
                    if (failure.get() != null) throw new IllegalStateException("workload failure", failure.get());
                    long before = System.nanoTime();
                    long sequence = sent.getAndIncrement();
                    int part = (int) (sequence % partitions);
                    byte[] payload = body;
                    if (read) {
                        payload = body.clone();
                        ByteBuffer.wrap(payload).putLong(before).putLong(sequence);
                    }
                    p.send(new ProducerRecord<>(topic, part, null, payload), (meta, error) -> {
                        if (error != null) failure.compareAndSet(null, error);
                        else {
                            acks.incrementAndGet();
                            if (measured) latencies.add(System.nanoTime() - before);
                        }
                    });
                    if (measured) sentDuring++;
                }
            }
            readEnd = readCount.get();
            ackEnd = acks.get();
            measuredNanos = System.nanoTime() - measuredNanos;
            Files.createFile(files.resolve("end"));
            p.flush();
        } finally {
            done.set(true);
        }
        if (reader != null) reader.join(20000);
        if (failure.get() != null) throw new IllegalStateException("workload failure", failure.get());
        if (acks.get() != sent.get() || read && committed.get() != sent.get())
            throw new IllegalStateException("records sent=" + sent + " acknowledged=" + acks + " committed=" + committed);
        long[] lat = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
        long[] e2e = endToEnd.stream().mapToLong(Long::longValue).sorted().toArray();
        if (lat.length != sentDuring || read && e2e.length != sentDuring)
            throw new IllegalStateException("incomplete latency sample");
        String e2eP99 = read
            ? String.format(Locale.ROOT, "%.3f", e2e[(int) Math.ceil(e2e.length * .99) - 1] / 1e6)
            : "null";
        double seconds = measuredNanos / 1e9;
        System.out.printf(Locale.ROOT,
            "{\"seconds\":%.3f,\"sent_total\":%d,\"acked_total\":%d,\"consumed_and_committed_total\":%d,\"offered_msg_s\":%.1f,\"acked_msg_s\":%.1f,\"consumed_msg_s\":%.1f,\"p50_ms\":%.3f,\"p95_ms\":%.3f,\"p99_ms\":%.3f,\"ack_samples\":%d,\"end_to_end_samples\":%d,\"end_to_end_p99_ms\":%s,\"submitted_unread_at_window_end\":%d}%n",
            seconds, sent.get(), acks.get(), committed.get(), sentDuring / seconds, (ackEnd - ackStart) / seconds, (readEnd - readStart) / seconds,
            lat[(int)(lat.length * .50)] / 1e6, lat[(int)(lat.length * .95)] / 1e6, lat[(int)(lat.length * .99)] / 1e6,
            lat.length, e2e.length, e2eP99, read ? sent.get() - readEnd : 0L);
    }
}
