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
    private static final int PARTITIONS = 6;
    private static Properties base(String seeds) {
        var p = new Properties();
        p.put("bootstrap.servers", seeds);
        p.put("request.timeout.ms", "10000");
        return p;
    }
    private static KafkaConsumer<byte[],byte[]> consumer(String seeds, String group) {
        var p = base(seeds);
        p.put("group.id", group);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("session.timeout.ms", "6000");
        p.put("heartbeat.interval.ms", "1000");
        p.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        return new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    public static void main(String[] args) throws Exception {
        String seeds = args[1];
        String topic = args[2];
        if (args[0].equals("prepare")) {
            short rf = Short.parseShort(args[3]);
            try (var admin = Admin.create(base(seeds))) {
                var topics = List.of("write", "mix", "mix_large").stream()
                    .map(name -> new NewTopic(name, PARTITIONS, rf).configs(Map.of("retention.ms", "-1"))).toList();
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
        var readCount = new AtomicLong();
        var committed = new AtomicLong();
        var sent = new AtomicLong();
        var done = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var assigned = new CountDownLatch(read ? 1 : 0);
        Thread reader = null;
        if (read) {
            reader = new Thread(() -> {
                try (var c = consumer(seeds, "resource-" + topic)) {
                    c.subscribe(List.of(topic));
                    var offsets = new long[PARTITIONS];
                    long lastSync = System.nanoTime();
                    while (!done.get() || readCount.get() < sent.get()) {
                        var records = c.poll(Duration.ofMillis(75));
                        if (c.assignment().size() == PARTITIONS) assigned.countDown();
                        for (var item : records) {
                            if (item.offset() != offsets[item.partition()]++ || item.value().length != size)
                                throw new IllegalStateException("unexpected record");
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
        properties.put("linger.ms", "5");
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
                    int part = (int) (sent.getAndIncrement() % PARTITIONS);
                    p.send(new ProducerRecord<>(topic, part, null, body), (meta, error) -> {
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
        double seconds = measuredNanos / 1e9;
        System.out.printf(Locale.ROOT,
            "{\"seconds\":%.3f,\"sent_total\":%d,\"acked_total\":%d,\"consumed_and_committed_total\":%d,\"offered_msg_s\":%.1f,\"acked_msg_s\":%.1f,\"consumed_msg_s\":%.1f,\"p50_ms\":%.3f,\"p95_ms\":%.3f,\"p99_ms\":%.3f,\"submitted_unread_at_window_end\":%d}%n",
            seconds, sent.get(), acks.get(), committed.get(), sentDuring / seconds, (ackEnd - ackStart) / seconds, (readEnd - readStart) / seconds,
            lat[(int)(lat.length * .50)] / 1e6, lat[(int)(lat.length * .95)] / 1e6, lat[(int)(lat.length * .99)] / 1e6,
            read ? sent.get() - readEnd : 0L);
    }
}
