"""Measure native Iggy TCP and NATS JetStream publish and consume."""
import asyncio
from datetime import timedelta
import json
from pathlib import Path
import random
import struct
import sys
import time

WARMUP = 4


def percentile(values, fraction):
    return sorted(values)[min(len(values) - 1, int(len(values) * fraction))] / 1e6


class Iggy:
    def __init__(self, endpoint, topic, password):
        self.url = f"iggy+tcp://bench:{password}@{endpoint}"
        self.topic = topic
        self.stream = "benchmark"

    async def prepare(self):
        import apache_iggy as api

        self.api = api
        self.producer = api.IggyClient.from_connection_string(self.url)
        await self.producer.connect()
        if await self.producer.get_stream(self.stream) is None:
            await self.producer.create_stream(self.stream)
        await self.producer.create_topic(self.stream, self.topic, api.TopicOptions(
            partitions_count=1, message_expiry=api.IggyExpiry.NeverExpire(),
        ))

    async def publish(self, records, acknowledge):
        await self.producer.send_messages(
            self.stream, self.topic, self.api.Partitioning.partition_id(0),
            [self.api.SendMessage(record) for record in records],
        )
        return time.monotonic_ns()

    async def read(self, check, done, expected):
        api = self.api
        reader = api.IggyClient.from_connection_string(self.url)
        await reader.connect()
        consumer = api.Consumer.Single("benchmark")
        offset = 0
        while not done.is_set() or offset < expected:
            fetched = await reader.poll_messages(
                self.stream, self.topic, partition_id=0, consumer=consumer,
                polling_strategy=api.PollingStrategy.Offset(offset),
                options=api.PollOptions(count=256, max_wait=timedelta(milliseconds=10)),
            )
            received_at = time.monotonic_ns()
            for message in fetched.messages:
                if message.offset() != offset:
                    raise AssertionError("out of order Iggy offset")
                check(message.payload(), received_at)
                offset += 1
        await reader.store_consumer_offset(self.stream, self.topic, consumer=consumer,
                                           partition_id=0, offset=offset - 1)
        state = await reader.get_consumer_offset(self.stream, self.topic, consumer=consumer, partition_id=0)
        if state is None or state.stored_offset != offset - 1:
            raise AssertionError("Iggy did not return the saved consumer offset")


class JetStream:
    def __init__(self, endpoints, topic, rf):
        self.endpoints = ["nats://" + endpoint for endpoint in endpoints.split(",")]
        self.topic = topic
        self.rf = rf
        self.pending = []

    async def prepare(self):
        import nats
        from nats.js.api import StorageType, StreamConfig

        self.nc = await nats.connect(servers=self.endpoints, connect_timeout=1)
        self.js = self.nc.jetstream(timeout=4)
        await self.js.add_stream(config=StreamConfig(name=self.topic, subjects=[self.topic],
                                                     storage=StorageType.FILE, num_replicas=self.rf))

    async def publish(self, records, acknowledge):
        for record in records:
            ticket = await self.js.publish_async(self.topic, record, wait_stall=10)
            ticket.add_done_callback(lambda future, payload=record: acknowledge(future.result(), payload))
            self.pending.append(ticket)
        return None

    async def drain(self):
        await asyncio.gather(*self.pending)

    async def subscribe(self, check, failure):
        async def received(message):
            try:
                check(message.data, time.monotonic_ns())
                await message.ack()
            except Exception as error:
                failure.append(error)

        await self.js.subscribe(self.topic, durable="benchmark", cb=received, manual_ack=True)
        await self.nc.flush()

    async def committed(self, expected):
        await self.nc.flush()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            info = await self.js.consumer_info(self.topic, "benchmark")
            if info.ack_floor.stream_seq == expected:
                return
            await asyncio.sleep(.05)
        raise AssertionError("JetStream did not persist the final consumer acknowledgement")


async def exercise(broker, mode, endpoint, topic, size, rate, seconds, stage, rf, password):
    active = Iggy(endpoint, topic, password) if broker == "iggy" else JetStream(endpoint, topic, rf)
    deadline = time.monotonic() + 30
    while True:
        try:
            await active.prepare()
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(.25)
    read = mode == "mixed"
    ack_latencies, receive_latencies, failed = [], [], []
    warmup_count = WARMUP * rate
    expected = (WARMUP + seconds) * rate
    sent = acked = received = 0
    done = asyncio.Event()
    body = random.Random(23).randbytes(size - 16)

    def record_received(payload, received_at):
        nonlocal received
        start, seq = struct.unpack_from("!QQ", payload)
        if len(payload) != size or seq != received or payload[16:] != body:
            raise AssertionError(f"unexpected payload at {received}")
        if seq >= warmup_count:
            receive_latencies.append(received_at - start)
        received += 1

    def record_ack(when, record):
        nonlocal acked
        start, seq = struct.unpack_from("!QQ", record)
        if seq >= warmup_count:
            ack_latencies.append(when - start)
        acked += 1

    def completed(_, record):
        record_ack(time.monotonic_ns(), record)

    reader = None
    if read and broker == "nats":
        await active.subscribe(record_received, failed)
    elif read:
        reader = asyncio.create_task(active.read(record_received, done, expected))

    start = time.monotonic()
    for slot in range((WARMUP + seconds) * 100):
        await asyncio.sleep(max(0, start + slot * .01 - time.monotonic()))
        if slot == WARMUP * 100:
            steady = time.monotonic()
            ack_start, read_start = acked, received
            (stage / "steady").touch()
        if failed:
            raise failed[0]
        if reader and reader.done():
            reader.result()
        records = []
        count = (slot + 1) * rate // 100 - slot * rate // 100
        for _ in range(count):
            records.append(struct.pack("!QQ", time.monotonic_ns(), sent) + body)
            sent += 1
        if records:
            accepted = await active.publish(records, completed)
            if accepted is not None:
                for record in records:
                    record_ack(accepted, record)
    elapsed = time.monotonic() - steady
    ack_end, read_end = acked, received
    (stage / "end").touch()
    done.set()
    if broker == "nats":
        await active.drain()
        if read:
            deadline = time.monotonic() + 15
            while received < expected and time.monotonic() < deadline and not failed:
                await asyncio.sleep(.03)
            await active.committed(expected)
        await active.nc.close()
    if reader:
        await asyncio.wait_for(reader, 20)
    if failed:
        raise failed[0]
    if sent != expected or acked != expected or (read and received != expected):
        raise AssertionError(f"expected={expected} sent={sent} acknowledged={acked} received={received}")
    print(json.dumps({
        "seconds": elapsed, "sent_total": sent, "acked_total": acked,
        "consumed_and_committed_total": received, "offered_msg_s": (sent - warmup_count) / elapsed,
        "acked_msg_s": (ack_end - ack_start) / elapsed,
        "consumed_msg_s": (read_end - read_start) / elapsed,
        "p50_ms": percentile(ack_latencies, .50), "p95_ms": percentile(ack_latencies, .95),
        "p99_ms": percentile(ack_latencies, .99), "ack_samples": len(ack_latencies),
        "end_to_end_samples": len(receive_latencies),
        "end_to_end_p99_ms": percentile(receive_latencies, .99) if read else None,
    }), flush=True)


if __name__ == "__main__":
    broker, mode, seeds, topic, size, rate, seconds, stage, rf, password = sys.argv[1:]
    asyncio.run(exercise(broker, mode, seeds, topic, int(size), int(rate), int(seconds),
                         Path(stage), int(rf), password))
