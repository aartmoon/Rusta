"""Run isolated broker profiles against Docker named volumes."""
import argparse
import concurrent.futures
from copy import copy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import secrets
import socket
import subprocess as sp
import sys
import time
import uuid

from run import command, run_phase, root

IMAGES = {
    "rusta": "rusta:roadmap6",
    "kafka": "apache/kafka:4.1.2",
    "redpanda": "docker.redpanda.com/redpandadata/redpanda:v26.3.1",
    "iggy": "apache/iggy:0.10.0",
    "nats": "nats:2.12.4-alpine",
}
DATA = {
    "rusta": "/var/lib/rusta", "kafka": "/var/lib/kafka/data",
    "redpanda": "/var/lib/redpanda/data", "iggy": "/app/local_data", "nats": "/data",
}


@dataclass(frozen=True)
class Profile:
    broker: str
    rf: int
    policy: str

    @property
    def name(self):
        return f"{self.broker}-rf{self.rf}-{self.policy}"


PROFILES = {
    profile.name: profile
    for broker, factors, policies in [
        ("rusta", (1,), ("durable",)),
        ("rusta", (3,), ("cache", "durable")),
        ("kafka", (1, 3), ("cache", "flush")),
        ("redpanda", (1, 3), ("cache", "durable")),
        ("iggy", (1,), ("buffered",)),
        ("nats", (1, 3), ("cache", "sync")),
    ]
    for rf in factors for policy in policies for profile in [Profile(broker, rf, policy)]
}


def docker(*args, **kwargs):
    return command(["docker", *map(str, args)], **kwargs)


def java(output, *args):
    return ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
            "-cp", f"{output}:{output}/deps/*", "ResourceProbe", *map(str, args)]


def local_socket(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
            return True
    except OSError:
        return False


def wait_for(check, label, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {label}")


class Cluster:
    def __init__(self, profile, output, options, image):
        self.profile, self.output, self.options, self.image = profile, output, options, image
        self.project = f"rusta-compare-{uuid.uuid4().hex[:10]}"
        self.nodes = {}
        self.volumes = []
        self.network = False
        self.password = "BenchA9" + secrets.token_urlsafe(20)
        self.ports = list(range(options.base_port, options.base_port + profile.rf))
        self.bootstrap = ",".join(f"127.0.0.1:{port}" for port in self.ports)
        self.pki = output / "pki"
        self.cluster_id = str(uuid.uuid4())
        self.kafka_cluster_id = self.cluster_id.replace("-", "")[:22]

    def settings(self, i, host_port):
        broker, rf, policy = self.profile.broker, self.profile.rf, self.profile.policy
        node = f"node{i}"
        environment, mounts, argv = {}, [], []
        if broker == "rusta":
            environment["RUSTA_ADVERTISED"] = f"127.0.0.1:{host_port}"
            environment["RUSTA_INTERNAL_BIND"] = "0.0.0.0:29092"
            environment["RUSTA_INTERNAL_ADVERTISED"] = f"{node}:29092"
            if rf > 1:
                environment.update({
                    "RUSTA_NODE_ID": str(i), "RUSTA_CLUSTER_ID": self.cluster_id,
                    "RUSTA_CLUSTER_CERTS": "/etc/rusta/pki",
                    "RUSTA_CLUSTER_ACK_MODE": "page-cache" if policy == "cache" else "durable",
                    "RUSTA_CLUSTER_PEERS": ";".join(
                        f"{j},raft{j}:7000,127.0.0.1:{port},node{j}:29092"
                        for j, port in enumerate(self.ports, 1)
                    ),
                })
                mounts.extend([
                    f"type=bind,src={self.pki / 'public'},dst=/etc/rusta/pki,readonly",
                    f"type=bind,src={self.pki / f'node-{i}.key'},dst=/etc/rusta/pki/key.pem,readonly",
                ])
            port = 9092
        elif broker == "kafka":
            environment.update({
                "CLUSTER_ID": self.kafka_cluster_id, "KAFKA_NODE_ID": str(i),
                "KAFKA_PROCESS_ROLES": "broker,controller",
                "KAFKA_CONTROLLER_QUORUM_VOTERS": ",".join(f"{j}@node{j}:29093" for j in range(1, rf + 1)),
                "KAFKA_LISTENERS": "OUTSIDE://:9092,INTERNAL://:29092,CONTROLLER://:29093",
                "KAFKA_ADVERTISED_LISTENERS": f"OUTSIDE://127.0.0.1:{host_port},INTERNAL://{node}:29092",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "OUTSIDE:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
                "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": str(rf),
                "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "4",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
                "KAFKA_MIN_INSYNC_REPLICAS": str(2 if rf == 3 else 1),
                "KAFKA_HEAP_OPTS": "-Xms256m -Xmx256m",
                "KAFKA_LOG_DIRS": DATA[broker],
            })
            port = 9092
        elif broker == "redpanda":
            argv = ["redpanda", "start", "--overprovisioned", "--check=false",
                    "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
                    "--kafka-addr", "internal://0.0.0.0:29092,external://0.0.0.0:9092",
                    "--advertise-kafka-addr", f"internal://{node}:29092,external://127.0.0.1:{host_port}",
                    "--rpc-addr", "0.0.0.0:33145", "--advertise-rpc-addr", f"{node}:33145",
                    "--set", "redpanda.group_topic_partitions=4"]
            if i > 1:
                argv += ["--seeds", "node1:33145"]
            port = 9092
        elif broker == "nats":
            config = self.output / f"{node}.conf"
            # A fresh store on an isolated bridge. Routes only listen on that bridge.
            routes = ", ".join(f"nats://node{j}:6222" for j in range(1, rf + 1) if j != i)
            cluster = f'cluster {{\nname: bench\nlisten: "0.0.0.0:6222"\nroutes: [{routes}]\n}}' if routes else ""
            sync = "sync_interval: always" if policy == "sync" else ""
            config.write_text(f'server_name: {node}\njetstream {{\nstore_dir: /data\n{sync}\n}}\n{cluster}\n')
            mounts.append(f"type=bind,src={config},dst=/bench.conf,readonly")
            argv = ["-c", "/bench.conf"]
            port = 4222
        else:
            environment.update({
                "IGGY_ROOT_USERNAME": "bench", "IGGY_ROOT_PASSWORD": self.password,
                "IGGY_TCP_ADDRESS": "0.0.0.0:8090", "IGGY_NODE_ADVERTISED_ADDRESS": "127.0.0.1",
                "IGGY_HTTP_ENABLED": "false", "IGGY_QUIC_ENABLED": "false", "IGGY_WEBSOCKET_ENABLED": "false",
            })
            port = 8090
        return port, environment, mounts, argv

    def start(self):
        if self.profile.broker == "rusta" and self.profile.rf > 1:
            command([str(root / "scripts/cluster-certs.sh"), str(self.pki), str(self.profile.rf)],
                    stdout=sp.DEVNULL, stderr=sp.DEVNULL)
        network_args = ["--subnet", self.options.subnet] if self.options.subnet else []
        docker("network", "create", *network_args, self.project, stdout=sp.DEVNULL)
        self.network = True
        started = time.monotonic()
        for i, host_port in enumerate(self.ports, 1):
            node = f"node{i}"
            volume = f"{self.project}-{node}"
            docker("volume", "create", volume, stdout=sp.DEVNULL)
            self.volumes.append(volume)
            port, env, mounts, argv = self.settings(i, host_port)
            flags = ["--security-opt", "seccomp=unconfined"] if self.profile.broker == "iggy" else []
            options = ["-e" + key for key in env]
            for mount in mounts:
                options.extend(["--mount", mount])
            self.nodes[node] = docker(
                "run", "-d", "--name", f"{self.project}-{node}",
                "--network", self.project, "--network-alias", node,
                "--network-alias", f"raft{i}",
                "--cpus", "1", "--memory", self.options.memory,
                "-p", f"127.0.0.1:{host_port}:{port}",
                "--mount", f"type=volume,src={volume},dst={DATA[self.profile.broker]}",
                *flags, *options, self.image, *argv, capture_output=True, env={**os.environ, **env},
            ).stdout.strip()
        wait_for(lambda: all(map(local_socket, self.ports)), "published ports")
        if self.profile.broker in ("kafka", "redpanda", "rusta"):
            def metadata_ready():
                probe = sp.run(java(self.options.output, "ready", self.bootstrap, "-"),
                               text=True, capture_output=True, timeout=8)
                return probe.returncode == 0 and probe.stdout.strip() == str(self.profile.rf)
            wait_for(metadata_ready, "Kafka metadata")
        return time.monotonic() - started

    def describe(self):
        nodes = {}
        for name, cid in self.nodes.items():
            probe = docker("exec", cid, "sh", "-c",
                           f"stat -f -c %T {DATA[self.profile.broker]}; cat /proc/1/comm",
                           capture_output=True)
            filesystem, process = probe.stdout.strip().splitlines()
            nodes[name] = {"filesystem": filesystem, "pid1": process}
        return nodes

    def close(self):
        for name, cid in self.nodes.items():
            with (self.output / f"{name}.log").open("w") as log:
                sp.run(["docker", "logs", "--tail", "500", cid], stdout=log, stderr=sp.STDOUT)
            sp.run(["docker", "rm", "-fv", cid], stdout=sp.DEVNULL)
        for volume in self.volumes:
            sp.run(["docker", "volume", "rm", volume], stdout=sp.DEVNULL)
        if self.network:
            sp.run(["docker", "network", "rm", self.project], stdout=sp.DEVNULL)


def scenario(cluster, output, topic, workload, size, partitions=6, options=None):
    settings = options or cluster.options
    broker = cluster.profile.broker
    native = broker in ("nats", "iggy")
    overrides = "-"
    if broker == "kafka" and cluster.profile.policy == "flush":
        overrides = "flush.messages=1"
    if broker == "redpanda" and cluster.profile.policy == "cache":
        overrides = "write.caching=true"
    if not native and size:
        command(java(settings.output, "prepare", cluster.bootstrap, topic, cluster.profile.rf, partitions, overrides))

    def client(stage):
        arguments = [cluster.bootstrap, topic, size, settings.rate, settings.seconds, stage]
        if native:
            return [settings.native_python, str(root / "tests/bench/native_probe.py"),
                    broker, workload, *map(str, arguments), str(cluster.profile.rf), cluster.password]
        return java(settings.output, workload, *arguments, settings.linger_ms,
                    settings.fetch_max_wait_ms, partitions)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(cluster.nodes)) as pool:
        result = run_phase(cluster.profile.name, topic, size, cluster.nodes, cluster.bootstrap, pool,
                           output=output, settings=settings, client=client)
    result.update(workload=workload, partitions=partitions if not native else 1, target_rate=settings.rate)
    if size:
        result["kafka"]["acked_payload_MiB_s"] = result["kafka"]["acked_msg_s"] * size / 2**20
    if native:
        result["native"] = result.pop("kafka")
    return result


def shapes(cluster, output, options):
    """Change the workload shape while keeping the offered aggregate rate."""
    first = scenario(cluster, output, "shape-one", "mixed", 4096, partitions=1)
    split = copy(options)
    split.rate = max(1, options.rate // 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [
            pool.submit(scenario, cluster, output, f"shape-pair-{i}", "mixed", 4096, 3, split)
            for i in (1, 2)
        ]
        pair = [task.result() for task in tasks]
    return {
        "one_topic_one_partition_one_client_pair": first,
        "two_topics_three_partitions_each_two_client_pairs": pair,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--profiles", nargs="+", choices=PROFILES, default=list(PROFILES))
    parser.add_argument("--rusta-image", default=IMAGES["rusta"])
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--seconds", type=int, default=8)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--base-port", type=int, default=19592)
    parser.add_argument("--memory", default="1g")
    parser.add_argument("--subnet", help="optional dedicated Docker bridge subnet")
    parser.add_argument("--native-python", default=sys.executable)
    parser.add_argument("--linger-ms", type=int, default=0)
    parser.add_argument("--fetch-max-wait-ms", type=int, default=10)
    parser.add_argument("--shape-matrix", action="store_true",
                        help="also exercise single-partition and concurrent 4 KiB workloads on Rusta")
    options = parser.parse_args()
    if options.repeat < 1 or options.seconds < 4 or options.rate < 1 or not 1024 <= options.base_port <= 65533:
        parser.error("choose positive repetitions and rate, at least four seconds and three unprivileged ports")
    options.output = options.output.resolve()
    options.output.mkdir()
    command(["mvn", "-B", "-q", "-f", str(root / "tests/java/pom.xml"),
             "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies",
             f"-DoutputDirectory={options.output / 'deps'}"])
    command(["javac", "-cp", f"{options.output}/deps/*", "-d", str(options.output),
             str(root / "tests/bench/ResourceProbe.java")])
    metadata = {
        "utc": datetime.now(timezone.utc).isoformat(), "host": platform.platform(),
        "docker": json.loads(docker("version", "--format", "{{json .Server}}", capture_output=True).stdout),
        "docker_vm": docker("info", "--format", "{{.NCPU}} CPU, {{.MemTotal}} bytes, {{.Driver}}, cgroup {{.CgroupVersion}}",
                            capture_output=True).stdout.strip(),
        "commit": command(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True).stdout.strip(),
        "dirty_worktree": bool(command(["git", "status", "--porcelain"], cwd=root, capture_output=True).stdout.strip()),
        "memory_limit_per_broker": options.memory, "cpu_quota_per_broker": 1,
        "rate": options.rate, "warmup_seconds": 4, "seconds": options.seconds,
        "repetitions": options.repeat, "linger_ms": options.linger_ms,
        "fetch_max_wait_ms": options.fetch_max_wait_ms,
    }
    (options.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    failed = False
    for name in options.profiles:
        profile = PROFILES[name]
        report = options.output / name
        report.mkdir()
        image = options.rusta_image if profile.broker == "rusta" else IMAGES[profile.broker]
        cluster = Cluster(profile, report, options, image)
        result = {"profile": name, "rf": profile.rf, "policy": profile.policy, "image": image, "samples": []}
        print(f"Running {name}", flush=True)
        try:
            inspected = json.loads(docker("image", "inspect", image, capture_output=True).stdout)[0]
            result["image_id"] = inspected["Id"]
            result["image_digests"] = inspected.get("RepoDigests", [])
            result["image_bytes"] = inspected["Size"]
            result["startup_seconds"] = cluster.start()
            result["nodes"] = cluster.describe()
            result["samples"].append(scenario(cluster, report, "idle", "idle", 0))
            for rep in range(1, options.repeat + 1):
                for case, payload in (("write", 512), ("mixed", 1024)):
                    result["samples"].append(scenario(cluster, report, f"{case}-{rep}", case, payload))
            if options.shape_matrix and profile.broker == "rusta":
                result["shapes"] = shapes(cluster, report, options)
        except (RuntimeError, ValueError, sp.SubprocessError) as error:
            result["error"] = str(error)
            print(f"{name}: {error}", file=sys.stderr, flush=True)
            failed = True
        finally:
            (report / "results.json").write_text(json.dumps(result, indent=2) + "\n")
            cluster.close()
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
