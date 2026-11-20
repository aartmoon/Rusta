import argparse
import concurrent.futures as futures
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess as sp
import tempfile
import time

root = Path(__file__).resolve().parents[2]
cgroup_probe = """
awk '/^VmRSS:|^VmHWM:|^RssAnon:|^RssFile:/{gsub(":","",$1);print $1,$2*1024}' /proc/1/status
if test -r /proc/1/smaps; then
    awk '/^[0-9a-f]+-/ {heap=($NF=="[heap]")}
         heap && /^Rss:/ {kib+=$2}
         END {print "brk_heap_rss", kib*1024}' /proc/1/smaps
fi
awk '{print "broker_ticks", $14+$15}' /proc/1/stat
printf 'ticks_per_sec '; getconf CLK_TCK
printf 'cgroup_current '; cat /sys/fs/cgroup/memory.current
awk '$1=="file"||$1=="anon"||$1=="sock"||$1=="inactive_file"{print $1,$2}' /sys/fs/cgroup/memory.stat
if test -r /sys/fs/cgroup/memory.peak; then
    printf 'cgroup_peak '; cat /sys/fs/cgroup/memory.peak
fi
awk '{
    for (i=2; i<=NF; i++) {
        split($i, pair, "=");
        totals[pair[1]] += pair[2]
    }
} END { print "disk_read_bytes", totals["rbytes"]+0; print "disk_write_bytes", totals["wbytes"]+0;
        print "disk_read_ios", totals["rios"]+0; print "disk_write_ios", totals["wios"]+0; }' /sys/fs/cgroup/io.stat
awk '$1=="usage_usec"{print $1,$2}' /sys/fs/cgroup/cpu.stat
"""

def command(argv, **kwargs):
    return sp.run(argv, check=True, text=True, **kwargs)

def sample(cid):
    point = command(["docker", "exec", cid, "sh", "-ec", cgroup_probe], capture_output=True, timeout=10)
    return {key: int(float(value)) for key, value in map(str.split, point.stdout.splitlines())}

def collect(ids, samples, pool):
    data = dict(zip(ids, pool.map(sample, ids.values())))
    samples.append({"t": time.monotonic(), "nodes": data})

def run_phase(mode, topic, payload, ids, seeds, pool, *, output=None, settings=None, client=None):
    output = work if output is None else output
    settings = args if settings is None else settings
    stage = output / f"{mode}-{topic}"
    stage.mkdir()
    records = []
    target = settings.rate
    measured = settings.seconds
    summary = {}
    process = None
    log = stage / "java.log"
    try:
        deadline = time.monotonic() + measured
        if payload:
            with log.open("w") as handle:
                if client is None:
                    workload = "write" if topic == "write" else "mixed"
                    argv = java(workload, seeds, topic, str(payload), str(target),
                                str(measured), str(stage), str(settings.linger_ms),
                                str(settings.fetch_max_wait_ms))
                else:
                    argv = client(stage)
                process = sp.Popen(argv, stdout=handle, stderr=sp.STDOUT)
            warmup_deadline = time.monotonic() + 60
            while not (stage / "steady").exists():
                if process.poll() is not None or time.monotonic() > warmup_deadline:
                    raise RuntimeError(f"Workload failed during warmup: {log.read_text()}")
                time.sleep(0.1)
            deadline = time.monotonic() + measured + 6
        while time.monotonic() < deadline:
            if process and ((stage / "end").exists() or process.poll() is not None):
                break
            collect(ids, records, pool)
            time.sleep(.95)
        if process:
            code = process.wait(timeout=45)
            result = log.read_text()
            if code != 0:
                raise RuntimeError(result)
            summary = json.loads(result.splitlines()[-1])
        if len(records) < 2:
            raise RuntimeError(f"Too few resource samples for {mode}/{topic}")
        span = records[-1]["t"] - records[0]["t"]
        node_summary = {}
        for name in ids:
            snapshots = [v["nodes"][name] for v in records]
            first, last = snapshots[0], snapshots[-1]
            node_summary[name] = {
                "rss_avg_MiB": statistics.mean(n["VmRSS"] for n in snapshots) / 2**20,
                "rss_peak_MiB": max(n["VmRSS"] for n in snapshots) / 2**20,
                "rss_anon_avg_MiB": statistics.mean(n["RssAnon"] for n in snapshots) / 2**20,
                "cpu_avg_pct_1core": 100 * (last["broker_ticks"] - first["broker_ticks"]) / last["ticks_per_sec"] / span,
                "cgroup_cpu_avg_pct_1core": 100 * (last["usage_usec"] - first["usage_usec"]) / 1e6 / span,
                "cgroup_avg_MiB": statistics.mean(n["cgroup_current"] for n in snapshots) / 2**20,
                "file_cache_end_MiB": last["file"] / 2**20,
                "cgroup_end_MiB": last["cgroup_current"] / 2**20,
                "cgroup_peak_MiB": max(n.get("cgroup_peak", n["cgroup_current"]) for n in snapshots) / 2**20,
                "socket_end_bytes": last.get("sock", 0),
                "brk_heap_rss_end_bytes": last.get("brk_heap_rss"),
                "disk_read_bytes": last["disk_read_bytes"] - first["disk_read_bytes"],
                "disk_write_bytes": last["disk_write_bytes"] - first["disk_write_bytes"],
                "disk_read_ios": last["disk_read_ios"] - first["disk_read_ios"],
                "disk_write_ios": last["disk_write_ios"] - first["disk_write_ios"],
            }
        (stage/"samples.json").write_text(json.dumps(records))
        result = {"mode":mode,"scenario":topic,"payload_bytes":payload,"samples":len(records),
                  "sampled_seconds":span,"nodes":node_summary,"kafka":summary}
        print(json.dumps(result), flush=True)
        return result
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait()

def java(*args):
    return ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
            "-cp", f"{work}:{work}/deps/*", "ResourceProbe", *args]


def benchmark(mode, pool, project):
    if mode == "rf3":
        manifest = root / "compose.cluster.yaml"
        ports = list(range(args.base_port + 1, args.base_port + 4))
        services = ["broker1", "broker2", "broker3"]
        env = {f"RUSTA_PORT_{i}": str(port) for i, port in enumerate(ports, 1)}
        env.update(RUSTA_CLUSTER_PKI=str(work / "pki"), RUSTA_CLUSTER_SNAPSHOT_INTERVAL="4096")
    else:
        manifest = root / "compose.yaml"
        ports = [args.base_port]
        services = ["broker"]
        env = {"RUSTA_PORT": str(args.base_port)}
    seeds = ",".join(f"127.0.0.1:{port}" for port in ports)
    env = dict(os.environ, **env)
    env["RUSTA_IMAGE"] = image
    compose = ["docker", "compose", "--project-directory", str(root), "-f", str(manifest),
               "-p", f"{project}-{mode}"]
    with (work / f"{mode}.log").open("w") as log:
        try:
            command(compose + ["up", "--no-build", "-d", "--wait"], env=env, stdout=log,
                    stderr=sp.STDOUT, timeout=90)
            command(java("prepare", seeds, "all", str(len(services))), stdout=log,
                    stderr=sp.STDOUT, timeout=60)
            ids = {name: command(compose + ["ps", "-q", name], env=env,
                                 capture_output=True).stdout.strip() for name in services}
            time.sleep(3)
            profiles = ([("write", 512), ("mix", 1024)] if args.rf3_latency
                        else [("idle", 0), ("write", 1024), ("mix", 1024), ("mix_large", 4096)])
            return [run_phase(mode, topic, payload, ids, seeds, pool) for topic, payload in profiles]
        finally:
            sp.run(compose + ["logs"], env=env, stdout=log, stderr=sp.STDOUT, timeout=30)
            sp.run(compose + ["down", "--volumes", "--remove-orphans"], env=env, stdout=log,
                   stderr=sp.STDOUT, timeout=40)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Measure Rusta with the Kafka Java client")
    parser.add_argument("output", type=Path, help="new directory for raw samples and results")
    parser.add_argument("--image", help="existing local image to exercise")
    parser.add_argument("--base-port", type=int, default=19692)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--seconds", type=int, default=16)
    parser.add_argument("--rf3-latency", action="store_true",
                        help="measure 512-byte ACK and 1 KiB end-to-end latency on RF3")
    parser.add_argument("--linger-ms", type=int, default=5)
    parser.add_argument("--fetch-max-wait-ms", type=int, default=500)
    args = parser.parse_args()
    if not (1024 <= args.base_port <= 65532 and args.rate > 0 and args.seconds >= 4):
        parser.error("use a base port from 1024 to 65532, a positive rate and at least four seconds")
    if args.linger_ms < 0 or args.fetch_max_wait_ms < 0:
        parser.error("linger and fetch wait must be nonnegative")
    work = args.output.resolve()
    work.mkdir()
    with tempfile.TemporaryDirectory(prefix="rusta-bench-") as token:
        project = Path(token).name.lower()
        image = args.image or f"rusta:{project}"
        try:
            if not args.image:
                command(["docker", "build", "-t", image, "."], cwd=root)
            command([str(root / "scripts/cluster-certs.sh"), str(work / "pki"), "3"],
                    stdout=sp.DEVNULL)
            command(["mvn", "-B", "-q", "-f", str(root / "tests/java/pom.xml"),
                     "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies",
                     f"-DoutputDirectory={work / 'deps'}"])
            command(["javac", "-cp", f"{work}/deps/*", "-d", str(work),
                     str(root / "tests/bench/ResourceProbe.java")])
            metadata = {
                "host": platform.platform(),
                "docker": command(["docker", "version", "--format", "{{json .Server}}"],
                                  capture_output=True).stdout.strip(),
                "image": command(["docker", "image", "inspect", "--format", "{{.Id}}", image],
                                 capture_output=True).stdout.strip(),
                "git_head": command(["git", "rev-parse", "HEAD"], cwd=root,
                                    capture_output=True).stdout.strip(),
                "git_dirty": bool(command(["git", "status", "--porcelain"], cwd=root,
                                          capture_output=True).stdout.strip()),
                "target_messages_per_second": args.rate,
                "seconds": args.seconds,
                "rf3_latency": args.rf3_latency,
                "cluster_ack_mode": os.environ.get("RUSTA_CLUSTER_ACK_MODE", "auto"),
                "producer_linger_ms": args.linger_ms,
                "consumer_fetch_max_wait_ms": args.fetch_max_wait_ms,
            }
            (work / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
            with futures.ThreadPoolExecutor(max_workers=3) as pool:
                results = [] if args.rf3_latency else benchmark("single", pool, project)
                results += benchmark("rf3", pool, project)
            (work / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        finally:
            if not args.image:
                sp.run(["docker", "image", "rm", image], stdout=sp.DEVNULL)
