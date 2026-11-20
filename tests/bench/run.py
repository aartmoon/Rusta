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
awk '{print "broker_ticks", $14+$15}' /proc/1/stat
printf 'ticks_per_sec '; getconf CLK_TCK
printf 'cgroup_current '; cat /sys/fs/cgroup/memory.current
awk '$1=="file"||$1=="anon"||$1=="sock"||$1=="inactive_file"{print $1,$2}' /sys/fs/cgroup/memory.stat
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

def run_phase(mode, topic, payload, ids, seeds, pool):
    stage = work / f"{mode}-{topic}"
    stage.mkdir()
    records = []
    target = args.rate
    measured = args.seconds
    summary = {}
    process = None
    log = stage / "java.log"
    try:
        deadline = time.monotonic() + measured
        if payload:
            with log.open("w") as handle:
                workload = "write" if topic == "write" else "mixed"
                process = sp.Popen(java(workload, seeds, topic, str(payload), str(target),
                                        str(measured), str(stage)), stdout=handle, stderr=sp.STDOUT)
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
            profiles = [("idle", 0), ("write", 1024), ("mix", 1024), ("mix_large", 4096)]
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
    args = parser.parse_args()
    if not (1024 <= args.base_port <= 65532 and args.rate > 0 and args.seconds >= 4):
        parser.error("use a base port from 1024 to 65532, a positive rate and at least four seconds")
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
            }
            (work / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
            with futures.ThreadPoolExecutor(max_workers=3) as pool:
                results = benchmark("single", pool, project) + benchmark("rf3", pool, project)
            (work / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        finally:
            if not args.image:
                sp.run(["docker", "image", "rm", image], stdout=sp.DEVNULL)
