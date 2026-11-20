"""Time the replica recovery and live expansion regression scenarios."""
import argparse
import json
import os
from pathlib import Path
import subprocess as sp
import time

from run import root


def run(script, output, env, phases):
    marks = {}
    started = time.monotonic()
    with (output / f"{script}.log").open("w") as log:
        with sp.Popen([str(root / "tests" / "java" / f"{script}-smoke.sh")], env=env,
                      stdout=sp.PIPE, stderr=sp.STDOUT, text=True) as process:
            for line in process.stdout:
                log.write(line)
                if line.startswith("benchmark-phase "):
                    marks[line.split()[1]] = time.monotonic() - started
            process.wait()
            if process.returncode:
                raise RuntimeError(f"{script} failed, see {log.name}")
    return {phase + "_seconds": marks[phase + "_completed"] - marks[phase + "_started"]
            for phase in phases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--image", default="rusta:roadmap6")
    options = parser.parse_args()
    options.output.mkdir()
    env = dict(os.environ, RUSTA_IMAGE=options.image)
    cache_env = dict(env, RUSTA_CACHE_RECOVERY_TAIL_RECORDS="8192",
                     RUSTA_CACHE_RECOVERY_RECORD_BYTES="1024")
    summary = run("cache-recovery", options.output, cache_env, ("cache_rejoin",))
    summary["cache_rejoin_payload_bytes"] = 8192 * 1024
    summary["cache_rejoin_payload_MiB_s_including_startup"] = 8 / summary["cache_rejoin_seconds"]
    summary.update(run("membership", options.output, env, ("expansion", "rebalance")))
    (options.output / "lifecycle.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
