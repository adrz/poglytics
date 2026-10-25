#!/usr/bin/env python3
"""Run paired ingestion benchmarks in fresh containers; never read scraper .env."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, cwd=ROOT, **kwargs).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--rate", type=int, default=600)
    parser.add_argument("--snapshot-rate", type=int, default=60)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--image", default="clickhouse/clickhouse-server:latest")
    parser.add_argument("--output", default="benchmarks/latest.json")
    args = parser.parse_args()
    if min(args.seconds, args.rate, args.rounds) < 1 or args.snapshot_rate < 0:
        parser.error("duration, rate, and rounds must be positive; snapshot rate cannot be negative")
    # Pin every run to the same locally installed image, with no implicit pull.
    image = command("docker", "image", "inspect", "--format", "{{.Id}}", args.image)
    report = {"created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "image": image, "docker": json.loads(command("docker", "info", "--format", "{{json .}}")),
              "parameters": vars(args), "runs": []}
    # Keep only resource metadata, not daemon paths, labels, or proxy settings.
    report["docker"] = {key: report["docker"].get(key) for key in ["Architecture", "NCPU", "MemTotal", "ServerVersion", "KernelVersion"]}
    output = (ROOT / args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="poglytics-benchmark-") as temp:
        binary = str(Path(temp) / "ingest-benchmark")
        subprocess.run(["go", "build", "-o", binary, "./cmd/ingest-benchmark"], cwd=ROOT, check=True)
        for round_number in range(args.rounds):
            # Alternate order to reduce systematic warmup / host-load bias.
            modes = ["legacy", "table"] if round_number % 2 == 0 else ["table", "legacy"]
            for mode in modes:
                name = "poglytics-benchmark-" + uuid.uuid4().hex[:12]
                print(f"Round {round_number + 1}/{args.rounds}, {mode}: {args.seconds}s replay + 10s merge tail", flush=True)
                created = False
                try:
                    command("docker", "create", "--name", name, "--label", "poglytics.benchmark=true",
                            "--memory", "2g", "-p", "127.0.0.1::9000", "-e", "CLICKHOUSE_PASSWORD=benchmark", image)
                    created = True
                    command("docker", "start", name)
                    deadline = time.monotonic() + 60
                    while True:
                        ready = subprocess.run(["docker", "exec", name, "clickhouse-client", "--password", "benchmark", "--query", "SELECT 1"],
                                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        if ready.returncode == 0:
                            break
                        if time.monotonic() >= deadline:
                            raise RuntimeError(f"ClickHouse did not become ready; inspect {name}")
                        time.sleep(1)
                    port = command("docker", "port", name, "9000/tcp").rsplit(":", 1)[1]
                    data = json.loads(command(binary, "-port", port, "-container", name, "-mode", mode,
                                              "-database", f"poglytics_bench_{mode}_{round_number}",
                                              "-seconds", str(args.seconds), "-rate", str(args.rate), "-snapshot-rate", str(args.snapshot_rate),
                                              timeout=args.seconds + 120))
                    data["Round"] = round_number + 1
                    if report["runs"]:
                        reference = report["runs"][0]["Tables"]
                        if data["Tables"].keys() != reference.keys():
                            raise RuntimeError("table set differs across runs")
                        for table, values in data["Tables"].items():
                            for key in ["Rows", "DistinctRows", "Checksum"]:
                                if values[key] != reference[table][key]:
                                    raise RuntimeError(f"data differs across runs: {table}.{key}")
                    report["runs"].append(data)
                    output.write_text(json.dumps(report, indent=2) + "\n")
                    parts = sum(table["NewParts"] for table in data["Tables"].values())
                    merges = sum(table["Merges"] for table in data["Tables"].values())
                    print(f"  DB CPU {data['DBIngestCPUPercent']:.1f}% of one core; {data['Inserts']} inserts, {parts} parts, {merges} merges; {data['ExpectedRows']} rows verified", flush=True)
                finally:
                    if created:
                        # Remove only this invocation's unique temporary container
                        # and its anonymous volumes; no Compose services or bind mounts.
                        subprocess.run(["docker", "rm", "-fv", name], stdout=subprocess.DEVNULL, check=True)
    print(f"Results: {output}")


if __name__ == "__main__":
    main()
