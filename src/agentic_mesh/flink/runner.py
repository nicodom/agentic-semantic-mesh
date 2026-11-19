"""Start, stop and inspect the Flink jobs: `asm-flink start|stop|status` (ADR 0023).

`start` creates the metric topics, registers the metric schemas, and submits each job that is not
already running. A job resumes from the savepoint it was last stopped with, or reads its topics
from the beginning if it has none. `stop` stops each running job with a savepoint, so the windows
it had open carry on after the next `start`.

Savepoint paths are kept in `data/local/flink/savepoints.json`: Flink itself forgets finished
jobs when the JobManager restarts.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from .ddl import Stack
from .jobs import (
    JOB_NAMES,
    MATERIALIZATION_JOB,
    METRIC_JOBS,
    materialization_script,
    metric_script,
    schema_str,
)

REPO_ROOT = Path(__file__).resolve().parents[3]  # where compose.yaml is
SAVEPOINTS = REPO_ROOT / "data" / "local" / "flink" / "savepoints.json"
PARTITIONS = 3
ACTIVE = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING"}


def flink_url() -> str:
    return os.environ.get("FLINK_REST_URL") or "http://localhost:8081"


def prepare(bootstrap: str, registry_url: str, topic_prefix: str = "") -> None:
    """Create the metric topics and register their schemas before any job writes to them."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [topic_prefix + job.topic for job in METRIC_JOBS]
    new = [NewTopic(t, PARTITIONS, 1, config={"cleanup.policy": "delete"}) for t in topics]
    for future in admin.create_topics(new).values():
        try:
            future.result()
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    registry = SchemaRegistryClient({"url": registry_url})
    for job, topic in zip(METRIC_JOBS, topics, strict=True):
        for field, name in (("key", f"{job.name}_key"), ("value", job.name)):
            registry.register_schema(f"{topic}-{field}", Schema(schema_str(name), "AVRO"))


def submit(sql: str) -> list[str]:
    """Run a SQL script with the SQL client in the JobManager container; return its job IDs."""
    result = subprocess.run(
        [
            *("docker", "compose", "exec", "-T", "jobmanager", "bash", "-c"),
            "f=$(mktemp) && cat > $f && ./bin/sql-client.sh -f $f; rc=$?; rm -f $f; exit $rc",
        ],
        input=sql,
        text=True,
        capture_output=True,
        cwd=REPO_ROOT,
        timeout=1800,
    )
    output = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout)  # the client colours its output
    if result.returncode != 0 or "[ERROR]" in output:
        raise RuntimeError(output[output.find("[ERROR]") :] or result.stderr)
    return re.findall(r"Job ID: ([0-9a-f]{32})", output)


def _rest(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        flink_url() + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def active_jobs() -> dict[str, str]:
    """Job name -> job ID, for jobs that are running or about to."""
    jobs = _rest("GET", "/jobs/overview")["jobs"]
    return {j["name"]: j["jid"] for j in jobs if j["state"] in ACTIVE}


def stop_with_savepoint(job_id: str, job_name: str) -> str:
    """Stop a job, taking a savepoint first; return the savepoint's path."""
    target = f"s3://flink/savepoints/{job_name}"
    trigger = _rest("POST", f"/jobs/{job_id}/stop", {"targetDirectory": target, "drain": False})
    while True:
        status = _rest("GET", f"/jobs/{job_id}/savepoints/{trigger['request-id']}")
        if status["status"]["id"] == "COMPLETED":
            operation = status["operation"]
            if "failure-cause" in operation:
                raise RuntimeError(operation["failure-cause"].get("stack-trace", operation))
            return operation["location"]
        time.sleep(1)


def _load_savepoints() -> dict[str, str]:
    return json.loads(SAVEPOINTS.read_text()) if SAVEPOINTS.exists() else {}


def _save_savepoints(savepoints: dict[str, str]) -> None:
    SAVEPOINTS.parent.mkdir(parents=True, exist_ok=True)
    SAVEPOINTS.write_text(json.dumps(savepoints, indent=2) + "\n")


def script(job_name: str, stack: Stack, restore_from: str | None = None) -> str:
    if job_name == MATERIALIZATION_JOB:
        return materialization_script(stack, restore_from)
    [job] = [j for j in METRIC_JOBS if j.job_name == job_name]
    return metric_script(job, stack, restore_from)


def start(stack: Stack) -> None:
    running = active_jobs()
    # A savepoint stays recorded after a job resumes from it, until the next stop replaces it:
    # if the job is lost without one (JobManager restart), resuming from it repeats a few
    # windows instead of all of them.
    savepoints = _load_savepoints()
    for name in JOB_NAMES:
        if name in running:
            print(f"{name}: already running ({running[name]})")
            continue
        restore = savepoints.get(name)
        [job_id] = submit(script(name, stack, restore))
        print(f"{name}: started {job_id}" + (f" from {restore}" if restore else " from earliest"))


def stop() -> None:
    savepoints = _load_savepoints()
    for name, job_id in active_jobs().items():
        if name not in JOB_NAMES:
            continue
        savepoints[name] = stop_with_savepoint(job_id, name)
        _save_savepoints(savepoints)
        print(f"{name}: stopped, savepoint {savepoints[name]}")


def status() -> None:
    running = active_jobs()
    savepoints = _load_savepoints()
    for name in JOB_NAMES:
        if name in running:
            print(f"{name}: running ({running[name]})")
        else:
            print(
                f"{name}: stopped"
                + (f", savepoint {savepoints[name]}" if name in savepoints else "")
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="asm-flink", description=__doc__.split("\n")[0])
    parser.add_argument("command", choices=("start", "stop", "status"))
    args = parser.parse_args(argv)
    try:
        if args.command == "start":
            prepare(
                os.environ.get("KAFKA_BOOTSTRAP") or "localhost:9092",
                os.environ.get("SCHEMA_REGISTRY_URL") or "http://localhost:8085",
            )
            start(Stack())
        elif args.command == "stop":
            stop()
        else:
            status()
    except (OSError, RuntimeError) as e:
        print(f"asm-flink: {e}", file=sys.stderr)
        return 1
    return 0
