"""The Flink SQL jobs of M1 (ADRs 0018, 0023) and the SQL scripts that start them.

Three metric jobs read event topics and write one metric topic each; the materialization job
copies every event and metric topic into Iceberg. Each job is one SQL script, submitted through
the SQL client in the JobManager container.
"""

import json
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import Any

from agentic_mesh.generator.events import EVENT_TYPES
from agentic_mesh.generator.events import schema as event_schema

from . import ddl
from .ddl import Stack

# Same catalog settings as the step 2 smoke test; S3 credentials come from the container's
# AWS_* environment (compose.yaml).
LAKE_CATALOG = """CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://rustfs:9000',
  's3.path-style-access' = 'true'
);"""


@dataclass(frozen=True)
class MetricJob:
    name: str  # also the base name of its schemas and SQL file
    sources: tuple[str, ...]  # event types it reads

    @property
    def topic(self) -> str:
        return f"metrics.{self.name}"

    @property
    def job_name(self) -> str:
        return self.topic


METRIC_JOBS: tuple[MetricJob, ...] = (
    MetricJob("orders_1m", ("order_placed",)),
    MetricJob("checkout_funnel_5m", ("checkout_started", "order_placed")),
    MetricJob("payments_by_provider_5m", ("payment_attempted",)),
)
MATERIALIZATION_JOB = "lake.materialization"
JOB_NAMES = (*(j.job_name for j in METRIC_JOBS), MATERIALIZATION_JOB)


@cache
def schema_str(name: str) -> str:
    """A metric schema (e.g. "orders_1m", "orders_1m_key") as JSON text."""
    return files(__package__).joinpath("schemas", f"{name}.avsc").read_text()


def schema(name: str) -> dict[str, Any]:
    return json.loads(schema_str(name))


def _sql(name: str) -> str:
    return files(__package__).joinpath("sql", f"{name}.sql").read_text()


def _settings(job_name: str, stack: Stack, restore_from: str | None) -> list[str]:
    settings = {
        "pipeline.name": job_name,
        # avro-confluent reads timestamp-millis as TIMESTAMP(3) in UTC (see ddl.py).
        "table.local-time-zone": "UTC",
        # A partition with no new events must not hold back the watermark of the others (live
        # mode: return_requested is rare).
        "table.exec.source.idle-timeout": "30 s",
        # Bounded runs (tests) wait for the job to finish; long-running jobs are submitted and
        # left running.
        "table.dml-sync": "true" if stack.bounded else "false",
    }
    if stack.bounded:
        # Fail instead of restarting, so an error ends the run rather than looping forever.
        settings["restart-strategy.type"] = "none"
    if restore_from:
        settings["execution.state-recovery.path"] = restore_from
    return [f"SET '{k}' = '{v}';" for k, v in settings.items()]


def metric_script(job: MetricJob, stack: Stack, restore_from: str | None = None) -> str:
    parts = _settings(job.job_name, stack, restore_from)
    for event in job.sources:
        parts.append(ddl.source_table(event, EVENT_TYPES[event].topic, event_schema(event), stack))
    parts.append(
        ddl.metric_sink_table(
            job.name, job.topic, schema(f"{job.name}_key"), schema(job.name), stack
        )
    )
    parts.append(_sql(job.name))
    return "\n\n".join(parts) + "\n"


def materialization_script(stack: Stack, restore_from: str | None = None) -> str:
    """A pure copy (ADR 0018) of every topic into `lake.shop.*` and `lake.metrics.*`."""
    tables = [
        ("shop", name, spec.topic, event_schema(name)) for name, spec in EVENT_TYPES.items()
    ] + [("metrics", job.name, job.topic, schema(job.name)) for job in METRIC_JOBS]

    parts = [*_settings(MATERIALIZATION_JOB, stack, restore_from), LAKE_CATALOG]
    for base in ("shop", "metrics"):
        parts.append(f"CREATE DATABASE IF NOT EXISTS lake.`{stack.database(base)}`;")
    inserts = []
    for base, name, topic, value_schema in tables:
        source = f"{base}_{name}"
        database = stack.database(base)
        parts.append(ddl.source_table(source, topic, value_schema, stack))
        parts.append(ddl.lake_table(database, name, value_schema))
        cols = ", ".join(
            f"`{c}`"
            for c in [f["name"] for f in value_schema["fields"]]
            + [c for c, _, _ in ddl.KAFKA_METADATA]
        )
        inserts.append(f"INSERT INTO lake.`{database}`.`{name}` SELECT {cols} FROM `{source}`;")
    # One job for all tables: a statement set runs its INSERTs together.
    parts.append("EXECUTE STATEMENT SET BEGIN\n" + "\n".join(inserts) + "\nEND;")
    return "\n\n".join(parts) + "\n"
