"""Kafka topic -> Flink SQL pure copy -> Iceberg table: the materialization path of ADR 0018."""

import json
import subprocess
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Producer
from pyiceberg.catalog import Catalog

from .conftest import KAFKA_BOOTSTRAP

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]

# Runs inside the stack, so it uses container hostnames (kafka:19092, iceberg-rest, rustfs).
MATERIALIZE_SQL = """
SET 'table.dml-sync' = 'true';

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://rustfs:9000',
  's3.path-style-access' = 'true'
);
CREATE DATABASE IF NOT EXISTS lake.smoke;
CREATE TABLE lake.smoke.{table} (id BIGINT, label STRING);

-- Bounded: read what is in the topic now, then finish (final checkpoint commits to Iceberg).
CREATE TEMPORARY TABLE source_topic (id BIGINT, label STRING) WITH (
  'connector' = 'kafka',
  'topic' = '{topic}',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = '{table}',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json'
);

INSERT INTO lake.smoke.{table} SELECT * FROM source_topic;
"""


def run_flink_sql(sql: str) -> None:
    """Submit a SQL script through the SQL client inside the JobManager container."""
    result = subprocess.run(
        [
            *("docker", "compose", "exec", "-T", "jobmanager", "bash", "-c"),
            "cat > /tmp/job.sql && ./bin/sql-client.sh -f /tmp/job.sql",
        ],
        input=sql,
        text=True,
        capture_output=True,
        cwd=REPO_ROOT,
        timeout=300,
    )
    assert result.returncode == 0 and "[ERROR]" not in result.stdout, result.stdout + result.stderr


def test_topic_is_materialized_into_iceberg(catalog: Catalog) -> None:
    suffix = uuid.uuid4().hex[:8]
    topic, table = f"smoke-mat-{suffix}", f"t_{suffix}"
    rows = [{"id": i, "label": f"row-{i}"} for i in range(5)]

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for row in rows:
        producer.produce(topic, value=json.dumps(row).encode())
    assert producer.flush(10) == 0, "messages not delivered"

    run_flink_sql(MATERIALIZE_SQL.format(topic=topic, table=table))

    try:
        result = catalog.load_table(f"smoke.{table}").scan().to_arrow().sort_by("id")
        assert result.to_pylist() == rows
    finally:
        catalog.purge_table(f"smoke.{table}")
