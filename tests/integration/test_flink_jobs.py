"""Flink metric jobs equal the generator's ground truth, and the lake holds every record (ADR 0023).

Runs every job bounded (read to the end of each topic, then finish) on a fresh backfill in
throwaway topics, Iceberg namespaces and schema subjects.
"""

import json
import uuid
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyiceberg.catalog import Catalog

from agentic_mesh.flink import runner
from agentic_mesh.flink.ddl import Stack
from agentic_mesh.flink.jobs import METRIC_JOBS, materialization_script, metric_script
from agentic_mesh.generator import cli
from agentic_mesh.generator.events import EVENT_TYPES

from .conftest import KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL

pytestmark = pytest.mark.integration

COUNTS = {
    "orders_1m": ("orders",),
    "checkout_funnel_5m": ("checkouts_started", "orders_placed"),
    "payments_by_provider_5m": ("attempts", "failures", "retries", "retries_succeeded"),
}
RATES = {"checkout_funnel_5m": "conversion", "payments_by_provider_5m": "failure_rate"}


@pytest.fixture
def stack(catalog: Catalog) -> Iterator[Stack]:
    prefix = f"smoke-{uuid.uuid4().hex[:8]}."
    stack = Stack(topic_prefix=prefix, bounded=True)
    yield stack
    topics = [prefix + t.topic for t in EVENT_TYPES.values()] + [
        prefix + j.topic for j in METRIC_JOBS
    ]
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})  # must outlive its futures
    for future in admin.delete_topics(topics).values():
        future.result()
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    for subject in registry.get_subjects():
        if subject.startswith(prefix):
            registry.delete_subject(subject)
            registry.delete_subject(subject, permanent=True)
    for base in ("shop", "metrics"):
        namespace = stack.database(base)
        if catalog.namespace_exists(namespace):
            for table in catalog.list_tables(namespace):
                catalog.purge_table(table)
            catalog.drop_namespace(namespace)


def read_topic(topic: str) -> list[tuple[dict[str, Any], dict[str, Any], int, int]]:
    """Every record of a topic, decoded: (key, value, partition, offset)."""
    consumer = Consumer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": topic, "enable.auto.commit": False}
    )
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}))
    try:
        partitions = consumer.list_topics(topic).topics[topic].partitions
        left = sum(consumer.get_watermark_offsets(TopicPartition(topic, p))[1] for p in partitions)
        consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
        records = []
        while left:
            msg = consumer.poll(10)
            assert msg is not None and msg.error() is None
            key = deserialize(msg.key(), SerializationContext(topic, MessageField.KEY))
            value = deserialize(msg.value(), SerializationContext(topic, MessageField.VALUE))
            records.append((key, value, msg.partition(), msg.offset()))
            left -= 1
        return records
    finally:
        consumer.close()


def row_key(record: dict[str, Any], key_fields: list[str]) -> tuple[Any, ...]:
    """Comparable key of a Flink record (datetimes) or a ground-truth row (ISO strings)."""
    return tuple(
        v.isoformat(timespec="milliseconds") if isinstance(v, datetime) else v
        for v in (record[f] for f in key_fields)
    )


def test_metrics_equal_ground_truth_and_lake_holds_every_record(
    stack: Stack, catalog: Catalog, tmp_path: Path
) -> None:
    argv = ["backfill", "--hours", "1", "--seed", "5", "--scenario", "payment-outage"]
    assert (
        cli.main([*argv, "--topic-prefix", stack.topic_prefix, "--truth-dir", str(tmp_path)]) == 0
    )
    [truth_file] = tmp_path.glob("*.jsonl")
    truth = [json.loads(line) for line in truth_file.read_text().splitlines()]
    totals = next(r for r in truth if r["record"] == "totals")

    runner.prepare(KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL, stack.topic_prefix)
    for job in METRIC_JOBS:
        runner.submit(metric_script(job, stack))
    runner.submit(materialization_script(stack))

    metric_counts = {}
    for job in METRIC_JOBS:
        records = read_topic(stack.topic_prefix + job.topic)
        metric_counts[job.name] = len(records)
        key_fields = [k for k in ("window_start", "provider") if k in records[0][0]]
        flink = {}
        for key, value, _, _ in records:
            assert key == {f: value[f] for f in key_fields}
            flink[row_key(value, key_fields)] = value
        assert len(flink) == len(records), f"{job.name}: repeated windows in a single run"

        expected = [r for r in truth if r["record"] == job.name]
        for row in expected:
            # Windows with no events have no Flink record; the ground truth has a zero row.
            got = flink.pop(row_key(row, key_fields), None)
            for field in COUNTS[job.name]:
                assert (got[field] if got else 0) == row[field], (job.name, row)
            if got and job.name in RATES:
                assert got[RATES[job.name]] == row[RATES[job.name]], (job.name, row)
        assert not flink, f"{job.name}: windows missing from the ground truth: {list(flink)[:3]}"

    for name in EVENT_TYPES:
        table = catalog.load_table(f"{stack.database('shop')}.{name}").scan().to_arrow()
        assert table.num_rows == totals[name], name
        positions = set(
            zip(
                table["kafka_partition"].to_pylist(),
                table["kafka_offset"].to_pylist(),
                strict=True,
            )
        )
        assert len(positions) == table.num_rows, f"{name}: a record was copied twice"
    for job in METRIC_JOBS:
        table = catalog.load_table(f"{stack.database('metrics')}.{job.name}").scan().to_arrow()
        assert table.num_rows == metric_counts[job.name], job.name
