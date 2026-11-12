import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from agentic_mesh.generator import cli
from agentic_mesh.generator.events import EVENT_TYPES

from .conftest import KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL

pytestmark = pytest.mark.integration


@pytest.fixture
def prefix() -> Iterator[str]:
    """Throwaway topics and subjects, deleted afterwards."""
    prefix = f"smoke-{uuid.uuid4().hex[:8]}."
    yield prefix
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})  # must outlive its futures
    for future in admin.delete_topics([prefix + t.topic for t in EVENT_TYPES.values()]).values():
        future.result()
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    for subject in registry.get_subjects():
        if subject.startswith(prefix):
            registry.delete_subject(subject)
            registry.delete_subject(subject, permanent=True)


def test_backfill_writes_avro_events_matching_ground_truth(prefix: str, tmp_path: Path) -> None:
    argv = ["backfill", "--hours", "0.5", "--seed", "5", "--scenario", "payment-outage"]
    assert cli.main([*argv, "--topic-prefix", prefix, "--truth-dir", str(tmp_path)]) == 0

    [truth_file] = tmp_path.glob("*.jsonl")
    totals = next(
        r for r in map(json.loads, truth_file.read_text().splitlines()) if r["record"] == "totals"
    )

    consumer = Consumer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": prefix, "enable.auto.commit": False}
    )
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserialize = AvroDeserializer(registry)
    try:
        for name, spec in EVENT_TYPES.items():
            topic = prefix + spec.topic
            # Exactly as many records as the ground truth counted (idempotent producer: no dupes).
            partitions = consumer.list_topics(topic).topics[topic].partitions
            offsets = [consumer.get_watermark_offsets(TopicPartition(topic, p)) for p in partitions]
            assert sum(high - low for low, high in offsets) == totals[name], name

            # Records decode with the registered schemas; the Kafka timestamp is the event time.
            consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
            msg = consumer.poll(10)
            assert msg is not None and msg.error() is None
            value = deserialize(msg.value(), SerializationContext(topic, MessageField.VALUE))
            key = deserialize(msg.key(), SerializationContext(topic, MessageField.KEY))
            assert key == {spec.key_field: value[spec.key_field]}
            assert msg.timestamp()[1] == int(value["event_time"].timestamp() * 1000)
    finally:
        consumer.close()
