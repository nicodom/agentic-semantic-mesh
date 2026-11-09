import uuid

import pytest
from confluent_kafka import Consumer, Producer

from .conftest import KAFKA_BOOTSTRAP

pytestmark = pytest.mark.integration


def test_produce_and_consume_round_trip() -> None:
    topic = f"smoke-{uuid.uuid4().hex[:8]}"  # auto-created on first write
    payload = uuid.uuid4().hex.encode()

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(topic, value=payload)
    assert producer.flush(10) == 0, "message not delivered"

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"smoke-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    try:
        received = None
        for _ in range(30):
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                received = msg.value()
                break
        assert received == payload
    finally:
        consumer.close()
