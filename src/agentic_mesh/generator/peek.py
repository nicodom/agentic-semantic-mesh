"""`asm-peek`: print events from a topic, decoded from Avro with the registry's schemas.

asm-peek shop.payment_attempted -n 5          # first 5 records
asm-peek shop.payment_attempted -n 5 --tail   # last 5 records of each partition
"""

import argparse
import json
import os
import uuid

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .simulator import iso


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="asm-peek", description=__doc__.split("\n")[0])
    parser.add_argument("topic")
    parser.add_argument("-n", type=int, default=5, help="number of records (default 5)")
    parser.add_argument("--tail", action="store_true", help="the last n of each partition")
    args = parser.parse_args(argv)

    registry = SchemaRegistryClient(
        {"url": os.environ.get("SCHEMA_REGISTRY_URL") or "http://localhost:8085"}
    )
    # No reader schema given: each record is decoded with the schema it was written with.
    deserialize = AvroDeserializer(registry)
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP") or "localhost:9092",
            "group.id": f"asm-peek-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    def from_tail(c: Consumer, partitions: list[TopicPartition]) -> None:
        for p in partitions:
            low, high = c.get_watermark_offsets(p)
            p.offset = max(low, high - args.n)
        c.assign(partitions)

    consumer.subscribe([args.topic], on_assign=from_tail if args.tail else lambda c, p: None)
    wanted = args.n * (len(consumer.list_topics(args.topic).topics[args.topic].partitions))
    wanted = wanted if args.tail else args.n
    seen = 0
    try:
        while seen < wanted and (msg := consumer.poll(5.0)) is not None:
            if msg.error():
                raise RuntimeError(msg.error())
            topic = msg.topic()
            record = {
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": iso(msg.timestamp()[1]),
                "key": deserialize(msg.key(), SerializationContext(topic, MessageField.KEY)),
                "value": deserialize(msg.value(), SerializationContext(topic, MessageField.VALUE)),
            }
            print(json.dumps(record, default=str))
            seen += 1
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
