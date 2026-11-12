"""Sends generated events to Kafka as Avro, with schemas from the registry (ADR 0020).

The `.avsc` files are the source of truth: they are registered explicitly when the writer starts,
and the serializers never register schemas themselves. Each record's Kafka timestamp is its event
time (ADR 0021), so Flink can window by event time and retention counts from it.
"""

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .events import EVENT_TYPES, Event, schema_str

PARTITIONS = 3


class KafkaWriter:
    def __init__(self, bootstrap: str, registry_url: str, topic_prefix: str = "") -> None:
        """`topic_prefix` isolates test runs from the real `shop.*` topics."""
        self.prefix = topic_prefix
        self.producer = Producer(
            {"bootstrap.servers": bootstrap, "linger.ms": 50, "enable.idempotence": True}
        )
        self.errors: list[KafkaError] = []
        self._create_topics(bootstrap)

        registry = SchemaRegistryClient({"url": registry_url})
        serializer_conf = {"auto.register.schemas": False}
        self.serializers: dict[str, tuple[AvroSerializer, AvroSerializer]] = {}
        for name, spec in EVENT_TYPES.items():
            topic = self.topic(name)
            # TopicNameStrategy: subjects are <topic>-key and <topic>-value.
            for field, schema_name in (("key", spec.key_schema), ("value", spec.value_schema)):
                registry.register_schema(
                    f"{topic}-{field}", Schema(schema_str(schema_name), "AVRO")
                )
            self.serializers[name] = (
                AvroSerializer(registry, schema_str(spec.key_schema), conf=serializer_conf),
                AvroSerializer(registry, schema_str(spec.value_schema), conf=serializer_conf),
            )

    def topic(self, event_type: str) -> str:
        return self.prefix + EVENT_TYPES[event_type].topic

    def _create_topics(self, bootstrap: str) -> None:
        # Append-only event topics: `delete` cleanup, never compaction, so no event overwrites
        # another with the same key.
        admin = AdminClient({"bootstrap.servers": bootstrap})
        new = [
            NewTopic(self.topic(name), PARTITIONS, 1, config={"cleanup.policy": "delete"})
            for name in EVENT_TYPES
        ]
        for future in admin.create_topics(new).values():
            try:
                future.result()
            except KafkaException as e:
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise

    def write(self, event: Event) -> None:
        topic = self.topic(event.type)
        key_serializer, value_serializer = self.serializers[event.type]
        key = key_serializer(event.key, SerializationContext(topic, MessageField.KEY))
        value = value_serializer(event.value, SerializationContext(topic, MessageField.VALUE))
        while True:
            try:
                self.producer.produce(
                    topic,
                    key=key,
                    value=value,
                    timestamp=event.time_ms,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.producer.poll(1)  # local queue full (large backfill): wait for deliveries
        self.producer.poll(0)

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self.errors.append(err)

    def flush(self, timeout: float = 30) -> None:
        """Wait for every event to be acknowledged; raise if any could not be delivered."""
        remaining = self.producer.flush(timeout)
        if remaining or self.errors:
            raise RuntimeError(f"{remaining} undelivered, errors: {self.errors[:5]}")
