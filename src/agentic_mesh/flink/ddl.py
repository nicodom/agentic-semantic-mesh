"""Flink SQL table definitions generated from the `.avsc` files (ADRs 0020, 0023).

The Avro schemas are the only description of each topic. Every `CREATE TABLE` here is derived
from them, and the schema itself is handed to Flink's `avro-confluent` format, so the SQL and the
registered contract cannot drift apart.
"""

import json
from dataclasses import dataclass
from typing import Any

# Event-time watermarks trail the newest event by this much. The generator writes each partition
# in event-time order, so this only absorbs small skews between producers in live mode.
WATERMARK_DELAY = "INTERVAL '5' SECOND"

# Kafka metadata copied next to the value's fields in the lake (ADR 0023).
KAFKA_METADATA = (
    ("kafka_partition", "INT", "partition"),
    ("kafka_offset", "BIGINT", "offset"),
    ("kafka_timestamp", "TIMESTAMP_LTZ(3)", "timestamp"),
)


@dataclass(frozen=True)
class Stack:
    """Where the jobs run: container hostnames, as the SQL client sees them."""

    bootstrap: str = "kafka:19092"
    registry_url: str = "http://schema-registry:8081"
    topic_prefix: str = ""  # throwaway topics in tests
    bounded: bool = False  # read up to the current end of each topic, then finish

    def database(self, base: str) -> str:
        """Iceberg namespace for `base` ("shop", "metrics"), kept apart for prefixed test runs."""
        if not self.topic_prefix:
            return base
        return self.topic_prefix.strip(".").replace("-", "_") + "_" + base


def sql_type(avro: Any) -> str:
    """The Flink SQL type of an Avro field type, with NOT NULL unless it is a union with null."""
    if isinstance(avro, list):
        [inner] = [t for t in avro if t != "null"]
        return sql_type(inner).removesuffix(" NOT NULL")
    return f"{_base_type(avro)} NOT NULL"


def _base_type(avro: Any) -> str:
    if isinstance(avro, dict):
        logical = avro.get("logicalType")
        if logical == "timestamp-millis":
            # Not TIMESTAMP_LTZ: avro-confluent maps timestamp-millis to TIMESTAMP(3), a UTC
            # wall-clock time. Jobs set the session time zone to UTC to match.
            return "TIMESTAMP(3)"
        if logical == "decimal":
            return f"DECIMAL({avro['precision']}, {avro['scale']})"
        if avro["type"] == "enum":
            return "STRING"
        return _base_type(avro["type"])
    return {"string": "STRING", "int": "INT", "long": "BIGINT", "double": "DOUBLE"}[avro]


def columns(schema: dict[str, Any]) -> list[str]:
    return [f"`{f['name']}` {sql_type(f['type'])}" for f in schema["fields"]]


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _options(options: dict[str, str]) -> str:
    return ",\n".join(f"  {_literal(k)} = {_literal(v)}" for k, v in options.items())


def _compact(schema: dict[str, Any]) -> str:
    return json.dumps(schema, separators=(",", ":"))


def source_table(name: str, topic: str, value_schema: dict[str, Any], stack: Stack) -> str:
    """A Kafka topic read with its registered value schema, plus Kafka metadata columns.

    Event topics get event-time watermarks on `event_time`. The key is not read: its fields are
    also in the value.
    """
    body = [
        *columns(value_schema),
        *(f"`{c}` {t} METADATA FROM '{m}' VIRTUAL" for c, t, m in KAFKA_METADATA),
    ]
    if any(f["name"] == "event_time" for f in value_schema["fields"]):
        body.append(f"WATERMARK FOR `event_time` AS `event_time` - {WATERMARK_DELAY}")
    options = {
        "connector": "kafka",
        "topic": stack.topic_prefix + topic,
        "properties.bootstrap.servers": stack.bootstrap,
        "scan.startup.mode": "earliest-offset",
        **({"scan.bounded.mode": "latest-offset"} if stack.bounded else {}),
        "value.format": "avro-confluent",
        "value.avro-confluent.url": stack.registry_url,
        "value.avro-confluent.schema": _compact(value_schema),
    }
    return _create(name, body, options)


def metric_sink_table(
    name: str,
    topic: str,
    key_schema: dict[str, Any],
    value_schema: dict[str, Any],
    stack: Stack,
) -> str:
    """A metric topic written with the repo's key and value schemas; the key fields are in both."""
    key_fields = [f["name"] for f in key_schema["fields"]]
    options = {
        "connector": "kafka",
        "topic": stack.topic_prefix + topic,
        "properties.bootstrap.servers": stack.bootstrap,
        "sink.delivery-guarantee": "at-least-once",
        "key.format": "avro-confluent",
        "key.avro-confluent.url": stack.registry_url,
        "key.avro-confluent.schema": _compact(key_schema),
        "key.fields": ";".join(key_fields),
        "value.format": "avro-confluent",
        "value.avro-confluent.url": stack.registry_url,
        "value.avro-confluent.schema": _compact(value_schema),
        "value.fields-include": "ALL",
    }
    return _create(name, columns(value_schema), options)


def lake_table(database: str, name: str, value_schema: dict[str, Any]) -> str:
    """An unpartitioned Iceberg table: the value's fields plus Kafka metadata (ADR 0023)."""
    body = [*columns(value_schema), *(f"`{c}` {t}" for c, t, _ in KAFKA_METADATA)]
    cols = ",\n".join(f"  {line}" for line in body)
    return f"CREATE TABLE IF NOT EXISTS lake.`{database}`.`{name}` (\n{cols}\n);"


def _create(name: str, body: list[str], options: dict[str, str]) -> str:
    cols = ",\n".join(f"  {line}" for line in body)
    return f"CREATE TEMPORARY TABLE `{name}` (\n{cols}\n) WITH (\n{_options(options)}\n);"
