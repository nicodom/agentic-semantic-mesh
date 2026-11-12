"""Event types, their topics and their Avro schemas (ADR 0020)."""

import json
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import Any


@dataclass(frozen=True)
class EventType:
    name: str
    topic: str
    key_schema: str  # file name (without .avsc) of the key schema
    key_field: str  # value field copied into the key record

    @property
    def value_schema(self) -> str:
        return self.name


EVENT_TYPES: dict[str, EventType] = {
    t.name: t
    for t in (
        EventType("cart_updated", "shop.cart_updated", "session_key", "session_id"),
        EventType("checkout_started", "shop.checkout_started", "session_key", "session_id"),
        EventType("payment_attempted", "shop.payment_attempted", "session_key", "session_id"),
        EventType("order_placed", "shop.order_placed", "order_key", "order_id"),
        EventType("inventory_changed", "shop.inventory_changed", "sku_key", "sku"),
        EventType("return_requested", "shop.return_requested", "order_key", "order_id"),
    )
}


@dataclass(frozen=True)
class Event:
    """One generated event. `value` matches the Avro value schema of `type`."""

    type: str
    time_ms: int  # event time, epoch milliseconds; also `value["event_time"]`
    value: dict[str, Any]
    # Ground truth only, never serialized: the customer gave up after this failed payment.
    abandoned: bool = False

    @property
    def topic(self) -> str:
        return EVENT_TYPES[self.type].topic

    @property
    def key(self) -> dict[str, Any]:
        field = EVENT_TYPES[self.type].key_field
        return {field: self.value[field]}


@cache
def schema_str(name: str) -> str:
    """The Avro schema `name` (e.g. "session_key", "order_placed") as JSON text."""
    return files(__package__).joinpath("schemas", f"{name}.avsc").read_text()


def schema(name: str) -> dict[str, Any]:
    return json.loads(schema_str(name))
