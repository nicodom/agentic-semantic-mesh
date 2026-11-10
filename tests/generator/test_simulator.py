from collections import defaultdict
from itertools import islice

import fastavro
import pytest

from agentic_mesh.generator.events import EVENT_TYPES, Event, schema
from agentic_mesh.generator.simulator import MINUTE, Config, PaymentOutage, simulate

START = 1_800_000_000_000 - 1_800_000_000_000 % MINUTE
END = START + 120 * MINUTE
OUTAGE = PaymentOutage("adyen", START + 60 * MINUTE, START + 80 * MINUTE)


@pytest.fixture(scope="module")
def events() -> list[Event]:
    return list(simulate(42, START, END, scenarios=(OUTAGE,)))


def by(events: list[Event], type_: str) -> list[Event]:
    return [e for e in events if e.type == type_]


def test_same_seed_same_events(events: list[Event]) -> None:
    assert list(simulate(42, START, END, scenarios=(OUTAGE,))) == events


def test_different_seed_different_events(events: list[Event]) -> None:
    assert list(islice(simulate(43, START, END), 50)) != events[:50]


def test_events_in_range_and_time_order(events: list[Event]) -> None:
    times = [e.time_ms for e in events]
    assert times == sorted(times)
    assert times[0] >= START and times[-1] < END
    assert all(e.value["event_time"].timestamp() * 1000 == e.time_ms for e in events)


def test_every_event_type_is_generated(events: list[Event]) -> None:
    assert {e.type for e in events} == set(EVENT_TYPES)


def test_events_match_their_avro_schemas(events: list[Event]) -> None:
    for name, spec in EVENT_TYPES.items():
        key_schema = fastavro.parse_schema(schema(spec.key_schema))
        value_schema = fastavro.parse_schema(schema(spec.value_schema))
        for e in by(events, name)[:200]:
            fastavro.validation.validate(e.key, key_schema)
            fastavro.validation.validate(e.value, value_schema)


def test_live_mode_is_a_prefix_of_backfill(events: list[Event]) -> None:
    # Live mode has no end; its first events equal a backfill's from the same start and seed.
    live = list(islice(simulate(42, START, None, scenarios=(OUTAGE,)), 1000))
    assert live == events[:1000]


def test_event_ids_are_unique(events: list[Event]) -> None:
    ids = [e.value["event_id"] for e in events]
    assert len(ids) == len(set(ids))


def test_funnel_causality(events: list[Event]) -> None:
    carts = {e.value["session_id"]: e.time_ms for e in reversed(by(events, "cart_updated"))}
    checkouts = {e.value["order_id"]: e for e in by(events, "checkout_started")}
    attempts: defaultdict[str, list[Event]] = defaultdict(list)
    for e in by(events, "payment_attempted"):
        attempts[e.value["order_id"]].append(e)

    for checkout in checkouts.values():
        assert carts[checkout.value["session_id"]] < checkout.time_ms
    for order_id, tries in attempts.items():
        assert order_id in checkouts
        assert tries[0].time_ms > checkouts[order_id].time_ms
    for order in by(events, "order_placed"):
        tries = attempts[order.value["order_id"]]
        assert tries[-1].value["status"] == "succeeded"
        assert tries[-1].time_ms < order.time_ms
        assert order.value["provider"] == tries[-1].value["provider"]
        assert order.value["payment_attempts"] == len(tries)
        assert order.value["total"] == checkouts[order.value["order_id"]].value["cart_value"]


def test_retry_rules(events: list[Event]) -> None:
    attempts: defaultdict[str, list[Event]] = defaultdict(list)
    for e in by(events, "payment_attempted"):
        attempts[e.value["order_id"]].append(e)
    placed = {e.value["order_id"] for e in by(events, "order_placed")}

    assert any(len(tries) > 1 for tries in attempts.values())
    for order_id, tries in attempts.items():
        values = [t.value for t in tries]
        assert [v["attempt"] for v in values] == list(range(1, len(tries) + 1))
        assert len(tries) <= Config().max_attempts
        assert len({v["provider"] for v in values}) == len(tries)  # never the same provider twice
        assert all(v["status"] == "failed" for v in values[:-1])
        assert all((v["failure_reason"] is None) == (v["status"] == "succeeded") for v in values)
        # Abandoned exactly when the last attempt failed (no order follows).
        assert tries[-1].abandoned == (order_id not in placed)
        assert not any(t.abandoned for t in tries[:-1])


def test_outage_raises_failures_for_its_provider_only(events: list[Event]) -> None:
    def failure_rate(provider: str, during: bool) -> float:
        tries = [
            e.value
            for e in by(events, "payment_attempted")
            if e.value["provider"] == provider
            and (OUTAGE.start_ms <= e.time_ms < OUTAGE.end_ms) == during
        ]
        return sum(v["status"] == "failed" for v in tries) / len(tries)

    assert failure_rate("adyen", during=True) > 0.5
    assert failure_rate("adyen", during=False) < 0.1
    assert failure_rate("stripe", during=True) < 0.1


def test_stock_never_negative_and_consistent(events: list[Event]) -> None:
    last: dict[str, int] = {}
    for e in by(events, "inventory_changed"):
        v = e.value
        if v["sku"] in last:
            assert v["stock_after"] == last[v["sku"]] + v["delta"]
        assert v["stock_after"] >= 0
        last[v["sku"]] = v["stock_after"]
    assert {e.value["reason"] for e in by(events, "inventory_changed")} == {
        "order",
        "restock",
        "return",
    }


def test_returns_refer_to_earlier_orders(events: list[Event]) -> None:
    orders = {e.value["order_id"]: e for e in by(events, "order_placed")}
    for r in by(events, "return_requested"):
        assert orders[r.value["order_id"]].time_ms < r.time_ms
