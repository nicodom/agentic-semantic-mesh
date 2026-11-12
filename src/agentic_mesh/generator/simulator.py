"""The pure simulation core: seed, time range, config and scenarios in; time-ordered events out.

Sessions arrive as a Poisson process. Each session's funnel (cart, checkout, payment attempts,
order or abandonment, maybe a return) is decided when it arrives, using an RNG of its own, so a
session's events depend only on the seed and its index. Overlapping sessions interleave, so events
wait in a heap ordered by event time. Inventory is the one state shared across sessions: sessions
emit pending stock changes, applied in time order as they leave the heap.
"""

import heapq
import itertools
import random
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any

from .events import Event

PROVIDERS = ("stripe", "adyen", "paypal")

CENT = Decimal("0.01")  # money has scale 2, as in the Avro schemas
SECOND = 1_000
MINUTE = 60 * SECOND
# Sessions start arriving this long before `start_ms`, so traffic is in steady state from the
# first emitted minute instead of ramping up from an empty shop. Longer than any checkout funnel.
WARMUP_MS = 15 * MINUTE


@dataclass(frozen=True)
class Config:
    sessions_per_minute: float = 30.0
    checkout_rate: float = 0.4  # share of sessions with a non-empty cart that start checkout
    payment_rate: float = 0.85  # share of checkouts that reach a payment attempt
    provider_weights: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({"stripe": 0.5, "adyen": 0.3, "paypal": 0.2})
    )
    failure_rates: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({"stripe": 0.03, "adyen": 0.04, "paypal": 0.05})
    )
    retry_rate: float = 0.7  # chance a customer retries with another provider after a failure
    max_attempts: int = 2
    return_rate: float = 0.05  # share of orders with a return request
    customers: int = 5_000
    reorder_point: int = 40  # a restock is ordered when stock falls below this
    restock_quantity: int = 150
    restock_delay_ms: int = 10 * MINUTE

    def describe(self) -> dict[str, Any]:
        return {k: dict(v) if isinstance(v, Mapping) else v for k, v in self.__dict__.items()}


@dataclass(frozen=True)
class PaymentOutage:
    """A provider fails far more often than usual between `start_ms` and `end_ms`."""

    provider: str
    start_ms: int
    end_ms: int
    failure_rate: float = 0.7

    name = "payment-outage"

    def active(self, provider: str, time_ms: int) -> bool:
        return provider == self.provider and self.start_ms <= time_ms < self.end_ms

    def describe(self) -> dict[str, Any]:
        return {
            "scenario": self.name,
            "provider": self.provider,
            "start": iso(self.start_ms),
            "end": iso(self.end_ms),
            "failure_rate": self.failure_rate,
        }


# The product catalogue is fixed (independent of the seed), so prices are stable across runs.
_catalog_rng = random.Random("catalog")
CATALOG: dict[str, Decimal] = {
    f"SKU-{i:03d}": (Decimal(_catalog_rng.randrange(499, 19_999)) / 100).quantize(CENT)
    for i in range(1, 41)
}
INITIAL_STOCK: dict[str, int] = {sku: _catalog_rng.randrange(30, 121) for sku in CATALOG}
SKUS = tuple(CATALOG)

RETURN_REASONS = ("damaged", "wrong_item", "not_as_described", "changed_mind")


def iso(time_ms: int) -> str:
    return datetime.fromtimestamp(time_ms / 1000, UTC).isoformat(timespec="milliseconds")


def _ts(time_ms: int) -> datetime:
    return datetime.fromtimestamp(time_ms / 1000, UTC)


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _weighted(rng: random.Random, weights: Mapping[str, float]) -> str:
    names = sorted(weights)
    return rng.choices(names, weights=[weights[n] for n in names])[0]


@dataclass(frozen=True)
class _StockChange:
    """A stock change whose `stock_after` is known only once earlier changes are applied."""

    time_ms: int
    event_id: str
    sku: str
    delta: int
    reason: str  # order | return | restock


def simulate(
    seed: int,
    start_ms: int,
    end_ms: int | None = None,
    config: Config | None = None,
    scenarios: tuple[PaymentOutage, ...] = (),
) -> Iterator[Event]:
    """Yield events with `start_ms <= time_ms < end_ms`, in event-time order.

    With `end_ms=None` the stream never ends (live mode); the caller decides when to stop.
    Events before `start_ms` (the warm-up) are simulated but not emitted.
    """
    cfg = config or Config()
    arrivals = random.Random(f"{seed}:arrivals")
    rate_per_ms = cfg.sessions_per_minute / MINUTE
    heap: list[tuple[int, int, Event | _StockChange]] = []
    seq = itertools.count()  # tie-breaker: equal times leave the heap in the order they entered
    stock = dict(INITIAL_STOCK)
    restock_ordered: set[str] = set()

    def push(item: Event | _StockChange) -> None:
        heapq.heappush(heap, (item.time_ms, next(seq), item))

    def apply(change: _StockChange) -> Event:
        stock[change.sku] += change.delta
        if change.reason == "restock":
            restock_ordered.discard(change.sku)
        elif stock[change.sku] < cfg.reorder_point and change.sku not in restock_ordered:
            restock_ordered.add(change.sku)
            restock_id = uuid.uuid5(uuid.NAMESPACE_OID, f"{seed}:{change.sku}:{change.time_ms}")
            push(
                _StockChange(
                    change.time_ms + cfg.restock_delay_ms,
                    str(restock_id),
                    change.sku,
                    cfg.restock_quantity,
                    "restock",
                )
            )
        return Event(
            "inventory_changed",
            change.time_ms,
            {
                "event_id": change.event_id,
                "event_time": _ts(change.time_ms),
                "sku": change.sku,
                "delta": change.delta,
                "stock_after": stock[change.sku],
                "reason": change.reason,
            },
        )

    index = 0
    next_arrival = start_ms - WARMUP_MS + round(arrivals.expovariate(rate_per_ms))
    arrivals_open = True
    while True:
        # A session's events never precede its arrival, so once the next arrival is later than
        # the earliest pending item, that item is safe to emit.
        while arrivals_open and (not heap or next_arrival <= heap[0][0]):
            if end_ms is not None and next_arrival >= end_ms:
                arrivals_open = False
                break
            for item in _session(seed, index, next_arrival, cfg, scenarios):
                push(item)
            index += 1
            next_arrival += max(1, round(arrivals.expovariate(rate_per_ms)))
        if not heap:
            return
        time_ms, _, item = heapq.heappop(heap)
        if end_ms is not None and time_ms >= end_ms:
            return  # everything still pending is later
        if isinstance(item, _StockChange):
            item = apply(item)  # also during warm-up, so stock levels carry over
        if time_ms >= start_ms:
            yield item


def _session(
    seed: int,
    index: int,
    start_ms: int,
    cfg: Config,
    scenarios: tuple[PaymentOutage, ...],
) -> list[Event | _StockChange]:
    """One shopping session, from its first cart change to an order or abandonment."""
    rng = random.Random(f"{seed}:session:{index}")
    session_id = _uuid(rng)
    customer_id = f"C-{rng.randrange(cfg.customers):05d}"
    out: list[Event | _StockChange] = []
    t = start_ms

    # Cart: a few changes, mostly additions.
    cart: dict[str, int] = {}
    for i in range(rng.randint(1, 5)):
        if i:
            t += rng.randint(5 * SECOND, 60 * SECOND)
        if cart and rng.random() < 0.15:
            sku, delta = rng.choice(sorted(cart)), -1
        else:
            sku, delta = rng.choice(SKUS), rng.choice((1, 1, 1, 2, 3))
        cart[sku] = cart.get(sku, 0) + delta
        if cart[sku] == 0:
            del cart[sku]
        out.append(
            Event(
                "cart_updated",
                t,
                {
                    "event_id": _uuid(rng),
                    "event_time": _ts(t),
                    "session_id": session_id,
                    "customer_id": customer_id,
                    "sku": sku,
                    "quantity_delta": delta,
                    "unit_price": CATALOG[sku],
                },
            )
        )
    if not cart or rng.random() >= cfg.checkout_rate:
        return out

    # Checkout creates the order as pending.
    t += rng.randint(10 * SECOND, 90 * SECOND)
    order_id = _uuid(rng)
    total = sum((CATALOG[sku] * qty for sku, qty in cart.items()), Decimal(0)).quantize(CENT)
    item_count = sum(cart.values())
    out.append(
        Event(
            "checkout_started",
            t,
            {
                "event_id": _uuid(rng),
                "event_time": _ts(t),
                "session_id": session_id,
                "order_id": order_id,
                "customer_id": customer_id,
                "cart_value": total,
                "item_count": item_count,
            },
        )
    )

    if rng.random() >= cfg.payment_rate:
        return out  # left at the checkout page

    # Payment: after a failure the customer may retry with a provider not tried yet.
    tried: list[str] = []
    provider = _weighted(rng, cfg.provider_weights)
    for attempt in itertools.count(1):
        t += rng.randint(3 * SECOND, 20 * SECOND) if attempt == 1 else rng.randint(15, 60) * SECOND
        tried.append(provider)
        outage = any(s.active(provider, t) for s in scenarios)
        failure_rate = max(
            [cfg.failure_rates[provider]]
            + [s.failure_rate for s in scenarios if s.active(provider, t)]
        )
        failed = rng.random() < failure_rate
        reason = None
        if failed:
            # An outage fails on the provider's side; normal failures are mostly card declines.
            reason = (
                rng.choices(("timeout", "provider_error"), weights=(0.4, 0.6))[0]
                if outage
                else rng.choices(
                    ("declined", "timeout", "provider_error"), weights=(0.7, 0.2, 0.1)
                )[0]
            )
        untried = {p: w for p, w in cfg.provider_weights.items() if p not in tried}
        retry = (
            failed
            and attempt < cfg.max_attempts
            and bool(untried)
            and rng.random() < cfg.retry_rate
        )
        out.append(
            Event(
                "payment_attempted",
                t,
                {
                    "event_id": _uuid(rng),
                    "event_time": _ts(t),
                    "session_id": session_id,
                    "order_id": order_id,
                    "attempt": attempt,
                    "provider": provider,
                    "amount": total,
                    "status": "failed" if failed else "succeeded",
                    "failure_reason": reason,
                },
                abandoned=failed and not retry,
            )
        )
        if not failed:
            break
        if not retry:
            return out
        provider = _weighted(rng, untried)

    # Order placed; stock goes down for each line.
    t += rng.randint(500, 3 * SECOND)
    out.append(
        Event(
            "order_placed",
            t,
            {
                "event_id": _uuid(rng),
                "event_time": _ts(t),
                "order_id": order_id,
                "session_id": session_id,
                "customer_id": customer_id,
                "total": total,
                "item_count": item_count,
                "provider": provider,
                "payment_attempts": attempt,
            },
        )
    )
    for sku, qty in cart.items():
        out.append(_StockChange(t, _uuid(rng), sku, -qty, "order"))

    # Sometimes a return request later on; the goods come back to stock after that.
    if rng.random() < cfg.return_rate:
        sku = rng.choice(sorted(cart))
        quantity = rng.randint(1, cart[sku])
        t += rng.randint(10, 90) * MINUTE
        out.append(
            Event(
                "return_requested",
                t,
                {
                    "event_id": _uuid(rng),
                    "event_time": _ts(t),
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "sku": sku,
                    "quantity": quantity,
                    "reason": rng.choice(RETURN_REASONS),
                },
            )
        )
        t += rng.randint(30, 120) * MINUTE
        out.append(_StockChange(t, _uuid(rng), sku, quantity, "return"))
    return out
