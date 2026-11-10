"""Exact counts of what the generator emitted, written as JSONL (ADR 0021).

Windows are tumbling and aligned to the epoch, like Flink's TUMBLE, so the step 4 jobs can be
checked row by row. A window not fully inside the run is marked `partial`. Every window in the
run gets a row, including those with zero counts.
"""

import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .events import EVENT_TYPES, Event
from .simulator import MINUTE, PROVIDERS, iso

ORDERS_WINDOW_MS = MINUTE
FUNNEL_WINDOW_MS = 5 * MINUTE


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


class GroundTruth:
    def __init__(self) -> None:
        self.totals: Counter[str] = Counter()
        self.orders: Counter[int] = Counter()  # window start -> orders placed
        self.funnel: defaultdict[int, Counter[str]] = defaultdict(Counter)
        self.payments: defaultdict[tuple[int, str], Counter[str]] = defaultdict(Counter)

    def observe(self, event: Event) -> None:
        self.totals[event.type] += 1
        v = event.value
        w5 = event.time_ms - event.time_ms % FUNNEL_WINDOW_MS
        if event.type == "checkout_started":
            self.funnel[w5]["checkouts_started"] += 1
        elif event.type == "order_placed":
            self.orders[event.time_ms - event.time_ms % ORDERS_WINDOW_MS] += 1
            self.funnel[w5]["orders_placed"] += 1
        elif event.type == "payment_attempted":
            p = self.payments[(w5, v["provider"])]
            failed = v["status"] == "failed"
            retry = v["attempt"] > 1
            p["attempts"] += 1
            p["failures"] += failed
            p["retries"] += retry
            p["retries_succeeded"] += retry and not failed
            if event.abandoned:
                self.funnel[w5]["abandoned_after_payment_failure"] += 1

    def records(self, start_ms: int, end_ms: int) -> Iterable[dict[str, Any]]:
        yield {"record": "totals", **{t: self.totals[t] for t in EVENT_TYPES}}
        for w in _windows(start_ms, end_ms, ORDERS_WINDOW_MS):
            yield {
                "record": "orders_1m",
                **_window(w, ORDERS_WINDOW_MS, start_ms, end_ms),
                "orders": self.orders[w],
            }
        for w in _windows(start_ms, end_ms, FUNNEL_WINDOW_MS):
            f = self.funnel[w]
            yield {
                "record": "checkout_funnel_5m",
                **_window(w, FUNNEL_WINDOW_MS, start_ms, end_ms),
                "checkouts_started": f["checkouts_started"],
                "orders_placed": f["orders_placed"],
                "conversion": _rate(f["orders_placed"], f["checkouts_started"]),
                "abandoned_after_payment_failure": f["abandoned_after_payment_failure"],
            }
            for provider in PROVIDERS:
                p = self.payments[(w, provider)]
                yield {
                    "record": "payments_by_provider_5m",
                    **_window(w, FUNNEL_WINDOW_MS, start_ms, end_ms),
                    "provider": provider,
                    "attempts": p["attempts"],
                    "failures": p["failures"],
                    "failure_rate": _rate(p["failures"], p["attempts"]),
                    "retries": p["retries"],
                    "retries_succeeded": p["retries_succeeded"],
                }


def _windows(start_ms: int, end_ms: int, size_ms: int) -> range:
    return range(start_ms - start_ms % size_ms, end_ms, size_ms)


def _window(w: int, size_ms: int, start_ms: int, end_ms: int) -> dict[str, Any]:
    return {
        "window_start": iso(w),
        "window_end": iso(w + size_ms),
        "partial": w < start_ms or w + size_ms > end_ms,
    }


def write(path: Path, run: dict[str, Any], truth: GroundTruth, start_ms: int, end_ms: int) -> None:
    """Write the run's parameters, then every count, one JSON object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        f.write(json.dumps({"record": "run", **run}) + "\n")
        for record in truth.records(start_ms, end_ms):
            f.write(json.dumps(record) + "\n")
