import json
from collections import Counter
from pathlib import Path

from agentic_mesh.generator import ground_truth
from agentic_mesh.generator.ground_truth import GroundTruth
from agentic_mesh.generator.simulator import MINUTE, PaymentOutage, iso, simulate

# Starts and ends mid-window, so partial windows are exercised.
START = 1_800_000_000_000 - 1_800_000_000_000 % (5 * MINUTE) + 2 * MINUTE
END = START + 61 * MINUTE
OUTAGE = PaymentOutage("adyen", START + 20 * MINUTE, START + 40 * MINUTE)


def run(tmp_path: Path) -> tuple[list, list[dict]]:
    events = list(simulate(7, START, END, scenarios=(OUTAGE,)))
    truth = GroundTruth()
    for e in events:
        truth.observe(e)
    path = tmp_path / "truth.jsonl"
    ground_truth.write(path, {"seed": 7}, truth, START, END)
    return events, [json.loads(line) for line in path.read_text().splitlines()]


def test_counts_match_an_independent_recount(tmp_path: Path) -> None:
    events, records = run(tmp_path)
    assert records[0] == {"record": "run", "seed": 7}
    totals = next(r for r in records if r["record"] == "totals")
    assert {k: v for k, v in totals.items() if k != "record"} == Counter(e.type for e in events)

    def window(e, size):
        return iso(e.time_ms - e.time_ms % size)

    orders = Counter(window(e, MINUTE) for e in events if e.type == "order_placed")
    for r in (r for r in records if r["record"] == "orders_1m"):
        assert r["orders"] == orders[r["window_start"]]

    attempts = [e for e in events if e.type == "payment_attempted"]
    for r in (r for r in records if r["record"] == "payments_by_provider_5m"):
        mine = [
            e.value
            for e in attempts
            if window(e, 5 * MINUTE) == r["window_start"] and e.value["provider"] == r["provider"]
        ]
        assert r["attempts"] == len(mine)
        assert r["failures"] == sum(v["status"] == "failed" for v in mine)
        assert r["retries"] == sum(v["attempt"] > 1 for v in mine)


def test_every_window_has_a_row_and_edges_are_partial(tmp_path: Path) -> None:
    _, records = run(tmp_path)
    minutes = [r for r in records if r["record"] == "orders_1m"]
    funnel = [r for r in records if r["record"] == "checkout_funnel_5m"]
    assert len(minutes) == 61
    assert len(funnel) == 13  # 61 minutes starting 2 minutes into a window
    assert funnel[0]["partial"] and funnel[-1]["partial"]
    assert not any(r["partial"] for r in funnel[1:-1])
    assert len([r for r in records if r["record"] == "payments_by_provider_5m"]) == 13 * 3


def test_outage_is_visible_in_the_funnel(tmp_path: Path) -> None:
    _, records = run(tmp_path)
    funnel = [r for r in records if r["record"] == "checkout_funnel_5m" and not r["partial"]]
    during = [r for r in funnel if iso(OUTAGE.start_ms) <= r["window_start"] < iso(OUTAGE.end_ms)]
    before = [r for r in funnel if r["window_start"] < iso(OUTAGE.start_ms)]
    assert sum(r["abandoned_after_payment_failure"] for r in during) > sum(
        r["abandoned_after_payment_failure"] for r in before
    )
