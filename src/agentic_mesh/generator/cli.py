"""`asm-generate`: run the generator in backfill or live mode (ADR 0021).

    asm-generate backfill --hours 2 --seed 42 --scenario payment-outage
    asm-generate live --scenario payment-outage --outage-at 5

Each run writes its ground truth to data/local/ground-truth/.
"""

import argparse
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from . import ground_truth
from .ground_truth import GroundTruth
from .kafka_writer import KafkaWriter
from .simulator import MINUTE, PROVIDERS, Config, PaymentOutage, iso, simulate

TRUTH_DIR = Path("data/local/ground-truth")
# Kafka deletes records by their timestamp after 7 days by default; stay well inside that.
MAX_BACKFILL_HOURS = 6 * 24


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="asm-generate", description=__doc__.split("\n")[0])
    modes = parser.add_subparsers(dest="mode", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--seed", type=int, default=42)
    common.add_argument("--sessions-per-minute", type=float, default=Config.sessions_per_minute)
    common.add_argument("--scenario", choices=["payment-outage"], action="append", default=[])
    common.add_argument("--outage-provider", choices=PROVIDERS, default="adyen")
    common.add_argument(
        "--outage-at",
        type=float,
        help="minutes after the start (default: halfway for backfill, 5 for live)",
    )
    common.add_argument("--outage-minutes", type=float, default=20)
    common.add_argument("--dry-run", action="store_true", help="don't send to Kafka")
    common.add_argument("--topic-prefix", default="", help=argparse.SUPPRESS)  # for tests
    common.add_argument("--truth-dir", type=Path, default=TRUTH_DIR)

    backfill = modes.add_parser("backfill", parents=[common], help="generate a past time range")
    backfill.add_argument("--hours", type=float, default=2)
    backfill.add_argument(
        "--end",
        type=datetime.fromisoformat,
        help="end of the range, ISO 8601 (default: now, rounded down to the minute)",
    )
    modes.add_parser("live", parents=[common], help="generate in real time until Ctrl-C")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = Config(sessions_per_minute=args.sessions_per_minute)

    if args.mode == "backfill":
        if args.hours > MAX_BACKFILL_HOURS:
            sys.exit(f"--hours must be at most {MAX_BACKFILL_HOURS} (Kafka retention is 7 days)")
        if args.end:
            end_ms = int(args.end.replace(tzinfo=args.end.tzinfo or UTC).timestamp() * 1000)
        else:
            end_ms = _now_ms() - _now_ms() % MINUTE
        start_ms = end_ms - round(args.hours * 60 * MINUTE)
        if start_ms < _now_ms() - MAX_BACKFILL_HOURS * 60 * MINUTE:
            sys.exit("the range starts too far back for Kafka retention (7 days)")
        outage_at = args.outage_at if args.outage_at is not None else args.hours * 30
    else:
        start_ms, end_ms = _now_ms(), None
        outage_at = args.outage_at if args.outage_at is not None else 5

    outage_start = start_ms + round(outage_at * MINUTE)
    scenarios = tuple(
        PaymentOutage(
            args.outage_provider, outage_start, outage_start + round(args.outage_minutes * MINUTE)
        )
        for _ in args.scenario
    )
    writer = None
    if not args.dry_run:
        writer = KafkaWriter(
            os.environ.get("KAFKA_BOOTSTRAP") or "localhost:9092",
            os.environ.get("SCHEMA_REGISTRY_URL") or "http://localhost:8085",
            args.topic_prefix,
        )

    for s in scenarios:
        print(f"scenario {s.name}: {s.provider} from {iso(s.start_ms)} to {iso(s.end_ms)}")
    truth = GroundTruth()
    events = simulate(args.seed, start_ms, end_ms, config, scenarios)
    try:
        if args.mode == "backfill":
            print(f"backfill {iso(start_ms)} .. {iso(end_ms)}, seed {args.seed}")
            for event in events:
                truth.observe(event)
                if writer:
                    writer.write(event)
        else:
            print(f"live from {iso(start_ms)}, seed {args.seed}; Ctrl-C to stop")
            _run_live(events, truth, writer)
    except KeyboardInterrupt:
        pass
    finally:
        if writer:
            writer.flush()
    end_ms = end_ms if end_ms is not None else _now_ms()

    stamp = datetime.fromtimestamp(start_ms / 1000, UTC).strftime("%Y%m%dT%H%M%SZ")
    path = args.truth_dir / f"{args.mode}-{stamp}-seed{args.seed}.jsonl"
    run = {
        "mode": args.mode,
        "seed": args.seed,
        "start": iso(start_ms),
        "end": iso(end_ms),
        "topic_prefix": args.topic_prefix,
        "dry_run": args.dry_run,
        "config": config.describe(),
        "scenarios": [s.describe() for s in scenarios],
    }
    ground_truth.write(path, run, truth, start_ms, end_ms)
    print(", ".join(f"{t}={n}" for t, n in sorted(truth.totals.items())))
    print(f"ground truth: {path}")
    return 0


def _run_live(events, truth: GroundTruth, writer: KafkaWriter | None) -> None:
    next_report = _now_ms() + MINUTE
    for event in events:
        # Wait until the event's time arrives; polling the producer meanwhile serves deliveries.
        while (wait_ms := event.time_ms - _now_ms()) > 0:
            if writer:
                writer.producer.poll(min(wait_ms, 1000) / 1000)
            else:
                time.sleep(min(wait_ms, 1000) / 1000)
        truth.observe(event)
        if writer:
            writer.write(event)
        if event.time_ms >= next_report:
            print(
                iso(event.time_ms), ", ".join(f"{t}={n}" for t, n in sorted(truth.totals.items()))
            )
            next_report += MINUTE


if __name__ == "__main__":
    raise SystemExit(main())
