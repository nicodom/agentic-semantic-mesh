# 21. A custom Python event generator with event time, and live and backfill modes

Status: Accepted (2026-11-09)

## Context

ADR 0004 chose synthetic e-commerce events so that every eval case has a known answer. The evals (ADR 0011) and the agent's questions depend on four properties of the generated data:

- **Causality across topics:** a session moves through cart, checkout, payment and order; no order exists without a successful payment. Without this, "conversion dropped because provider X failed" is noise, not an explainable fact.
- **Scheduled scenarios:** e.g. a payment-provider outage between T1 and T2.
- **Ground truth:** what was injected, and the exact resulting numbers, so answers and Flink's results can be checked.
- **Repeatability and speed:** the same inputs give the same events and the same expected answers, and an eval run doesn't take as long as the scenario it replays.

## Options considered

- Confluent's Datagen source connector (Kafka Connect, Apache-2.0): schema-driven random records, no code, with a managed version in Confluent Cloud. But each connector writes independent records to one topic, so there is no cross-topic causality. It has no random seed, and no way to schedule a scenario except reconfiguring connectors through the Connect REST API at run time. It records no ground truth, and it needs Kafka Connect, which ADR 0018 chose not to run
- ShadowTraffic: built for realistic streams, with correlated topics and time-based stages, but commercial and not open source, and it needs a licence key (conflicts with ADR 0003)
- Flink's `datagen` connector: random rows inside Flink; same limits as Datagen
- A custom Python generator

## Decision

A custom Python generator (`src/agentic_mesh/generator/`) that simulates shopper sessions through the funnel and writes Avro events (ADR 0020) to one topic per event type.

- **Event time:** every event carries `event_time`, which is also set as the Kafka record timestamp. Flink windows by event time with watermarks, not by arrival time.
- **Live mode:** real-time traffic for demos; scenarios happen while you watch.
- **Backfill mode:** generates a time range (e.g. 2 hours ending now) in seconds, with past timestamps. It takes a fixed random seed and a scenario schedule, so the same arguments give the same events. Evals use this mode.
- **Ground truth:** each run writes a JSONL log under `data/local/` (gitignored). It records the run's parameters and seed, each injected scenario with its times, and exact counts the generator computed itself: orders per minute, checkout conversion per 5-minute window, payment failure rate per provider.
- **Pure core:** the simulation is a pure function of seed, schedule and time range, with no Kafka inside, so it is unit-testable; a thin writer sends its output to Kafka.

## Consequences

+ Every eval answer is checkable against exact numbers, and runs are fast and repeatable
+ The generator's own counts double as a correctness check for the Flink jobs in step 4
+ No extra service; a CLI on the host
- Code to write and maintain, unlike a configured connector
- Backfilled timestamps must stay within topic retention (Kafka deletes by record timestamp; the default is 7 days), so backfills cover hours, not weeks
- Synthetic data can be too clean (ADR 0004); a real public stream is still planned as a second source
