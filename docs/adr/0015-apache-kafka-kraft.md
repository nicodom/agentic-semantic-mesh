# 15. Apache Kafka in KRaft mode as the event log

Status: Accepted (2026-11-09)

## Context

Every event in the system flows through a durable, replayable log (README, target architecture). The M1 plan names Kafka and asks for an ADR if an alternative is chosen. A protocol-compatible alternative exists, so the choice is recorded either way.

## Options considered

- Apache Kafka 4.x, single broker in KRaft mode (Kafka's own Raft-based metadata quorum; ZooKeeper was removed in 4.0)
- Redpanda: Kafka-API-compatible, written in C++, a single binary that starts faster and uses less memory
- Confluent Platform images: Kafka plus Confluent's extras, under the Confluent Community License

## Decision

Apache Kafka 4.4.0 (`apache/kafka` image), one node acting as both broker and controller.

## Consequences

+ The reference implementation, which readers know and which a Confluent Cloud profile (scope, *Later*) speaks natively
+ One container; no ZooKeeper
- Heavier and slower to start than Redpanda (JVM)
- A single node cannot replicate, so internal topics run with replication factor 1. Fine locally, never in production
