# 2. Python for agents and services, Flink SQL for stream processing

Status: Accepted (2026-10-27)

## Context

The agent side depends on LLM SDKs, MCP SDKs, evaluation and retrieval tooling, which are Python-first. The streaming side needs windowed aggregations and anomaly detection over Kafka.

## Options considered

- Python agent + Flink SQL
- Java/Kotlin throughout (familiar, but against the grain of the agent ecosystem)
- Python agent + a JVM Flink DataStream job (two codebases)

## Decision

Python for agents, services and the generator; Flink SQL for stream processing. No JVM codebase.

## Consequences

+ One codebase language; the agent ecosystem's native language
+ Flink SQL keeps the streaming logic declarative and reviewable
- Complex stateful logic may later outgrow Flink SQL; revisit with a new ADR if so
