# 9. OpenTelemetry and Langfuse for AI observability

Status: Accepted (2026-10-27)

## Context

Every agent step needs traces, token counts, cost and latency, tied to eval scores, to compare providers and debug behaviour.

## Options considered

- OpenTelemetry + Langfuse
- OpenTelemetry + Grafana stack
- Langfuse SDK only

## Decision

Instrument with OpenTelemetry using the GenAI semantic conventions, and view traces, cost and eval scores in self-hosted Langfuse in Docker Compose.

## Consequences

+ Vendor-neutral instrumentation; the backend can change
+ An LLM-specific UI for traces and evals
- Langfuse brings its own dependencies (Postgres, ClickHouse, Redis, object storage)
