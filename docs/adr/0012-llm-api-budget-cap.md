# 12. A hard monthly cap on paid LLM API spend

Status: Accepted (2026-10-27)

## Context

Three providers, eval suites and agent loops can generate unbounded API cost; a runaway loop is the main risk.

## Options considered

- EUR 20/month
- EUR 50/month
- EUR 100/month

## Decision

A hard cap of EUR 50/month across Claude and OpenAI, set in both provider consoles. Development defaults to the local model; small models for evals; large models for demos and comparisons. Cost per run is tracked in Langfuse. Expected spend is EUR 10-30/month.

## Consequences

+ A runaway loop cannot become an expensive surprise
- A hit cap stops paid runs until the next month or a deliberate change (new ADR)
