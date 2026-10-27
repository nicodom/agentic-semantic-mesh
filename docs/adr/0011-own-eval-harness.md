# 11. A pytest eval harness with results in Langfuse

Status: Accepted (2026-10-27)

## Context

Agent quality must be measured on every change and per provider. The generator provides ground truth.

## Options considered

- Own pytest harness
- An eval framework (e.g. promptfoo, DeepEval)
- Langfuse datasets only

## Decision

Eval cases as data (question, expected answer derived from generator ground truth), run with pytest, scored by exact checks and an LLM judge where needed, results pushed to Langfuse. Runs per provider, and in CI against the local model.

## Consequences

+ Full understanding of how scoring works
+ Same cases measure quality and cost across providers
- More code than adopting a framework; LLM judges need their own calibration
