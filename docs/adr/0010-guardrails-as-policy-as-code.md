# 10. Guardrails as policy-as-code against excessive agency

Status: Accepted (2026-10-27)

## Context

OWASP LLM06 (excessive agency) is the main risk once agents can act. Guardrails must be enforced outside the model, and provably work.

## Options considered

- Policy-as-code at the tool layer, plus a human-approval gate
- An off-the-shelf guardrail library for input/output validation
- The approval gate only

## Decision

Per-agent tool allow-lists and data-product access policies enforced at the MCP layer, input and output checks, and the Temporal human-approval gate for any action with side effects. Every guardrail has eval cases proving it blocks what it should.

## Consequences

+ Enforcement does not depend on the model behaving
+ Each guardrail is tested, not assumed
- Policies need maintenance as tools and data products change
