# 19. Risk-tiered autonomy: policy decides which agent actions run automatically

Status: Accepted (2026-11-09). Supersedes [0010](0010-guardrails-as-policy-as-code.md)

## Context

ADR 0010 put every action with side effects behind the human-approval gate. That is safe, but it doesn't show bounded autonomy, which is the interesting part. A system where a human approves even "open an incident" makes the agent a recommender, and it trains people to approve without reading. Real operations let automation take low-risk, reversible steps on its own and escalate the rest. The excessive-agency risk (OWASP LLM06) remains the main concern, so the boundary must be enforced outside the model and proven by tests.

## Options considered

- Human approval for every action with side effects (ADR 0010)
- Risk tiers decided by policy: low-risk actions run automatically within limits, high-impact actions wait for approval
- Model-assessed risk: the agent rates its own action and decides whether to ask

## Decision

Everything in ADR 0010 still applies except its approval rule:
- per-agent tool allow-lists and data-product access policies, enforced at the MCP layer
- input and output checks
- eval cases for every guardrail

The approval rule changes as follows. Every action an agent proposes is classified by policy-as-code, outside the model, into one of these tiers:

| Tier | Example actions | Outcome |
|---|---|---|
| Read | Query metrics, read tables, inspect topics | Allowed within data-access policy |
| Low risk: reversible, small blast radius, no money moves | Open an incident, notify on-call, post a status note, annotate a dashboard | Runs automatically; reported afterwards |
| Bounded: reversible but customer-facing | Shift a share of payment traffic away from a failing provider | Runs automatically only within limits (e.g. ≤ 50% for ≤ 30 min); otherwise needs approval |
| High impact: irreversible, moves money, or affects many customers | Refunds, price changes, disabling a provider | Always waits for human approval |
| Forbidden | Anything outside the allow-list | Denied; the attempt is logged |

The policy decides from:
- reversibility: whether a compensating "undo" action exists
- blast radius: customers or money affected
- bounds: duration and percentage
- whether the critic model agrees with the diagnosis
- a rate limit on autonomous actions per hour
- a kill switch that sends every action to approval

Agents never choose their own tier.

In the Temporal workflow (ADR 0006), every action is still an activity, so the audit trail is complete. An automatic action records its compensating activity, so a human can undo it afterwards. An action that needs approval pauses the workflow until the approval signal arrives.

Each tier boundary is proven by eval cases (ADR 0011):
- a low-risk action is executed
- a bounded action over its limits is held
- a high-impact action is always held, even when the model insists it is urgent
- a forbidden action is denied, even under prompt injection

The policy engine (e.g. OPA/Rego, Cedar or plain Python) and the exact action catalogue are decided at the start of M3, in their own ADRs.

## Consequences

+ Shows bounded autonomy: the agent acts where the risk is acceptable and escalates where it is not
+ Approval requests are rarer and therefore meaningful, which reduces rubber-stamping
+ Enforcement still doesn't depend on the model behaving
- More policy to design, maintain and test than a single approval gate
- Automatic actions can be wrong; compensating actions, limits, the rate limit and the kill switch bound the damage but do not remove it
- Each automatic action needs a designed undo, which constrains which actions qualify as low risk
