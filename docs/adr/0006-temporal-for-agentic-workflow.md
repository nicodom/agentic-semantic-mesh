# 6. Temporal to orchestrate the agentic workflow

Status: Accepted (2026-10-27)

## Context

The anomaly workflow makes several slow, paid LLM and tool calls and must pause, possibly for hours, until a human approves the proposed action. It must survive crashes without repeating paid calls.

## Options considered

- Temporal (durable execution)
- LangGraph (agent-specific graph framework)
- Plain Python with state in Kafka

## Decision

Temporal. Each LLM or tool call is an activity (retried, never repeated once it succeeds); human approval is a signal; the workflow history is the audit trail. Reasoning inside each step uses plain provider SDK calls, not an agent framework.

## Consequences

+ Crash-safe, resumable workflows and a complete audit trail for governance
+ Approval is a real pause, not a polling loop
- Workflow code must be deterministic
- One more service to run and learn
