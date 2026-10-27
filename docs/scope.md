# Scope

Agreed on 2026-10-27, before any code was written. Individual decisions and their trade-offs are in [`adr/`](adr/).

## Goal

A project good enough to explain in depth: a working system, the decisions behind it, and what was learned. Quality over speed; there are no fixed dates.

## In scope

| Area | Decision | ADR |
|---|---|---|
| Language | Python for agents and services; Flink SQL for stream processing | [0002](adr/0002-python-and-flink-sql.md) |
| Infrastructure | Local-first Docker Compose; anyone can run the demo | [0003](adr/0003-local-first-docker-compose.md) |
| Data | Synthetic e-commerce events from a generator with known ground truth; a real public stream later as a second source | [0004](adr/0004-synthetic-ecommerce-domain.md) |
| Models | Claude, OpenAI and a local open model behind one provider interface | [0005](adr/0005-multi-provider-llm-interface.md) |
| Agent modes | Conversational assistant + anomaly-triggered workflow with human approval | [0006](adr/0006-temporal-for-agentic-workflow.md) |
| Orchestration | Temporal; reasoning inside each step is plain SDK calls, no agent framework | [0006](adr/0006-temporal-for-agentic-workflow.md) |
| Semantics | Semantic mesh from M2: data products with descriptors, exposed through MCP | [0007](adr/0007-semantic-mesh-of-data-products.md) |
| Stores | Postgres + pgvector for retrieval; Neo4j for the semantic graph | [0008](adr/0008-postgres-pgvector-and-neo4j.md) |
| Observability | OpenTelemetry (GenAI conventions) + self-hosted Langfuse | [0009](adr/0009-opentelemetry-and-langfuse.md) |
| Guardrails | Policy-as-code against excessive agency (OWASP LLM06), enforced at the MCP layer, plus the approval gate | [0010](adr/0010-guardrails-as-policy-as-code.md) |
| Evals | Own pytest harness; ground truth from the generator; results in Langfuse | [0011](adr/0011-own-eval-harness.md) |
| Budget | €50/month hard cap on paid LLM APIs | [0012](adr/0012-llm-api-budget-cap.md) |

Conventions: MIT licence, Python managed with `uv`, CI on GitHub Actions running evals against the local model.

## Out of scope (for now)

- Production deployment, multi-user auth, a UI beyond what a demo needs.
- Agent frameworks (LangChain, LangGraph, CrewAI): the point is to understand the mechanics.
- Formal proofs over the ontology. Open question for M2: whether to model the semantic mesh as an OWL/SHACL ontology (e.g. with [open-ontologies](https://github.com/fabio-rovai/open-ontologies)) projected into Neo4j. To be decided in an ADR at the start of M2.

## Later

- Graphify (codebase knowledge graph) around M3, with a measured before/after token comparison.
- A Confluent Cloud deployment profile.
