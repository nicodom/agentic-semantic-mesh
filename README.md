# agentic-semantic-mesh

AI agents over real-time event streams, grounded in a semantic data mesh and governed by design.

> **Status:** early development, milestone M1 in progress. The architecture below is the target, not the current state; see [Milestones](#milestones).

## What this is

A working reference system for one question: **what does it take to let AI agents act on live business data safely?**

The answer this project tests is that agents need more than connectivity. Giving an agent a tool that queries a Kafka topic is easy. Knowing which data to trust, what it means, who owns it and what the agent is allowed to do with it is the hard part. That is a data-architecture and organisation-design problem as much as an AI one.

The system has two agent modes over one event platform:

- **Assistant:** answers questions about what is happening in the business right now ("why did checkout conversion drop in the last 10 minutes?").
- **Agentic workflow:** when the stream processor detects an anomaly, an agent plans an investigation, gathers evidence through tools, has its diagnosis critiqued by a second model, and proposes an action. Nothing runs until a human approves it.

## Target architecture

```
 synthetic e-commerce events ──► Kafka ──► Flink SQL ──► Iceberg (MinIO)
                                   │          │
                                   │          └─► anomaly.detected ──► Temporal workflow ──► human approval
                                   │                                          │
                                   ▼                                          ▼
                     semantic mesh: data products with descriptors   agent (Claude / OpenAI / local)
                     (meaning, owner, contracts, access policy, links)        │
                                   │                                          │
                                   └──────────────► MCP server ◄──────────────┘
                                                (policy-enforced tools)

 Postgres + pgvector (retrieval) · Neo4j (semantic graph) · OpenTelemetry + Langfuse (traces, cost, evals)
```

## Milestones

| | Milestone | Shows |
|---|---|---|
| **M1** | Thin slice | Events → Kafka → Flink SQL → Iceberg; an agent answers a fixed set of questions through tool calls on three LLM providers; scored evals with token cost per answer |
| **M2** | Semantic mesh | Streams and tables become data products with descriptors; an MCP server exposes data products, not raw topics; the agent finds data by meaning; Neo4j holds the semantic graph |
| **M3** | Agentic workflow + guardrails | Anomaly → investigation → proposed action → human approval on Temporal; excessive-agency guardrails as policy-as-code, each proven by eval cases |
| **M4** | Write-up | Long-form write-up and a meetup talk |

There are no fixed dates: each milestone is done when it works and its decisions are recorded.

## Develop

Prerequisites: a Docker runtime ([OrbStack](https://orbstack.dev) is used here, any works; [ADR 0013](docs/adr/0013-orbstack-container-runtime.md)), [`uv`](https://docs.astral.sh/uv/), and [Ollama](https://ollama.com) with the default local model ([ADR 0014](docs/adr/0014-qwen3-14b-local-model.md)):

```sh
ollama pull qwen3:14b
cp .env.example .env   # then fill in
make install           # Python 3.12 and dependencies into .venv
make lint test
```

| Target | Does |
|---|---|
| `make install` | Install Python and dependencies |
| `make lint` / `make fmt` | Check / fix lint and formatting (ruff) |
| `make test` | Unit tests, no LLM calls |
| `make up` / `make down` | Start / stop the local stack (from M1 step 2) |
| `make demo` | End-to-end demo (from M1 step 9) |
| `make eval` | Agent eval suite (from M1 step 8) |

## Decisions

Every significant decision is recorded as an Architecture Decision Record in [`docs/adr/`](docs/adr/). The scope and the reasoning behind it are in [`docs/scope.md`](docs/scope.md).

## Credits

The "semantic mesh" framing builds on Zhamak Dehghani's work on Data Mesh and her talk *Why AI Agents Need More Than MCP Connectivity*. This project is an independent exploration of that idea on real-time event streams; it is not affiliated with Nextdata.

## License

MIT
