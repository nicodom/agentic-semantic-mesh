# M1: Thin slice

**Goal:** the smallest end-to-end system that works and can be measured. Synthetic e-commerce events flow through Kafka and Flink SQL into Iceberg; an agent answers a fixed set of questions through tool calls on three LLM providers; an eval suite scores the answers and reports token cost per answer.

**Done when:** `docker compose up` plus one command runs the whole slice, the eval suite passes on at least one provider, the results compare all three, and every decision taken along the way has an ADR.

**Deliberately not in M1:** data products and descriptors, MCP, retrieval, Neo4j, Temporal, guardrails. M1 uses raw tools, so M2 has a baseline to beat.

## Steps

Each step ends with something runnable and a commit.

### 0. Toolchain

- Container runtime for Docker Compose → **ADR** (OrbStack, Docker Desktop or Colima)
- `uv` for Python
- Ollama and a local model that supports tool calling → **ADR** (which model, and why)
- Anthropic and OpenAI API keys, with spend limits set in both consoles (ADR 0012)

### 1. Repo skeleton

- `uv` project, Python 3.12+, `ruff`, `pytest`
- `.env.example`, `Makefile` (or `justfile`) with `up`, `down`, `demo`, `eval`
- GitHub Actions: lint and unit tests

### 2. Streaming platform in Compose

- Kafka (single broker, KRaft) → **ADR** if choosing an alternative (e.g. Redpanda)
- Flink (JobManager, TaskManager, SQL client)
- MinIO and an Iceberg REST catalog
- Smoke test: produce a message, read it back; create an Iceberg table, query it

### 3. Event generator with ground truth

- Event types: `cart_updated`, `checkout_started`, `order_placed`, `payment_attempted`, `inventory_changed`, `return_requested` → **ADR** on schema format (JSON Schema vs Avro with a schema registry)
- Steady-state traffic plus injectable scenarios, starting with *payment-provider outage*: payment failures rise, checkout conversion drops
- The generator writes a ground-truth log (what was injected, when, expected effect) used by the evals

### 4. Flink SQL jobs

- Kafka source tables for each event type
- Tumbling windows: orders per minute, checkout conversion (5 min), payment failure rate by provider (5 min)
- Sinks: raw events and windowed metrics to Iceberg; metrics also to a Kafka topic

### 5. Provider interface

- One interface: messages, tool definitions, tool calls, token usage
- Adapters: Claude, OpenAI, Ollama; normalise tool-calling differences
- Unit tests with recorded responses (no paid calls in CI)

### 6. Assistant agent with raw tools

- Tools: query Iceberg tables → **ADR** on the query engine (DuckDB with PyIceberg vs Trino), and read the latest metrics from Kafka
- An agent loop written directly against the interface (no framework)
- A fixed set of about five questions, e.g. "What is checkout conversion in the last 5 minutes?", "Is any payment provider failing more than usual?", "When did the drop start?"

### 7. Observability

- Langfuse in Compose
- OpenTelemetry instrumentation with GenAI semantic conventions: one trace per question, a span per LLM call and per tool call, token counts and cost

### 8. Evals

- About 10 cases as data, expected answers derived from the ground-truth log
- `pytest` harness: exact checks for numbers and timestamps, an LLM judge only where wording varies
- One run per provider; a results table with accuracy, latency and cost per answer; scores pushed to Langfuse

### 9. Close M1

- README "Run it" section and a short demo script
- M1 retrospective (`docs/retros/m1.md`): what worked, what surprised, what M2 must fix
- ADRs for every decision taken in M1
