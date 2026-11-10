# agentic-semantic-mesh

AI agents over real-time event streams, grounded in a semantic data mesh and governed by design.

> **Status:** early development, milestone M1 in progress. The architecture below is the target, not the current state; see [Milestones](#milestones).

## What this is

A working reference system for one question: **what does it take to let AI agents act on live business data safely?**

The answer this project tests is that agents need more than connectivity. Giving an agent a tool that queries a Kafka topic is easy. Knowing which data to trust, what it means, who owns it and what the agent is allowed to do with it is the hard part. That is a data-architecture and organisation-design problem as much as an AI one.

The system has two agent modes over one event platform:

- **Assistant:** answers questions about what is happening in the business right now ("why did checkout conversion drop in the last 10 minutes?").
- **Agentic workflow:** when the stream processor detects an anomaly, an agent plans an investigation, gathers evidence through tools, has its diagnosis critiqued by a second model, and proposes an action. A policy outside the model decides what happens next: low-risk, reversible actions within limits run automatically and are reported; high-impact actions wait for a human to approve them.

## Target architecture

```
 synthetic e-commerce events ──► Kafka topics ◄──► Flink SQL jobs (metrics, anomalies: written back to Kafka)
                                   │
                                   ├─► materialization job ──► Iceberg tables (RustFS)   [Tableflow in the cloud]
                                   │
                                   ├─► anomaly.detected ──► Temporal workflow ──► policy: low risk → runs automatically
                                   │                                          │           high impact → human approval
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
| **M3** | Agentic workflow + guardrails | Anomaly → investigation → proposed action on Temporal; policy decides whether the action runs automatically (low risk, within limits) or waits for human approval (high impact); excessive-agency guardrails as policy-as-code, each tier boundary proven by eval cases |
| **M4** | Write-up | Long-form write-up and a meetup talk |

There are no fixed dates: each milestone is done when it works and its decisions are recorded.

## Develop

Prerequisites: a Docker runtime ([OrbStack](https://orbstack.dev) is used here, any works; [ADR 0013](docs/adr/0013-orbstack-container-runtime.md)), [`uv`](https://docs.astral.sh/uv/), and [Ollama](https://ollama.com) with the default local model ([ADR 0014](docs/adr/0014-qwen3-14b-local-model.md)):

```sh
ollama pull qwen3:14b
cp .env.example .env   # then fill in
make install           # Python 3.12 and dependencies into .venv
make lint test
make up smoke          # start the local stack, then check data flows through it
```

| Target | Does |
|---|---|
| `make install` | Install Python and dependencies |
| `make lint` / `make fmt` | Check / fix lint and formatting (ruff) |
| `make test` | Unit tests: no LLM calls, no running stack |
| `make up` / `make down` | Start the local stack and wait until healthy / stop it, keeping data |
| `make clean` | Stop the stack and delete its data |
| `make smoke` | Smoke tests against the running stack: Kafka, Iceberg, topic → Flink → Iceberg, and the event generator |
| `make backfill` | Generate the last 2 hours of shop events into Kafka (seed 42), with a payment-provider outage halfway |
| `make generate` | Generate events live until Ctrl-C, with an outage after 5 minutes |
| `make peek TOPIC=…` | Show the latest events of a topic, decoded from Avro |
| `make demo` | End-to-end demo (from M1 step 9) |
| `make eval` | Agent eval suite (from M1 step 8) |

The local stack ([`compose.yaml`](compose.yaml)) uses about 2 GB of memory:

| Service | Host port | Notes |
|---|---|---|
| Kafka 4.4 (KRaft) | `localhost:9092` | [ADR 0015](docs/adr/0015-apache-kafka-kraft.md) |
| Schema Registry | `localhost:8085` | Avro schemas of every topic; [ADR 0020](docs/adr/0020-avro-with-confluent-schema-registry.md) |
| Flink 2.2 | UI on [localhost:8081](http://localhost:8081) | SQL client: `docker compose exec jobmanager ./bin/sql-client.sh` |
| Iceberg REST catalog | `localhost:8181` | [ADR 0016](docs/adr/0016-reference-iceberg-rest-catalog.md) |
| RustFS (S3) | API `localhost:9000`, console [localhost:9001](http://localhost:9001) | Login `asm-local` / `asm-local-secret` (local defaults); [ADR 0017](docs/adr/0017-rustfs-object-storage.md) |

### Events and ground truth

The generator ([ADR 0021](docs/adr/0021-python-event-generator-with-event-time.md)) simulates shopper sessions and writes one topic per event type, as Avro, with the event time as the Kafka timestamp:

| Topic | Key | What happened |
|---|---|---|
| `shop.cart_updated` | `session_id` | Units of a product added to or removed from a cart |
| `shop.checkout_started` | `session_id` | Checkout started; the order is created as pending |
| `shop.payment_attempted` | `session_id` | One payment attempt; after a failure the customer may retry once with another provider |
| `shop.order_placed` | `order_id` | Order paid and placed |
| `shop.inventory_changed` | `sku` | Stock changed by an order, a return or a restock |
| `shop.return_requested` | `order_id` | Return requested for a product of an order |

A session is one customer's visit, from the first cart change to checkout, with at most one order. Keys are partitioning keys, not primary keys: all events of a session share a partition and stay in order. The topics are append-only. The schemas, with a description of every field, are in [`src/agentic_mesh/generator/schemas/`](src/agentic_mesh/generator/schemas/).

Each run writes its ground truth to `data/local/ground-truth/` as JSONL: the run's parameters and scenarios, then exact counts per epoch-aligned window (orders per minute; checkouts, orders, conversion and abandonment per 5 minutes; payment attempts, failures and retries by provider per 5 minutes). The evals and the Flink jobs are checked against these numbers. A backfill with the same `--seed` and `--end` produces identical events.

## Decisions

Every significant decision is recorded as an Architecture Decision Record in [`docs/adr/`](docs/adr/). The scope and the reasoning behind it are in [`docs/scope.md`](docs/scope.md).

## Credits

The "semantic mesh" framing builds on Zhamak Dehghani's work on Data Mesh and her talk *Why AI Agents Need More Than MCP Connectivity*. This project is an independent exploration of that idea on real-time event streams; it is not affiliated with Nextdata.

## License

MIT
