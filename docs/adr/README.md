# Architecture Decision Records

Format and rules: [0001](0001-record-architecture-decisions.md). Never edit an accepted ADR; supersede it with a new one.

| # | Decision | Status |
|---|---|---|
| 1 | [Record architecture decisions](0001-record-architecture-decisions.md) | Accepted |
| 2 | [Python for agents and services, Flink SQL for stream processing](0002-python-and-flink-sql.md) | Accepted |
| 3 | [Local-first infrastructure with Docker Compose](0003-local-first-docker-compose.md) | Accepted |
| 4 | [Synthetic e-commerce events as the primary data source](0004-synthetic-ecommerce-domain.md) | Accepted |
| 5 | [Claude, OpenAI and a local model behind one provider interface](0005-multi-provider-llm-interface.md) | Accepted |
| 6 | [Temporal to orchestrate the agentic workflow](0006-temporal-for-agentic-workflow.md) | Accepted |
| 7 | [A semantic mesh of data products, exposed through MCP](0007-semantic-mesh-of-data-products.md) | Accepted |
| 8 | [Postgres with pgvector for retrieval, Neo4j for the semantic graph](0008-postgres-pgvector-and-neo4j.md) | Accepted |
| 9 | [OpenTelemetry and Langfuse for AI observability](0009-opentelemetry-and-langfuse.md) | Accepted |
| 10 | [Guardrails as policy-as-code against excessive agency](0010-guardrails-as-policy-as-code.md) | Accepted |
| 11 | [A pytest eval harness with results in Langfuse](0011-own-eval-harness.md) | Accepted |
| 12 | [A hard monthly cap on paid LLM API spend](0012-llm-api-budget-cap.md) | Accepted |
| 13 | [OrbStack as the local container runtime](0013-orbstack-container-runtime.md) | Accepted |
| 14 | [Qwen3 14B as the default local model](0014-qwen3-14b-local-model.md) | Accepted |
| 15 | [Apache Kafka in KRaft mode as the event log](0015-apache-kafka-kraft.md) | Accepted |
| 16 | [Apache's reference Iceberg REST catalog for M1](0016-reference-iceberg-rest-catalog.md) | Accepted |
| 17 | [RustFS as the local S3-compatible object store](0017-rustfs-object-storage.md) | Accepted |
| 18 | [Flink jobs write to Kafka topics; a separate job materializes topics into Iceberg](0018-kafka-topics-first-iceberg-materialization.md) | Accepted |
