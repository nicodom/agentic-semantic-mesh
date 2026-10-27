# 8. Postgres with pgvector for retrieval, Neo4j for the semantic graph

Status: Accepted (2026-10-27)

## Context

Two kinds of metadata need a home: embeddings for retrieval (descriptors, glossary, runbooks; a few thousand chunks) and the semantic graph (data products, teams, policies and their relationships). Events stay in Kafka and Iceberg.

## Options considered

- Postgres + pgvector for both (graph as tables, recursive queries)
- Postgres + pgvector, plus Neo4j for the graph
- A dedicated vector database

## Decision

Postgres with pgvector for embeddings, and Neo4j for the semantic graph from the start.

## Consequences

+ Native graph queries and path finding; the graph can be shown live in demos and talks
+ Postgres is already needed by Langfuse and Temporal
- One more service (JVM, 1-2 GB RAM)
- Two stores to keep consistent
