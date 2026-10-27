# 7. A semantic mesh of data products, exposed through MCP

Status: Accepted (2026-10-27)

## Context

Giving an agent raw access to topics and tables tells it nothing about meaning, ownership, quality or what it may touch. Zhamak Dehghani's argument that agents need more than MCP connectivity applies directly.

## Options considered

- Raw tools over Kafka and Iceberg only
- Data products with descriptors, exposed through MCP (from M2)
- Data products from M1

## Decision

From M2, every stream and table is a data product with a descriptor: schema, business meaning, owner, freshness and quality guarantees, access policy, and links to related products. The MCP server exposes data products, not raw topics; the agent discovers data by meaning, then queries it. M1 stays a thin slice with raw tools, as the baseline to compare against.

## Consequences

+ The agent's access is scoped by design, which is where guardrails live
+ The M1 baseline makes the difference measurable
- Descriptors must be kept accurate; treat them as contracts
- Open question for M2: model descriptors as an OWL/SHACL ontology projected into Neo4j (new ADR)
