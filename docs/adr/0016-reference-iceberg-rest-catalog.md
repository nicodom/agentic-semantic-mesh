# 16. Apache's reference Iceberg REST catalog for M1

Status: Accepted (2026-11-09)

## Context

Iceberg tables need a catalog: the registry of which tables exist and which metadata file is current for each. A catalog that speaks the Iceberg REST API lets Flink (the writer, ADR 0018) and Python (the agent's reader, M1 step 6) see the same tables. Access control at the catalog becomes relevant once data products and guardrails arrive (ADRs 0007, 0010), but not in M1.

Versions on 2026-11-09: Iceberg 1.12.0 ships Flink runtimes up to Flink 2.3. The Flink Kafka connector's newest build is `5.0.0-2.2`, with none for 2.3, so the connector is what limits Flink. The stack therefore uses **Flink 2.2.1 with Iceberg 1.12.0**. The newest catalog fixture image is 1.10.1. That is fine, because clients and servers talk through the versioned REST API and don't need matching releases.

## Options considered

- Apache reference implementation (`apache/iceberg-rest-fixture`): one small container, built for testing, no access control
- Lakekeeper: production-grade, Rust, Postgres-backed, authorization hooks
- Apache Polaris: role-based access control built in; heavier (Java) and needs bootstrapping
- The catalog built into the object store (SeaweedFS, RustFS "S3 Tables")

## Decision

The reference implementation (`apache/iceberg-rest-fixture:1.10.1`) for M1, with its table registry in SQLite on a Docker volume so tables survive restarts. The choice is revisited at the start of M2, when access control at the catalog starts to matter. The built-in catalogs of SeaweedFS and RustFS are candidates then.

## Consequences

+ Simplest possible catalog; storage and catalog stay separate, swappable parts
+ A neutral baseline for the M2 comparison
- No authentication or authorization; anyone who can reach port 8181 can change tables
- The fixture is a test tool, not a supported product; acceptable because the tables can always be rebuilt from Kafka and the generator
