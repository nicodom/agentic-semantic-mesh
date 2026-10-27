# 3. Local-first infrastructure with Docker Compose

Status: Accepted (2026-10-27)

## Context

The demo must be reproducible by anyone who clones the repo, and should cost nothing to run.

## Options considered

- Local Docker Compose (Kafka, Flink, MinIO, Iceberg REST catalog, and the services below)
- Confluent Cloud (managed Kafka, Flink, Tableflow): closest to production practice, but metered
- Both from day one

## Decision

Docker Compose first. A Confluent Cloud profile may follow later as an optional deployment.

## Consequences

+ `docker compose up` runs the whole system, at zero cost
+ Every component is inspectable locally
- Local resource use grows with each service; keep an eye on memory
- Managed-service concerns (quotas, networking, IAM) are not exercised
