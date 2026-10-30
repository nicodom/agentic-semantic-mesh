# 13. OrbStack as the local container runtime

Status: Accepted (2026-10-30)

## Context

ADR 0003 runs everything in Docker Compose. On macOS a container runtime runs a Linux VM, and the stack is heavy: Kafka, Flink (JobManager and TaskManager), MinIO, an Iceberg catalog, and later Langfuse with its own Postgres, ClickHouse and Redis, all alongside a local model in Ollama. The development machine is an Apple Silicon Mac with 64 GB of RAM.

## Options considered

- OrbStack: a lightweight VM tuned for Apple Silicon, with dynamic memory and fast startup. Proprietary, free for personal use
- Colima: open source (Lima), CLI-driven; the VM's CPU and memory are sized by hand
- Docker Desktop: the reference experience, heavier on resources, licence restricts larger companies

## Decision

OrbStack on the development machine. The compose files use only standard Docker Compose features, so readers can run the demo with any runtime.

## Consequences

+ Low idle memory and fast container start, which matters with a dozen services plus a local model
+ Memory is returned to macOS when containers stop
- Proprietary; a reader who wants an all-open-source setup should use Colima
- OrbStack's default memory limit (about 16 GB on this machine) may need raising once Langfuse joins the stack
