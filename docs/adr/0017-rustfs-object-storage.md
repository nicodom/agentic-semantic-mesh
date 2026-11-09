# 17. RustFS as the local S3-compatible object store

Status: Accepted (2026-11-09)

## Context

Iceberg stores its data and metadata files in object storage. The scope and the M1 plan assumed MinIO. By 2026-11-09 MinIO had stopped being a usable open-source dependency: its GitHub repository is archived, its last release was in October 2025, and its images are no longer on Docker Hub. No ADR had decided on MinIO, so this records the replacement and why it was needed.

## Options considered

- SeaweedFS (Apache-2.0, Go, started 2014): mature, broad (files, objects, Hadoop FS), a built-in Iceberg catalog with table maintenance; about 11k of its ~15k commits are by one author; basic admin UI
- RustFS (Apache-2.0, Rust, started 2023, 1.0 on 2026-09-16): a MinIO reimplementation with MinIO's storage model and a full web console; built-in Iceberg catalog in preview
- Garage (AGPL-3.0, Rust): lightweight, needs a cluster-layout init step
- Scality CloudServer (Apache-2.0, Node.js): mostly used for testing, small community

## Decision

RustFS 1.0.4 (`rustfs/rustfs`). The deciding factor is its web console, which makes the files Iceberg writes easy to inspect during demos. SeaweedFS is the fallback if RustFS causes problems.

## Consequences

+ A MinIO-like experience; most MinIO tutorials and concepts still apply
+ Small image (about 110 MB) and a single container
- Young: a two-month-old 1.0 at the bottom of the stack. Any problem triggers the switch to SeaweedFS
- Switching is cheap: the stack uses only plain S3 (put, get, list, delete, path-style addressing), and the data can be rebuilt from Kafka and the generator
- A lesson for the write-up: a local-first stack depends on open-source projects staying open, and MinIO is an example that didn't
