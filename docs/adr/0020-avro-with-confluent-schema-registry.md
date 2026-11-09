# 20. Avro with Confluent Schema Registry for event serialization

Status: Accepted (2026-11-09)

## Context

Events from the generator (ADR 0021) are read by Flink SQL jobs, copied into Iceberg by the materialization job (ADR 0018), possibly replaced later by Tableflow, and described as data products with contracts in M2 (ADR 0007). All of these need typed events with a schema that lives somewhere other than in code.

Facts checked on 2026-11-09:
- Flink 2.2.1 has the `avro-confluent` format (`flink-sql-avro-confluent-registry`). It has no registry-aware JSON format, only plain `json`, so it cannot read JSON written with the registry's wire format (a 5-byte schema-ID prefix).
- [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-schemas.html) accepts Avro, Protobuf or JSON Schema from Schema Registry and does not support schemaless topics. Both key and value need a schema; for Avro the root type must be a record.

## Options considered

- Plain JSON, with types only in code: readable, no extra service; no contract, and incompatible with Tableflow
- JSON Schema with a registry: readable and registered; Flink cannot read the framing
- Protobuf with a registry: compact and typed; Flink has no registry-aware Protobuf format
- Avro with a registry. Registry options:
  - Confluent Schema Registry (Confluent Community License: free to use, not OSI open source; closest to Confluent Cloud)
  - Karapace (Apache-2.0, Confluent-compatible API)
  - Apicurio (Apache-2.0, Confluent-compatible API)

## Decision

Avro, with Confluent Schema Registry (`confluentinc/cp-schema-registry:8.4.0`) in Compose.

- The `.avsc` files in the repo are the source of truth. They are registered under the default subject naming, `<topic>-key` and `<topic>-value` (TopicNameStrategy).
- Keys and values both have Avro **record** schemas, e.g. `{"session_id": ...}` as the key, as Tableflow requires.
- Compatibility mode is the registry default, BACKWARD: consumers on a new schema can read data written with the previous one.
- Flink SQL reads and writes topics with `key.format` and `value.format` set to `avro-confluent`.

## Consequences

+ Flink, Python and (later) Tableflow and Kafka Connect all read the same registered schemas
+ The registry is a natural home for data-product contracts in M2
+ Same registry API as Confluent Cloud, so the cloud profile changes a URL and credentials, not code
- Messages are binary; inspecting a topic needs a registry-aware tool (a `make peek` helper)
- One more service in Compose
- The registry is not OSI open source; Karapace is the drop-in alternative if that matters later
- Schema changes are now governed by compatibility rules, which adds discipline and some friction
