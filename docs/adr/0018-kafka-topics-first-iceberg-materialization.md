# 18. Flink jobs write to Kafka topics; a separate job materializes topics into Iceberg

Status: Accepted (2026-11-09)

## Context

The M1 plan had Flink SQL jobs write their results (raw events, windowed metrics) straight to Iceberg tables, with metrics also copied to a Kafka topic. The scope lists a Confluent Cloud deployment profile as later work ([scope](../scope.md)). In Confluent Cloud, Tableflow keeps an Iceberg table in sync with a Kafka topic as a managed service: no sink job, with file layout, compaction and the catalog handled for you. Tableflow cannot run locally, so ADR 0003 keeps it out of the default stack.

## Options considered

- Flink jobs write directly to Iceberg (the original plan)
- Flink jobs write only to Kafka topics; one separate materialization job copies the topics into Iceberg tables
- Both: each job writes to Kafka and to Iceberg

For the local materializer:

- A Flink SQL job (`INSERT INTO <iceberg table> SELECT * FROM <kafka topic>`) on the same Flink cluster
- Iceberg's Kafka Connect sink: configured per topic rather than written, the closest local analogue of Tableflow. Its installable archive is not published, so it would have to be built from Iceberg's source in our image. It would also be one more JVM service, and it infers types from plain JSON instead of using a declared schema

## Decision

Every Flink SQL job writes its output only to Kafka topics. A single, separate materialization job copies the chosen topics into Iceberg tables. Locally that job is a Flink SQL `INSERT INTO <iceberg table> SELECT * FROM <kafka topic>`. In a Confluent Cloud profile it is replaced by switching on Tableflow for the same topics.

The materialization job is a pure copy: no filters, joins or computed columns. Any logic belongs in an upstream Flink job that writes a topic. This keeps the Tableflow swap clean.

## Consequences

+ Moving to Confluent Cloud swaps one job for Tableflow; business logic in the Flink jobs does not change
+ Kafka topics are the single source of truth, and tables are derived from them. This fits data products in M2 (ADR 0007): a product is defined around its topic, and its table is a projection
+ The materialization path is written by hand once, which shows what Tableflow automates (commit intervals, small files, compaction, schema evolution) and gives a baseline to compare it with
- One more hop: data reaches Iceberg a little later than with a direct sink (bounded by the materialization job's checkpoint interval)
- Every Iceberg table needs a topic, so purely intermediate results also pass through Kafka
- The pure-copy rule is a convention, not something the engine enforces; it has to be kept in review
+ A later comparison measures all three materializers on the same topics: this Flink job, the Kafka Connect sink, and Tableflow
