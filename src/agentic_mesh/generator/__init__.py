"""Synthetic e-commerce event generator with ground truth (ADR 0021).

`simulator` is the pure core: shopper sessions in, time-ordered events out, no Kafka.
`ground_truth` counts what was generated, `kafka_writer` sends events to Kafka as Avro,
and `cli` ties them together.
"""
