"""Flink SQL jobs: windowed metrics and the materialization into Iceberg (ADRs 0018, 0023).

`ddl` derives table definitions from the `.avsc` files, `jobs` builds each job's SQL script,
and `runner` registers schemas, submits scripts and stops jobs with savepoints.
"""
