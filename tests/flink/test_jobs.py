import re

import fastavro
import pytest

from agentic_mesh.flink import ddl, jobs
from agentic_mesh.flink.ddl import Stack
from agentic_mesh.generator.events import EVENT_TYPES


@pytest.mark.parametrize(
    ("avro", "sql"),
    [
        ("string", "STRING NOT NULL"),
        ("long", "BIGINT NOT NULL"),
        ({"type": "long", "logicalType": "timestamp-millis"}, "TIMESTAMP(3) NOT NULL"),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2},
            "DECIMAL(10, 2) NOT NULL",
        ),
        ({"type": "enum", "name": "E", "symbols": ["a"]}, "STRING NOT NULL"),
        (["null", "double"], "DOUBLE"),
    ],
)
def test_avro_types_map_to_flink_sql(avro: object, sql: str) -> None:
    assert ddl.sql_type(avro) == sql


@pytest.mark.parametrize("job", jobs.METRIC_JOBS, ids=lambda j: j.name)
def test_metric_schemas_are_valid_and_keys_repeat_value_fields(job: jobs.MetricJob) -> None:
    key, value = jobs.schema(f"{job.name}_key"), jobs.schema(job.name)
    fastavro.parse_schema(key)
    fastavro.parse_schema(value)
    value_types = {f["name"]: f["type"] for f in value["fields"]}
    for field in key["fields"]:
        assert value_types[field["name"]] == field["type"]


def test_metric_script_reads_its_sources_and_writes_its_topic() -> None:
    job = jobs.METRIC_JOBS[1]  # checkout funnel: two sources
    sql = jobs.metric_script(job, Stack(topic_prefix="t.", bounded=True), "s3://flink/sp-1")
    assert "SET 'execution.state-recovery.path' = 's3://flink/sp-1';" in sql
    assert "SET 'table.dml-sync' = 'true';" in sql
    assert "'topic' = 't.shop.checkout_started'" in sql
    assert "'topic' = 't.shop.order_placed'" in sql
    assert "'topic' = 't.metrics.checkout_funnel_5m'" in sql
    assert sql.count("'scan.bounded.mode' = 'latest-offset'") == 2
    assert "INSERT INTO checkout_funnel_5m" in sql


def test_schemas_are_inlined_as_sql_literals() -> None:
    sql = jobs.metric_script(jobs.METRIC_JOBS[0], Stack())
    # Event docs contain apostrophes ("customer's"); SQL doubles them.
    assert "customer''s" in sql
    assert "scan.bounded.mode" not in sql
    assert "SET 'table.dml-sync' = 'false';" in sql


def test_only_event_topics_get_watermarks() -> None:
    sql = jobs.materialization_script(Stack())
    sources = re.findall(r"CREATE TEMPORARY TABLE `(\w+)` \((.*?)\) WITH", sql, re.S)
    assert len(sources) == len(EVENT_TYPES) + len(jobs.METRIC_JOBS)
    for name, body in sources:
        assert ("WATERMARK FOR" in body) == name.startswith("shop_"), name


def test_materialization_copies_every_topic_with_kafka_metadata() -> None:
    sql = jobs.materialization_script(Stack(topic_prefix="smoke-ab12."))
    inserts = re.findall(r"INSERT INTO lake\.`(\w+)`\.`(\w+)` SELECT (.*?) FROM", sql)
    assert {(db, t) for db, t, _ in inserts} == {
        *(("smoke_ab12_shop", e) for e in EVENT_TYPES),
        *(("smoke_ab12_metrics", j.name) for j in jobs.METRIC_JOBS),
    }
    for _, _, cols in inserts:
        assert cols.endswith("`kafka_partition`, `kafka_offset`, `kafka_timestamp`")
    assert "EXECUTE STATEMENT SET BEGIN" in sql
