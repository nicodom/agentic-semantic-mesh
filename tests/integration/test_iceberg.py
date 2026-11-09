import uuid

import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog

pytestmark = pytest.mark.integration


def test_create_append_and_read_table(catalog: Catalog) -> None:
    catalog.create_namespace_if_not_exists("smoke")
    name = f"smoke.t_{uuid.uuid4().hex[:8]}"
    rows = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "label": ["a", "b", "c"]})

    table = catalog.create_table(name, schema=rows.schema)
    try:
        table.append(rows)  # writes Parquet to RustFS, commits a snapshot via the catalog
        result = catalog.load_table(name).scan().to_arrow().sort_by("id")
        assert result.to_pydict() == rows.to_pydict()
    finally:
        catalog.purge_table(name)
