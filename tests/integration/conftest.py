"""Shared settings for smoke tests against the local stack (`make up`, then `make smoke`)."""

import os

import pytest
from pyiceberg.catalog import Catalog, load_catalog

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP") or "localhost:9092"


@pytest.fixture(scope="session")
def catalog() -> Catalog:
    # Same defaults as compose.yaml (an empty variable means "use the default", as there);
    # the host reaches RustFS on localhost:9000.
    return load_catalog(
        "local",
        type="rest",
        uri=os.environ.get("ICEBERG_REST_URI") or "http://localhost:8181",
        **{
            "s3.endpoint": os.environ.get("S3_ENDPOINT") or "http://localhost:9000",
            "s3.access-key-id": os.environ.get("S3_ACCESS_KEY") or "asm-local",
            "s3.secret-access-key": os.environ.get("S3_SECRET_KEY") or "asm-local-secret",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
