import os
import boto3
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import pytest

@pytest.fixture(scope="session")
def pg_engine():
    url = os.getenv("PG_URL")  # e.g. "postgresql://user:pass@host:5432/db"
    return create_engine(url)

@pytest.fixture(scope="session")
def s3_client():
    return boto3.client("s3")

@pytest.fixture
def download_parquet(tmp_path, s3_client):
    def _download(table_name):
        bucket = os.getenv("S3_BUCKET")
        key = f"parquet/{table_name}/{table_name}.parquet"
        local_path = tmp_path / f"{table_name}.parquet"
        s3_client.download_file(bucket, key, str(local_path))
        return pq.read_table(str(local_path)).to_pandas()
    return _download
