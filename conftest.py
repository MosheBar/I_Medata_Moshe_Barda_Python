"""
Global test configuration and fixtures.
"""
import os
import boto3
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from typing import Generator, Dict, Any
from datetime import datetime, timezone, UTC
from core.db.postgres_client import PostgresClient
from core.aws.aws_client import AWSClient
from config.config import config

@pytest.fixture(scope="session")
def postgres_client() -> PostgresClient:
    """Create a PostgreSQL client for testing."""
    return PostgresClient(config.postgres_url)

@pytest.fixture(scope="session")
def aws_client() -> AWSClient:
    """Create an AWS client for testing."""
    return AWSClient()

@pytest.fixture(scope="session")
def pg_engine():
    """Create database engine."""
    engine = create_engine(config.postgres_url)
    yield engine
    engine.dispose()

@pytest.fixture(scope="function")
def test_data() -> Dict[str, pd.DataFrame]:
    """Create test data for all tables."""
    # Sample patient data
    patients_data = pd.DataFrame({
        'patient_id': ['TEST001', 'TEST002'],
        'first_name': ['John', 'Jane'],
        'last_name': ['Doe', 'Smith'],
        'date_of_birth': ['1990-01-01', '1992-02-02'],
        'primary_physician': ['Dr. House', 'Dr. Wilson'],
        'insurance_provider': ['Medicare', 'BlueCross'],
        'blood_type': ['A+', 'O-'],
        'allergies': ['Penicillin', 'None']
    })

    # Sample lab tests data
    lab_tests_data = pd.DataFrame({
        'test_id': ['TST001', 'TST002'],
        'patient_id': ['TEST001', 'TEST002'],
        'test_name': ['Blood Test', 'X-Ray'],
        'order_date': ['2024-03-14', '2024-03-14'],
        'order_time': ['10:00:00', '11:00:00'],
        'ordering_physician': ['Dr. House', 'Dr. Wilson']
    })

    # Sample lab results data
    lab_results_data = pd.DataFrame({
        'result_id': ['TEST001', 'TEST002'],
        'test_id': ['TST001', 'TST002'],
        'result_value': [85.5, 90.2],
        'result_unit': ['mg/dL', None],
        'reference_range': ['70-100', None],
        'result_status': ['Final', 'Preliminary'],
        'performed_date': ['2024-03-14', '2024-03-14'],
        'performed_time': ['12:00:00', '13:00:00'],
        'reviewing_physician': ['Lab Tech 1', 'Lab Tech 2']
    })

    # Sample admissions data
    admissions_data = pd.DataFrame({
        'hospitalization_case_number': ['TEST001', 'TEST002'],
        'patient_id': ['TEST001', 'TEST002'],
        'admission_date': ['2024-03-14', '2024-03-14'],
        'admission_time': ['09:00:00', '10:00:00'],
        'release_date': ['2024-03-15', None],
        'release_time': ['09:00:00', None],
        'department': ['Emergency', 'Outpatient'],
        'room_number': ['E101', 'O202']
    })

    return {
        'patient_information': patients_data,
        'lab_tests': lab_tests_data,
        'lab_results': lab_results_data,
        'admissions': admissions_data
    }

@pytest.fixture(scope="function")
def setup_test_data(postgres_client: PostgresClient, aws_client: AWSClient, 
                   test_data: Dict[str, pd.DataFrame]) -> Generator:
    """Set up test data in both PostgreSQL and S3."""
    # Clean up any existing test data first
    with postgres_client.transaction() as conn:
        for table_name in test_data.keys():
            pk_col = config.table_pk_map[table_name]
            conn.execute(text(f"""
                DELETE FROM medate_exam.{table_name}
                WHERE {pk_col} LIKE 'TEST%'
            """))

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    s3_paths = {}

    try:
        # Load data into PostgreSQL
        for table_name, df in test_data.items():
            postgres_client.write_dataframe(df, table_name, if_exists='append', schema='medate_exam')
            
            # Convert to parquet and upload to S3
            table = pa.Table.from_pandas(df)
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(table, parquet_buffer)
            
            s3_key = f"raw/parquet/{table_name}/{table_name}_{timestamp}.parquet"
            aws_client.s3.put_object(
                Bucket=config.s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue().to_pybytes()
            )
            s3_paths[table_name] = s3_key

        yield {
            'timestamp': timestamp,
            's3_paths': s3_paths,
            'data': test_data
        }

    finally:
        # Cleanup test data
        with postgres_client.transaction() as conn:
            for table_name in test_data.keys():
                pk_col = config.table_pk_map[table_name]
                conn.execute(text(f"""
                    DELETE FROM medate_exam.{table_name}
                    WHERE {pk_col} LIKE 'TEST%'
                """))
        
        for s3_key in s3_paths.values():
            aws_client.delete_s3_object(config.s3_bucket, s3_key)

@pytest.fixture(scope="session")
def schema_info(postgres_client: PostgresClient) -> Dict[str, list]:
    """Get schema information for all tables."""
    tables = ['patients', 'lab_tests', 'lab_results', 'admissions']
    schema_info = {}
    
    for table in tables:
        schema_info[table] = postgres_client.get_table_schema(table, schema='medate_exam')
    
    return schema_info

@pytest.fixture
def download_parquet(tmp_path, aws_client):
    """Download a Parquet file from S3."""
    def _download(table_name):
        key = f"parquet/{table_name}/{table_name}.parquet"
        local_path = tmp_path / f"{table_name}.parquet"
        aws_client.s3.download_file(config.s3_bucket, key, str(local_path))
        return pq.read_table(str(local_path)).to_pandas()
    return _download 