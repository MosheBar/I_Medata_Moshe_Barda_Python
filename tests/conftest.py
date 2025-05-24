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
        'patient_id': ['TEST001', 'TEST002', 'CRUD_PAT001'],
        'first_name': ['John', 'Jane', 'Bob'],
        'last_name': ['Doe', 'Smith', 'Johnson'],
        'date_of_birth': pd.Series(['1990-01-01', '1992-02-02', '1985-03-03']).apply(pd.to_datetime).dt.date,
        'primary_physician': ['Dr. House', 'Dr. Wilson', 'Dr. Smith'],
        'insurance_provider': ['Medicare', 'BlueCross', 'Aetna'],
        'blood_type': ['A+', 'O-', 'B+'],
        'allergies': ['Penicillin', 'None', 'None']
    })

    # Sample lab tests data
    lab_tests_data = pd.DataFrame({
        'test_id': ['TST001', 'TST002', 'TST003', 'CRUD_TST001'],
        'patient_id': ['TEST001', 'TEST002', 'TEST001', 'CRUD_PAT001'],
        'test_name': ['Blood Test', 'X-Ray', 'MRI', 'Blood Test'],
        'order_date': pd.Series(['2024-03-14', '2024-03-14', '2024-03-14', '2024-03-14']).apply(pd.to_datetime).dt.date,
        'order_time': pd.Series(['10:00:00', '11:00:00', '12:00:00', '13:00:00']).apply(pd.to_datetime).dt.time,
        'ordering_physician': ['Dr. House', 'Dr. Wilson', 'Dr. House', 'Dr. Smith']
    })

    # Sample lab results data
    lab_results_data = pd.DataFrame({
        'result_id': ['TEST001', 'TEST002', 'TEST003'],
        'test_id': ['TST001', 'TST002', 'TST003'],
        'result_value': [85.5, 90.2, 95.0],
        'result_unit': ['mg/dL', None, 'mg/dL'],
        'reference_range': ['70-100', None, '70-100'],
        'result_status': ['Final', 'Preliminary', 'Final'],
        'performed_date': pd.Series(['2024-03-14', '2024-03-14', '2024-03-14']).apply(pd.to_datetime).dt.date,
        'performed_time': pd.Series(['12:00:00', '13:00:00', '14:00:00']).apply(pd.to_datetime).dt.time,
        'reviewing_physician': ['Lab Tech 1', 'Lab Tech 2', 'Lab Tech 3']
    })

    # Sample admissions data
    admissions_data = pd.DataFrame({
        'hospitalization_case_number': ['TEST001', 'TEST002', 'TEST003'],
        'patient_id': ['TEST001', 'TEST002', 'TEST001'],
        'admission_date': pd.Series(['2024-03-14', '2024-03-14', '2024-03-14']).apply(pd.to_datetime).dt.date,
        'admission_time': pd.Series(['09:00:00', '10:00:00', '11:00:00']).apply(pd.to_datetime).dt.time,
        'release_date': pd.Series(['2024-03-15', None, '2024-03-16']).apply(lambda x: pd.to_datetime(x).date() if pd.notna(x) else None),
        'release_time': pd.Series(['09:00:00', None, '10:00:00']).apply(lambda x: pd.to_datetime(x).time() if pd.notna(x) else None),
        'department': ['Emergency', 'Outpatient', 'Emergency'],
        'room_number': ['E101', 'O202', 'E103']
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
    # Clean up any existing test data first in reverse order
    cleanup_order = ['lab_results', 'lab_tests', 'admissions', 'patient_information']
    with postgres_client.transaction() as conn:
        # First, delete all test data in reverse order to respect foreign keys
        for table_name in cleanup_order:
            conn.execute(text(f"""
                DELETE FROM medate_exam.{table_name}
                WHERE {config.table_pk_map[table_name]} LIKE 'TEST%' OR {config.table_pk_map[table_name]} LIKE 'SCHEMA%' OR {config.table_pk_map[table_name]} LIKE 'CRUD%' OR {config.table_pk_map[table_name]} LIKE 'REFL%'
            """))
        conn.commit()

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    s3_paths = {}

    try:
        # Load data into PostgreSQL in the correct order
        table_order = ['patient_information', 'lab_tests', 'lab_results', 'admissions']
        
        # First, insert all patient records
        with postgres_client.transaction() as conn:
            df = test_data['patient_information']
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT INTO medate_exam.patient_information
                    (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                    VALUES (:patient_id, :first_name, :last_name, :date_of_birth, :primary_physician, :insurance_provider, :blood_type, :allergies)
                    ON CONFLICT (patient_id) DO NOTHING
                """), row.to_dict())
            conn.commit()
            
        # Then insert lab tests
        with postgres_client.transaction() as conn:
            df = test_data['lab_tests']
            for _, row in df.iterrows():
                result = conn.execute(text("""
                    SELECT patient_id FROM medate_exam.patient_information WHERE patient_id = :patient_id
                """), {'patient_id': row['patient_id']})
                if result.scalar() is not None:
                    conn.execute(text("""
                        INSERT INTO medate_exam.lab_tests
                        (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                        VALUES (:test_id, :patient_id, :test_name, :order_date, :order_time, :ordering_physician)
                        ON CONFLICT (test_id) DO NOTHING
                    """), row.to_dict())
            conn.commit()
            
        # Then insert lab results
        with postgres_client.transaction() as conn:
            df = test_data['lab_results']
            for _, row in df.iterrows():
                result = conn.execute(text("""
                    SELECT test_id FROM medate_exam.lab_tests WHERE test_id = :test_id
                """), {'test_id': row['test_id']})
                if result.scalar() is not None:
                    conn.execute(text("""
                        INSERT INTO medate_exam.lab_results
                        (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                        VALUES (:result_id, :test_id, :result_value, :result_unit, :reference_range, :result_status, :performed_date, :performed_time, :reviewing_physician)
                        ON CONFLICT (result_id) DO NOTHING
                    """), row.to_dict())
            conn.commit()
            
        # Finally insert admissions
        with postgres_client.transaction() as conn:
            df = test_data['admissions']
            for _, row in df.iterrows():
                result = conn.execute(text("""
                    SELECT patient_id FROM medate_exam.patient_information WHERE patient_id = :patient_id
                """), {'patient_id': row['patient_id']})
                if result.scalar() is not None:
                    conn.execute(text("""
                        INSERT INTO medate_exam.admissions
                        (hospitalization_case_number, patient_id, admission_date, admission_time, release_date, release_time, department, room_number)
                        VALUES (:hospitalization_case_number, :patient_id, :admission_date, :admission_time, :release_date, :release_time, :department, :room_number)
                        ON CONFLICT (hospitalization_case_number) DO NOTHING
                    """), row.to_dict())
            conn.commit()
        
        # Upload data to S3
        for table_name in table_order:
            df = test_data[table_name]
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
        # Cleanup test data in reverse order to respect foreign keys
        with postgres_client.transaction() as conn:
            for table_name in cleanup_order:
                conn.execute(text(f"""
                    DELETE FROM medate_exam.{table_name}
                    WHERE {config.table_pk_map[table_name]} LIKE 'TEST%' OR {config.table_pk_map[table_name]} LIKE 'SCHEMA%' OR {config.table_pk_map[table_name]} LIKE 'CRUD%' OR {config.table_pk_map[table_name]} LIKE 'REFL%'
                """))
            conn.commit()
        
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