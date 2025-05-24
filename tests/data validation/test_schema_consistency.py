"""
Test schema consistency between PostgreSQL and Parquet.
"""
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
import allure
from datetime import datetime, UTC
import tempfile
import os

def get_pg_schema(pg_engine, table_name):
    """Get schema information from PostgreSQL."""
    with pg_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'medate_exam'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """))
        return {row[0]: row[1] for row in result}

def create_sample_parquet(df, tmp_path, table_name):
    """Create a sample Parquet file from DataFrame."""
    file_path = os.path.join(tmp_path, f"{table_name}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    return file_path

def is_compatible_type(pa_type: str, pg_type: str) -> bool:
    """Check if PyArrow type is compatible with PostgreSQL type."""
    pa_type = str(pa_type).lower()
    pg_type = pg_type.lower()
    
    # Handle string types
    if pg_type in ['character varying', 'text', 'varchar']:
        return 'string' in pa_type
    
    # Handle numeric types
    if pg_type in ['double precision', 'float8', 'real']:
        return 'double' in pa_type or 'float' in pa_type
    
    if pg_type in ['integer', 'int']:
        return 'int' in pa_type
    
    # Handle date/time types
    if pg_type == 'date':
        return 'date' in pa_type or 'timestamp' in pa_type or 'string' in pa_type
    
    if pg_type == 'time without time zone':
        return 'time' in pa_type or 'string' in pa_type
    
    if 'timestamp' in pg_type:
        return 'timestamp' in pa_type or 'string' in pa_type
    
    return False

@pytest.mark.parametrize("table_name", [
    "admissions", "lab_results", "lab_tests", "patient_information"
])
@allure.story("Schema Consistency")
@allure.title("Test schema consistency between PostgreSQL and Parquet")
@allure.severity(allure.severity_level.CRITICAL)
def test_schema_consistency(pg_engine, test_data, table_name, tmp_path):
    """Test schema consistency between PostgreSQL and Parquet."""
    # Get PostgreSQL schema
    pg_schema = get_pg_schema(pg_engine, table_name)
    
    # Create sample Parquet file
    df = test_data[table_name]
    parquet_path = create_sample_parquet(df, tmp_path, table_name)
    parquet_schema = pq.read_schema(parquet_path)
    
    # Compare schemas
    for pg_col, pg_type in pg_schema.items():
        # Verify column exists in Parquet schema
        assert pg_col in parquet_schema.names, f"Column {pg_col} missing in Parquet schema"
        
        # Get PyArrow type
        pa_type = parquet_schema.field(pg_col).type
        
        # Verify type compatibility
        assert is_compatible_type(pa_type, pg_type), \
            f"Type mismatch for {pg_col}: PostgreSQL type {pg_type} not compatible with PyArrow type {pa_type}"
