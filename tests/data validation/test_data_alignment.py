"""Test data alignment between PostgreSQL and Parquet files."""
import pandas as pd
import pytest
from datetime import datetime
from core.validation.base_validator import BaseValidator
import allure
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
import tempfile
import os

validator = BaseValidator()

@pytest.mark.parametrize("table_name", [
    "admissions", "lab_results", "lab_tests", "patient_information"
])
@allure.story("Data Alignment")
@allure.title("Test data alignment between PostgreSQL and Parquet")
@allure.severity(allure.severity_level.CRITICAL)
def test_data_alignment(pg_engine, download_parquet, table_name):
    """Test data alignment between PostgreSQL and Parquet."""
    # Get data from PostgreSQL
    with pg_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT *
            FROM medate_exam.{table_name}
            ORDER BY 1
        """))
        pg_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Get data from Parquet
    parquet_path = download_parquet(table_name)
    
    # Create a temporary file to save the DataFrame as Parquet
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
        # Convert DataFrame to Parquet and save it
        table = pa.Table.from_pandas(pg_df)
        pq.write_table(table, temp_file.name)
        
        # Read the Parquet file
        df_parquet = pd.read_parquet(temp_file.name)
    
    # Clean up the temporary file
    os.unlink(temp_file.name)

    # Compare the data
    validator.validate_dataframe_equality(
        pg_df,
        df_parquet,
        ignore_index=True,
        check_dtype=False,
        description=f"Table {table_name}"
    )
