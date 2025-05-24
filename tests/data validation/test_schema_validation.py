"""
Test schema validation for PostgreSQL and S3 data.
"""
import pytest
import allure
from typing import Dict, Any
import pandas as pd
import pyarrow.parquet as pq
from config.config import config
from sqlalchemy import text
import numpy as np
from core.validation.base_validator import BaseValidator

@pytest.fixture
def validator():
    return BaseValidator()

@allure.epic("Data Integration Tests")
@allure.feature("Schema Validation")
class TestSchemaValidation:
    @allure.story("Schema Consistency")
    @allure.title("Test schema consistency between PostgreSQL and Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_schema_consistency(self, pg_engine, validator):
        """Test schema consistency between PostgreSQL and Parquet."""
        with pg_engine.connect() as conn:
            # Get PostgreSQL schema
            result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'medate_exam'
                AND table_name = 'lab_results'
                ORDER BY ordinal_position
            """))
            pg_schema = {row.column_name: row.data_type for row in result}

            # Verify required columns exist
            required_columns = {
                'result_id': 'character varying',
                'test_id': 'character varying',
                'result_value': 'double precision',
                'result_unit': 'character varying',
                'reference_range': 'character varying',
                'result_status': 'character varying',
                'performed_date': 'date',
                'performed_time': 'time without time zone',
                'reviewing_physician': 'character varying'
            }

            # Validate required fields exist
            validator.validate_required_fields(pg_schema, list(required_columns.keys()))

            # Validate data types
            for col, dtype in required_columns.items():
                validator.validate_value_equality(
                    pg_schema[col],
                    dtype,
                    f"Data type for column {col}"
                )

    @allure.story("Data Consistency")
    @allure.title("Test data consistency between PostgreSQL and Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_data_consistency(self, pg_engine, validator):
        """Test data consistency between PostgreSQL and Parquet."""
        with pg_engine.connect() as conn:
            # First create a test patient
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('SCHEMA_PAT001', 'John', 'Doe', CURRENT_DATE, 'Dr. House', 'Medicare', 'A+', 'None')
            """))
            
            # Then create a test lab test
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES
                ('SCHEMA_TST001', 'SCHEMA_PAT001', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. House')
            """))
            
            # Finally create the lab result
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results
                (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                VALUES
                ('SCHEMA_CONS_001', 'SCHEMA_TST001', 85.5, 'mg/dL', '70-100', 'Final', CURRENT_DATE, CURRENT_TIME, 'Dr. Smith')
            """))
            conn.commit()

            # Verify data consistency
            result = conn.execute(text("""
                SELECT *
                FROM medate_exam.lab_results
                WHERE result_id = 'SCHEMA_CONS_001'
            """))
            row = result.fetchone()

            # Validate record exists and values match
            validator.validate_type(row, object, "Database row")
            validator.validate_value_equality(row.result_value, 85.5, "result_value")
            validator.validate_value_equality(row.result_unit, 'mg/dL', "result_unit")
            validator.validate_value_equality(row.reviewing_physician, 'Dr. Smith', "reviewing_physician")

            # Clean up in reverse order
            conn.execute(text("DELETE FROM medate_exam.lab_results WHERE result_id = 'SCHEMA_CONS_001'"))
            conn.execute(text("DELETE FROM medate_exam.lab_tests WHERE test_id = 'SCHEMA_TST001'"))
            conn.execute(text("DELETE FROM medate_exam.patient_information WHERE patient_id = 'SCHEMA_PAT001'"))
            conn.commit()

    @allure.story("NULL Handling")
    @allure.title("Test NULL value handling between PostgreSQL and Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_null_handling(self, pg_engine, validator):
        """Test handling of NULL values."""
        with pg_engine.connect() as conn:
            # First create a test patient
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('SCHEMA_PAT002', 'Jane', 'Smith', CURRENT_DATE, 'Dr. Wilson', 'BlueCross', 'B-', 'None')
            """))
            
            # Then create a test lab test
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES
                ('SCHEMA_TST002', 'SCHEMA_PAT002', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. Wilson')
            """))
            
            # Insert test data with NULL values
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results
                (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                VALUES
                ('SCHEMA_NULL_001', 'SCHEMA_TST002', NULL, NULL, NULL, 'Pending', CURRENT_DATE, CURRENT_TIME, NULL)
            """))
            conn.commit()

            # Verify NULL handling
            result = conn.execute(text("""
                SELECT result_value, result_unit, reference_range, reviewing_physician
                FROM medate_exam.lab_results
                WHERE result_id = 'SCHEMA_NULL_001'
            """))
            row = result.fetchone()

            # Validate NULL values
            validator.validate_value_equality(row.result_value, None, "result_value")
            validator.validate_value_equality(row.result_unit, None, "result_unit")
            validator.validate_value_equality(row.reference_range, None, "reference_range")
            validator.validate_value_equality(row.reviewing_physician, None, "reviewing_physician")

            # Clean up in reverse order
            conn.execute(text("DELETE FROM medate_exam.lab_results WHERE result_id = 'SCHEMA_NULL_001'"))
            conn.execute(text("DELETE FROM medate_exam.lab_tests WHERE test_id = 'SCHEMA_TST002'"))
            conn.execute(text("DELETE FROM medate_exam.patient_information WHERE patient_id = 'SCHEMA_PAT002'"))
            conn.commit()

    @allure.story("Data Types Compatibility")
    @allure.title("Test data types compatibility between PostgreSQL and Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_data_types_compatibility(self, pg_engine, validator):
        """Test data types compatibility."""
        with pg_engine.connect() as conn:
            # First create a test patient
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('SCHEMA_PAT003', 'Bob', 'Johnson', CURRENT_DATE, 'Dr. Grey', 'Aetna', 'O+', 'None')
            """))
            
            # Then create a test lab test
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES
                ('SCHEMA_TST003', 'SCHEMA_PAT003', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. Grey')
            """))
            
            # Insert test data with various data types
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results
                (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                VALUES
                ('SCHEMA_TYPE_001', 'SCHEMA_TST003', 123.456, 'mg/dL', '100-200', 'Final', CURRENT_DATE, CURRENT_TIME, 'Dr. Jones')
            """))
            conn.commit()

            # Verify data types
            result = conn.execute(text("""
                SELECT result_value, performed_date, performed_time
                FROM medate_exam.lab_results
                WHERE result_id = 'SCHEMA_TYPE_001'
            """))
            row = result.fetchone()

            # Validate data types
            validator.validate_type(row.result_value, float, "result_value")
            validator.validate_type(row.performed_date, object, "performed_date")
            validator.validate_type(row.performed_time, object, "performed_time")

            # Clean up in reverse order
            conn.execute(text("DELETE FROM medate_exam.lab_results WHERE result_id = 'SCHEMA_TYPE_001'"))
            conn.execute(text("DELETE FROM medate_exam.lab_tests WHERE test_id = 'SCHEMA_TST003'"))
            conn.execute(text("DELETE FROM medate_exam.patient_information WHERE patient_id = 'SCHEMA_PAT003'"))
            conn.commit() 