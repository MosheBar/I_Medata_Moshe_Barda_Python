"""
Test CRUD operations for data validation.
"""
import pytest
import allure
from datetime import datetime, timezone, UTC, date, timedelta
import pandas as pd
from sqlalchemy import text
from config.config import config
from core.validation.base_validator import BaseValidator

@pytest.fixture
def validator():
    return BaseValidator()

@allure.epic("Data Integration Tests")
@allure.feature("CRUD Operations")
class TestCRUDOperations:
    @allure.story("Create Operation")
    @allure.title("Test creating new records in PostgreSQL and Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_create_operation(self, postgres_client, aws_client, setup_test_data, validator):
        """Test creating new records in PostgreSQL and verifying in Parquet."""
        # First ensure we have the required test data
        with postgres_client.transaction() as conn:
            # Clean up any existing test data in reverse order
            conn.execute(text("""
                DELETE FROM medate_exam.lab_results
                WHERE result_id LIKE 'TEST_CRUD%'
            """))
            conn.execute(text("""
                DELETE FROM medate_exam.lab_tests
                WHERE test_id LIKE 'TEST_CRUD%'
            """))
            conn.execute(text("""
                DELETE FROM medate_exam.patient_information
                WHERE patient_id LIKE 'TEST_CRUD%'
            """))
            conn.commit()

        # Create test data in correct order
        with postgres_client.transaction() as conn:
            # Create test patient first
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('TEST_CRUD_PAT001', 'John', 'Doe', CURRENT_DATE, 'Dr. House', 'Medicare', 'A+', 'None')
            """))
            conn.commit()

        # Verify patient was created
        with postgres_client.transaction() as conn:
            result = conn.execute(text("""
                SELECT patient_id FROM medate_exam.patient_information 
                WHERE patient_id = 'TEST_CRUD_PAT001'
            """))
            assert result.scalar() is not None, "Patient record was not created"
            
            # Create test lab test
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES
                ('TEST_CRUD_TST001', 'TEST_CRUD_PAT001', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. House')
            """))
            conn.commit()

        with allure.step("Creating new test lab result record"):
            new_result = pd.DataFrame({
                'result_id': ['TEST_CRUD_RES001'],
                'test_id': ['TEST_CRUD_TST001'],  # Use the test ID we just created
                'result_value': [85.5],
                'result_unit': ['mg/dL'],
                'reference_range': ['70-100'],
                'result_status': ['Final'],
                'performed_date': [date.today()],
                'performed_time': [datetime.now(UTC).time()],
                'reviewing_physician': ['Dr. Smith']
            })
        
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        s3_key = f"raw/parquet/lab_results/lab_results_{timestamp}.parquet"
        
        with allure.step("Inserting data into PostgreSQL"):
            with postgres_client.transaction() as conn:
                new_result.to_sql('lab_results', conn, schema='medate_exam', if_exists='append', index=False)
                conn.commit()
        
        with allure.step("Writing data to S3 Parquet"):
            aws_client.write_parquet(new_result, config.s3_bucket, s3_key)
        
        with allure.step("Verifying data in PostgreSQL"):
            query = "SELECT * FROM medate_exam.lab_results WHERE result_id = 'TEST_CRUD_RES001'"
            pg_result = postgres_client.execute_query(query)
            validator.validate_record_count(len(pg_result), 1, "Record count in PostgreSQL")
        
        with allure.step("Verifying data in Parquet"):
            s3_df = aws_client.read_parquet(config.s3_bucket, s3_key)
            validator.validate_record_exists(s3_df, 'result_id', 'TEST_CRUD_RES001', "Record in Parquet")

    @allure.story("Read Operation")
    @allure.title("Test reading and comparing data between PostgreSQL and Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_read_operation(self, postgres_client, aws_client, setup_test_data, validator):
        """Test reading and comparing data between PostgreSQL and Parquet."""
        test_data = setup_test_data
        timestamp = test_data['timestamp']
        
        for table_name in ['lab_results', 'lab_tests', 'admissions', 'patient_information']:
            with allure.step(f"Testing read operations for table: {table_name}"):
                with allure.step("Reading from PostgreSQL"):
                    pg_df = postgres_client.read_table(table_name)
                    # Filter by appropriate ID column
                    id_col = {
                        'lab_results': 'result_id',
                        'lab_tests': 'test_id',
                        'admissions': 'hospitalization_case_number',
                        'patient_information': 'patient_id'
                    }[table_name]
                    pg_df = pg_df[pg_df[id_col].str.startswith('TEST')]
                
                # Write to S3 first
                s3_key = f"raw/parquet/{table_name}/{table_name}_{timestamp}.parquet"
                aws_client.write_parquet(pg_df, config.s3_bucket, s3_key)
                
                with allure.step("Reading from Parquet"):
                    s3_df = aws_client.read_parquet(config.s3_bucket, s3_key)
                
                with allure.step("Comparing data"):
                    validator.validate_dataframe_equality(
                        pg_df, s3_df,
                        sort_by=id_col,
                        check_dtype=False,
                        description=f"DataFrame comparison for {table_name}"
                    )

    @allure.story("Update Operation")
    @allure.title("Test updating records in PostgreSQL and reflecting in Parquet")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_update_operation(self, postgres_client, aws_client, setup_test_data, validator):
        """Test updating records in PostgreSQL and reflecting changes in Parquet."""
        test_data = setup_test_data
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        
        # First, clean up any existing test records
        with postgres_client.transaction() as conn:
            conn.execute(text("""
                DELETE FROM medate_exam.lab_results
                WHERE result_id LIKE 'CRUD_UPDATE%'
            """))
            conn.execute(text("""
                DELETE FROM medate_exam.lab_tests
                WHERE test_id LIKE 'CRUD_UPDATE%'
            """))
            conn.execute(text("""
                DELETE FROM medate_exam.patient_information
                WHERE patient_id LIKE 'CRUD_UPDATE%'
            """))
            conn.commit()

        # Set up test data in correct order
        with postgres_client.transaction() as conn:
            # Create test patient first
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('CRUD_UPDATE_PAT001', 'John', 'Doe', CURRENT_DATE, 'Dr. House', 'Medicare', 'A+', 'None')
            """))
            
            # Create test lab test
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES
                ('CRUD_UPDATE_TST001', 'CRUD_UPDATE_PAT001', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. House')
            """))
            
            # Insert initial lab result
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results (
                    result_id, test_id, result_value, result_unit, reference_range,
                    result_status, performed_date, performed_time, reviewing_physician
                ) VALUES (
                    'CRUD_UPDATE_001', 'CRUD_UPDATE_TST001', 85.5, 'mg/dL', '70-100',
                    'Final', CURRENT_DATE, CURRENT_TIME, 'Dr. Original'
                )
            """))
            conn.commit()
        
        # Verify the record was inserted
        query = "SELECT * FROM medate_exam.lab_results WHERE result_id = 'CRUD_UPDATE_001'"
        pg_result = postgres_client.execute_query(query)
        validator.validate_record_count(len(pg_result), 1, "Initial record count")
        validator.validate_value_equality(
            pg_result[0]['reviewing_physician'],
            'Dr. Original',
            'reviewing_physician'
        )
        
        # Update the record
        with postgres_client.transaction() as conn:
            conn.execute(text("""
                UPDATE medate_exam.lab_results
                SET reviewing_physician = 'Dr. Updated'
                WHERE result_id = 'CRUD_UPDATE_001'
            """))
            conn.commit()
        
        # Read and verify the update in PostgreSQL
        pg_result = postgres_client.execute_query(query)
        validator.validate_record_count(len(pg_result), 1, "Record count after update")
        validator.validate_value_equality(
            pg_result[0]['reviewing_physician'],
            'Dr. Updated',
            'reviewing_physician after update'
        )
        
        # Write to Parquet and verify
        pg_df = postgres_client.read_table('lab_results')
        pg_df = pg_df[pg_df['result_id'].str.startswith('CRUD_UPDATE')]
        
        s3_key = f"raw/parquet/lab_results/lab_results_{timestamp}.parquet"
        aws_client.write_parquet(pg_df, config.s3_bucket, s3_key)
        
        s3_df = aws_client.read_parquet(config.s3_bucket, s3_key)
        updated_records = s3_df[s3_df['result_id'] == 'CRUD_UPDATE_001']
        validator.validate_record_count(len(updated_records), 1, "Record count in Parquet")
        validator.validate_value_equality(
            updated_records.iloc[0]['reviewing_physician'],
            'Dr. Updated',
            'reviewing_physician in Parquet'
        )

    @allure.story("Delete Operation")
    @allure.title("Test deleting records and maintaining consistency")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_delete_operation(self, postgres_client, aws_client, setup_test_data, validator):
        """Test deleting records and maintaining consistency."""
        test_data = setup_test_data
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        
        with allure.step("Deleting lab result from PostgreSQL"):
            with postgres_client.transaction() as conn:
                conn.execute(text("""
                    DELETE FROM medate_exam.lab_results
                    WHERE result_id = 'TEST002'
                """))
        
        with allure.step("Reading remaining data from PostgreSQL"):
            pg_df = postgres_client.read_table('lab_results')
            pg_df = pg_df[pg_df['result_id'].str.startswith('TEST')]
        
        s3_key = f"raw/parquet/lab_results/lab_results_{timestamp}.parquet"
        with allure.step("Writing updated data to Parquet"):
            aws_client.write_parquet(pg_df, config.s3_bucket, s3_key)
        
        with allure.step("Verifying deletion in PostgreSQL"):
            query = "SELECT COUNT(*) as count FROM medate_exam.lab_results WHERE result_id = 'TEST002'"
            pg_result = postgres_client.execute_query(query)
            validator.validate_value_equality(pg_result[0]['count'], 0, "Deleted record count in PostgreSQL")
        
        with allure.step("Verifying deletion in Parquet"):
            s3_df = aws_client.read_parquet(config.s3_bucket, s3_key)
            deleted_records = s3_df[s3_df['result_id'] == 'TEST002']
            validator.validate_record_not_exists(deleted_records, 'TEST002', 'lab_results', "Deleted record in Parquet")

    @allure.story("Transaction Consistency")
    @allure.title("Test data consistency during transactions")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_transaction_consistency(self, postgres_client, aws_client, setup_test_data, validator):
        """Test data consistency during transactions."""
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        
        # Clean up and prepare test data
        with postgres_client.transaction() as conn:
            # Clean up existing test records
            conn.execute(text("""
                DELETE FROM medate_exam.lab_results
                WHERE result_id LIKE 'CRUD_TRANS%'
            """))
            conn.execute(text("""
                DELETE FROM medate_exam.lab_tests
                WHERE test_id LIKE 'CRUD_TRANS%'
            """))
            conn.execute(text("""
                DELETE FROM medate_exam.patient_information
                WHERE patient_id LIKE 'CRUD_TRANS%'
            """))
            
            # First create test patient
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('CRUD_TRANS_PAT001', 'John', 'Doe', CURRENT_DATE, 'Dr. House', 'Medicare', 'A+', 'None')
            """))
            
            # Then create test lab tests
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES 
                ('CRUD_TRANS_TST001', 'CRUD_TRANS_PAT001', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. House'),
                ('CRUD_TRANS_TST002', 'CRUD_TRANS_PAT001', 'X-Ray', CURRENT_DATE, CURRENT_TIME, 'Dr. House'),
                ('CRUD_TRANS_TST004', 'CRUD_TRANS_PAT001', 'MRI', CURRENT_DATE, CURRENT_TIME, 'Dr. House')
            """))
            
            # Insert initial test records
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results
                (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                VALUES 
                ('CRUD_TRANS_001', 'CRUD_TRANS_TST001', 85.5, 'mg/dL', '70-100', 'Final', CURRENT_DATE, CURRENT_TIME, 'Dr. Original'),
                ('CRUD_TRANS_002', 'CRUD_TRANS_TST002', 90.2, 'mg/dL', '70-100', 'Final', CURRENT_DATE, CURRENT_TIME, 'Dr. Original')
            """))
            conn.commit()
        
        # Perform multiple operations in a transaction
        with postgres_client.transaction() as conn:
            # Insert new record
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results
                (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                VALUES (
                    'CRUD_TRANS_004', 'CRUD_TRANS_TST004', 85.5, 'mg/dL', '70-100',
                    'Final', CURRENT_DATE, CURRENT_TIME, 'Dr. Brown'
                )
            """))
            
            # Update existing record
            conn.execute(text("""
                UPDATE medate_exam.lab_results
                SET reviewing_physician = 'Dr. Green'
                WHERE result_id = 'CRUD_TRANS_001'
            """))
            
            # Delete record
            conn.execute(text("""
                DELETE FROM medate_exam.lab_results
                WHERE result_id = 'CRUD_TRANS_002'
            """))
            conn.commit()
        
        # Read final state from PostgreSQL
        pg_df = postgres_client.read_table('lab_results')
        # Filter only the test records and ensure we get the latest state
        pg_df = pg_df[pg_df['result_id'].str.startswith('CRUD_TRANS')]
        
        # Write to Parquet
        s3_key = f"raw/parquet/lab_results/lab_results_{timestamp}.parquet"
        aws_client.write_parquet(pg_df, config.s3_bucket, s3_key)
        
        # Verify final state
        s3_df = aws_client.read_parquet(config.s3_bucket, s3_key)
        
        # Check insert operation
        inserted_records = s3_df[s3_df['result_id'] == 'CRUD_TRANS_004']
        validator.validate_record_exists(inserted_records, 'CRUD_TRANS_004', "CRUD_TRANS_004", "Inserted record")
        validator.validate_value_equality(
            inserted_records.iloc[0]['reviewing_physician'],
            'Dr. Brown',
            'reviewing_physician of inserted record'
        )
        
        # Check update operation
        updated_records = s3_df[s3_df['result_id'] == 'CRUD_TRANS_001']
        validator.validate_record_exists(updated_records, 'CRUD_TRANS_001', "CRUD_TRANS_001", "Updated record")
        validator.validate_value_equality(
            updated_records.iloc[0]['reviewing_physician'],
            'Dr. Green',
            'reviewing_physician of updated record'
        )
        
        # Check delete operation
        deleted_records = s3_df[s3_df['result_id'] == 'CRUD_TRANS_002']
        validator.validate_record_not_exists(deleted_records, 'CRUD_TRANS_002', "CRUD_TRANS_002", "Deleted record")

    def validate_type(self, value, expected_type, field_name):
        validator.validate_type(value, expected_type, field_name)

    def validate_required_fields(self, data, required_fields):
        validator.validate_required_fields(data, required_fields)

    def validate_range(self, value, min_value, max_value, field_name):
        validator.validate_range(value, min_value, max_value, field_name)

    def validate_string_length(self, value, min_length, max_length, field_name):
        validator.validate_string_length(value, min_length, max_length, field_name)

    def validate_date_format(self, value, format, field_name):
        validator.validate_date_format(value, format, field_name)

    def validate_dataframe_schema(self, df, schema):
        validator.validate_dataframe_schema(df, schema)

    def validate_uniqueness(self, values, field_name):
        validator.validate_uniqueness(values, field_name)

    def validate_pattern(self, value, pattern, field_name):
        validator.validate_pattern(value, pattern, field_name)

    def validate_statistics(self, data, stats_config):
        validator.validate_statistics(data, stats_config)