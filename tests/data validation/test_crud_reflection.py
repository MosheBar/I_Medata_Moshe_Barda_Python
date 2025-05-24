"""
Test CRUD operations reflection in PostgreSQL.
"""
import pytest
from sqlalchemy import text
import allure
from datetime import datetime, UTC

@allure.story("CRUD Reflection")
@allure.title("Test CRUD operations reflection")
@allure.severity(allure.severity_level.CRITICAL)
def test_crud_reflection(pg_engine):
    """Test CRUD operations reflection."""
    with pg_engine.connect() as conn:
        try:
            # First create a test patient
            conn.execute(text("""
                INSERT INTO medate_exam.patient_information
                (patient_id, first_name, last_name, date_of_birth, primary_physician, insurance_provider, blood_type, allergies)
                VALUES
                ('REFL_PAT001', 'John', 'Doe', CURRENT_DATE, 'Dr. House', 'Medicare', 'A+', 'None')
            """))
            
            # Then create a test lab test
            conn.execute(text("""
                INSERT INTO medate_exam.lab_tests
                (test_id, patient_id, test_name, order_date, order_time, ordering_physician)
                VALUES
                ('REFL_TST001', 'REFL_PAT001', 'Blood Test', CURRENT_DATE, CURRENT_TIME, 'Dr. House')
            """))
            
            # Finally create the lab result
            conn.execute(text("""
                INSERT INTO medate_exam.lab_results
                (result_id, test_id, result_value, result_unit, reference_range, result_status, performed_date, performed_time, reviewing_physician)
                VALUES
                ('REFL_RES001', 'REFL_TST001', 85.5, 'mg/dL', '70-100', 'Final', CURRENT_DATE, CURRENT_TIME, 'Lab Tech 1')
            """))
            
            # Verify data was inserted
            result = conn.execute(text("""
                SELECT r.result_id, r.test_id, r.result_value, t.patient_id
                FROM medate_exam.lab_results r
                JOIN medate_exam.lab_tests t ON r.test_id = t.test_id
                WHERE r.result_id = 'REFL_RES001'
            """))
            row = result.fetchone()
            
            # Verify data integrity
            assert row is not None, "Result not found"
            assert row.result_id == 'REFL_RES001', "Wrong result_id"
            assert row.test_id == 'REFL_TST001', "Wrong test_id"
            assert row.result_value == 85.5, "Wrong result_value"
            assert row.patient_id == 'REFL_PAT001', "Wrong patient_id"
            
            conn.commit()
            
        finally:
            # Clean up in reverse order
            conn.execute(text("DELETE FROM medate_exam.lab_results WHERE result_id = 'REFL_RES001'"))
            conn.execute(text("DELETE FROM medate_exam.lab_tests WHERE test_id = 'REFL_TST001'"))
            conn.execute(text("DELETE FROM medate_exam.patient_information WHERE patient_id = 'REFL_PAT001'"))
            conn.commit()
