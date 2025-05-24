"""
Tests for the Medical Data API endpoints.
"""
import pytest
from datetime import datetime, timedelta
import time
import allure
from unittest.mock import MagicMock
from core.validation.base_validator import BaseValidator

validator = BaseValidator()

# Test data
VALID_API_KEY = "test_api_key"
INVALID_API_KEY = "invalid_key"
TEST_PATIENT_ID = "TEST001"
NONEXISTENT_PATIENT_ID = "NONEXISTENT"

@allure.feature("API Health Check")
class TestHealthCheck:
    @allure.story("Health Check Endpoint")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_health_check(self, client):
        """Test the health check endpoint."""
        response = client.get("/health")
        validator.validate_value_equality(response.status_code, 200, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["status", "timestamp"])
        validator.validate_value_equality(data["status"], "healthy", "Health status")

@allure.feature("Patient Information API")
class TestPatientAPI:
    @allure.story("Get Patient Details")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_get_patient_success(self, client_with_mocked_db, mock_db):
        """Test successful patient details retrieval."""
        # Mock database response
        mock_result = MagicMock()
        mock_result.fetchone.return_value = {
            "patient_id": TEST_PATIENT_ID,
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": datetime.now().date() - timedelta(days=365*30),
            "gender": "M"
        }
        mock_db.execute.return_value = mock_result
        
        response = client_with_mocked_db.get(
            f"/api/v1/patients/{TEST_PATIENT_ID}",
            headers={"X-API-Key": VALID_API_KEY}
        )
        validator.validate_value_equality(response.status_code, 200, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["data", "metadata"])
        validator.validate_required_fields(data["metadata"], ["response_time_ms"])
        validator.validate_required_fields(
            data["data"],
            ["patient_id", "first_name", "last_name", "date_of_birth", "gender"]
        )
        
        # Verify response time is within acceptable range (< 500ms)
        validator.validate_range(
            data["metadata"]["response_time_ms"],
            min_value=0,
            max_value=500,
            field_name="Response time"
        )
    
    @allure.story("Patient Not Found")
    @allure.severity(allure.severity_level.NORMAL)
    def test_get_patient_not_found(self, client_with_mocked_db, mock_db):
        """Test patient not found error handling."""
        # Mock database response for non-existent patient
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result
        
        response = client_with_mocked_db.get(
            f"/api/v1/patients/{NONEXISTENT_PATIENT_ID}",
            headers={"X-API-Key": VALID_API_KEY}
        )
        validator.validate_value_equality(response.status_code, 404, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["detail"])
        validator.validate_pattern(
            data["detail"],
            f"Patient {NONEXISTENT_PATIENT_ID} not found",
            "Error message"
        )
    
    @allure.story("Invalid API Key")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_get_patient_invalid_api_key(self, client_with_mocked_db):
        """Test invalid API key error handling."""
        response = client_with_mocked_db.get(
            f"/api/v1/patients/{TEST_PATIENT_ID}",
            headers={"X-API-Key": INVALID_API_KEY}
        )
        validator.validate_value_equality(response.status_code, 401, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["detail"])
        validator.validate_value_equality(data["detail"], "Invalid API key", "Error message")

@allure.feature("Lab Results API")
class TestLabResultsAPI:
    @allure.story("Get Patient Lab Results")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_get_lab_results_success(self, client_with_mocked_db, mock_db):
        """Test successful lab results retrieval."""
        # Mock database responses
        mock_patient_result = MagicMock()
        mock_patient_result.fetchone.return_value = {"patient_id": TEST_PATIENT_ID}
        
        mock_lab_result = MagicMock()
        mock_lab_result.fetchall.return_value = [{
            "result_id": "TEST_RES001",
            "test_id": "TEST_LAB001",
            "result_value": 85.5,
            "result_unit": "mg/dL",
            "reference_range": "70-100",
            "result_status": "Final",
            "performed_date": datetime.now().date(),
            "performed_time": datetime.now().time(),
            "reviewing_physician": "Dr. Smith",
            "test_name": "Blood Test"
        }]
        
        def mock_execute(query, params=None):
            if "patient_information" in query:
                return mock_patient_result
            return mock_lab_result
        
        mock_db.execute.side_effect = mock_execute
        
        response = client_with_mocked_db.get(
            f"/api/v1/patients/{TEST_PATIENT_ID}/lab_results",
            headers={"X-API-Key": VALID_API_KEY}
        )
        validator.validate_value_equality(response.status_code, 200, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["data", "metadata"])
        validator.validate_required_fields(
            data["metadata"],
            ["response_time_ms", "record_count"]
        )
        
        # Verify response time
        validator.validate_range(
            data["metadata"]["response_time_ms"],
            min_value=0,
            max_value=500,
            field_name="Response time"
        )
        
        # Verify lab results data
        for result in data["data"]:
            validator.validate_required_fields(
                result,
                ["result_id", "test_id", "result_value", "result_unit",
                 "reference_range", "result_status", "performed_date",
                 "performed_time", "reviewing_physician", "test_name"]
            )
    
    @allure.story("Lab Results Date Filtering")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_get_lab_results_with_date_filter(self, client_with_mocked_db, mock_db):
        """Test lab results retrieval with date filtering."""
        # Mock database responses
        mock_patient_result = MagicMock()
        mock_patient_result.fetchone.return_value = {"patient_id": TEST_PATIENT_ID}
        
        today = datetime.now().date()
        mock_lab_result = MagicMock()
        mock_lab_result.fetchall.return_value = [{
            "result_id": "TEST_RES001",
            "test_id": "TEST_LAB001",
            "result_value": 85.5,
            "result_unit": "mg/dL",
            "reference_range": "70-100",
            "result_status": "Final",
            "performed_date": today,
            "performed_time": datetime.now().time(),
            "reviewing_physician": "Dr. Smith",
            "test_name": "Blood Test"
        }]
        
        def mock_execute(query, params=None):
            if "patient_information" in query:
                return mock_patient_result
            return mock_lab_result
        
        mock_db.execute.side_effect = mock_execute
        
        from_date = (today - timedelta(days=7)).isoformat()
        to_date = today.isoformat()
        
        response = client_with_mocked_db.get(
            f"/api/v1/patients/{TEST_PATIENT_ID}/lab_results",
            params={"from_date": from_date, "to_date": to_date},
            headers={"X-API-Key": VALID_API_KEY}
        )
        validator.validate_value_equality(response.status_code, 200, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["data", "metadata"])
        
        # Verify all results are within date range
        for result in data["data"]:
            result_date = datetime.strptime(
                result["performed_date"],
                "%Y-%m-%d"
            ).date()
            validator.validate_range(
                result_date.toordinal(),
                min_value=datetime.strptime(from_date, "%Y-%m-%d").date().toordinal(),
                max_value=datetime.strptime(to_date, "%Y-%m-%d").date().toordinal(),
                field_name="Result date"
            )
    
    @allure.story("No Lab Results Found")
    @allure.severity(allure.severity_level.NORMAL)
    def test_get_lab_results_not_found(self, client_with_mocked_db, mock_db):
        """Test handling of no lab results found."""
        # Mock database responses
        mock_patient_result = MagicMock()
        mock_patient_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_patient_result
        
        response = client_with_mocked_db.get(
            f"/api/v1/patients/{NONEXISTENT_PATIENT_ID}/lab_results",
            headers={"X-API-Key": VALID_API_KEY}
        )
        validator.validate_value_equality(response.status_code, 404, "HTTP status code")
        
        data = response.json()
        validator.validate_required_fields(data, ["detail"])
        validator.validate_pattern(
            data["detail"],
            f"Patient {NONEXISTENT_PATIENT_ID} not found",
            "Error message"
        )

@allure.feature("API Performance")
class TestAPIPerformance:
    @allure.story("Response Time Under Load")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_api_performance_under_load(self, client_with_mocked_db, mock_db):
        """Test API performance under multiple sequential requests."""
        # Mock database response
        mock_result = MagicMock()
        mock_result.fetchone.return_value = {
            "patient_id": TEST_PATIENT_ID,
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": datetime.now().date() - timedelta(days=365*30),
            "gender": "M"
        }
        mock_db.execute.return_value = mock_result
        
        request_count = 10
        total_time = 0
        
        for _ in range(request_count):
            start_time = time.time()
            response = client_with_mocked_db.get(
                f"/api/v1/patients/{TEST_PATIENT_ID}",
                headers={"X-API-Key": VALID_API_KEY}
            )
            request_time = time.time() - start_time
            total_time += request_time
            
            validator.validate_value_equality(response.status_code, 200, "HTTP status code")
            validator.validate_range(
                request_time * 1000,  # Convert to milliseconds
                min_value=0,
                max_value=500,
                field_name="Individual request time"
            )
        
        average_time = (total_time / request_count) * 1000  # Convert to milliseconds
        validator.validate_range(
            average_time,
            min_value=0,
            max_value=200,
            field_name="Average response time"
        ) 