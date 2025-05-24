# Medical Data API Tests

This repository contains automated tests for the Medical Data API endpoints. The tests cover functionality, performance, and error handling for patient information and lab results retrieval.

## Prerequisites

- Python 3.8+
- PostgreSQL database with medical data schema
- pip (Python package installer)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project root with the following variables:
```env
POSTGRES_URL=postgresql://username:password@localhost:5432/medical_db
TEST_API_KEY=test_api_key
```

2. Update the database connection details in the `.env` file with your PostgreSQL credentials.

## Project Structure

```
.
├── api/
│   └── main.py                 # FastAPI application
├── tests/
│   ├── api/                    # API test directory
│   │   ├── conftest.py        # API test fixtures
│   │   └── test_api.py        # API test cases
│   └── data_validation/       # Data validation test directory
├── config/
│   └── config.py              # Configuration settings
├── validation/
│   └── base_validator.py      # Validation utilities
├── requirements.txt           # Project dependencies
└── README.md                 # Project documentation
```

## Running the Tests

### Running All Tests
To run all tests with detailed output:
```bash
pytest tests/test_api.py -v
```

### Running Tests with Allure Reports
1. Run tests and generate Allure results:
```bash
pytest tests/api/test_api.py --alluredir=./allure-results
```

2. Generate and open the Allure report:
```bash
allure serve ./allure-results
```

### Running Specific Test Categories
- Run only health check tests:
```bash
pytest tests/api/test_api.py -v -k "TestHealthCheck"
```

- Run only patient information tests:
```bash
pytest tests/api/test_api.py -v -k "TestPatientAPI"
```

- Run only lab results tests:
```bash
pytest tests/api/test_api.py -v -k "TestLabResultsAPI"
```

- Run only performance tests:
```bash
pytest tests/api/test_api.py -v -k "TestAPIPerformance"
```

## Test Coverage

The test suite covers the following areas:

### 1. Health Check
- Basic API health check endpoint
- Response format validation

### 2. Patient Information
- Successful patient details retrieval
- Patient not found handling
- Invalid API key handling
- Response time validation

### 3. Lab Results
- Successful lab results retrieval
- Date range filtering
- No results found handling
- Response format validation
- Data integrity checks

### 4. Performance
- Response time under load
- Average response time validation
- Individual request time validation

## Test Results Interpretation

### Success Criteria
- All tests pass with HTTP status codes matching expected values
- Response times are within acceptable ranges:
  - Individual requests: < 500ms
  - Average response time: < 200ms
- Data integrity checks pass for all responses
- Error handling works as expected

### Common Issues and Solutions
1. Database Connection Errors
   - Verify PostgreSQL is running
   - Check database credentials in `.env`
   - Ensure medical data schema exists

2. Slow Response Times
   - Check database indexes
   - Verify network connectivity
   - Monitor database performance

3. Authentication Failures
   - Verify API key in `.env`
   - Check API key header in requests
   - Ensure API key validation is working

## API Endpoints

### 1. Health Check
```
GET /health
```
Returns the API health status and current timestamp.

### 2. Get Patient Details
```
GET /api/v1/patients/{patient_id}
Headers: X-API-Key
```
Returns patient information and response time metrics.

### 3. Get Patient Lab Results
```
GET /api/v1/patients/{patient_id}/lab_results
Headers: X-API-Key
Query Parameters: from_date, to_date
```
Returns lab results for a patient with optional date filtering.

## Contributing

1. Create a feature branch
2. Add or modify tests
3. Ensure all tests pass
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.