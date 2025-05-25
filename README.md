# Medical Data API Tests

This repository contains automated tests for the Medical Data API endpoints. The tests cover functionality, performance, and error handling for patient information and lab results retrieval.

## Prerequisites

- Python 3.13+
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
POSTGRES_HOST=your_host
POSTGRES_PORT=5432
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=us-east-1
S3_BUCKET=your_bucket_name
```

2. Update the database and AWS credentials in the `.env` file with your values.

## Database Configuration

The project uses SQLAlchemy 2.0+ with psycopg v3 as the PostgreSQL driver. The database URL is automatically constructed using the environment variables above in the following format:

```python
postgresql+psycopg://{user}:{password}@{host}:{port}/{database}
```

## Project Structure

```
.
├── core/                      # Core application code
│   ├── api/                  # API implementation
│   │   └── main.py          # Main API endpoints
│   ├── aws/                 # AWS service integrations
│   ├── db/                  # Database models and operations
│   ├── entities/            # Business logic entities
│   └── validation/         # Data validation logic
│
├── config/                   # Configuration files
│   └── config.py           # Main configuration settings
│
├── tests/                    # Test suites
│   ├── api/                # API tests
│   └── data validation/    # Data validation tests
│
├── external/                 # External integrations
│
├── pages/                    # UI page objects (if applicable)
│
├── .env                      # Environment variables (not in repo)
├── conftest.py              # Global test configuration
├── pytest.ini               # Pytest configuration
├── requirements.txt         # Project dependencies
├── setup.py                 # Package setup file
└── README.md                # Project documentation
```

### Directory Descriptions

- **core/**: Contains the FastAPI application and API-related code
- **aws/**: AWS service integrations and utilities
- **config/**: Configuration management and settings
- **db/**: Database models, migrations, and operations
- **entities/**: Core business logic and domain entities
- **validation/**: Data validation and verification utilities

### Additional Directories (Development)

- **.github/**: GitHub Actions and workflows
- **.venv/**: Python virtual environment (not in repo)
- **.vscode/**: VS Code settings (optional)
- **allure-results/**: Test report data (not in repo)
- **temp/**: Temporary files (not in repo)
- **zInnerTest/**: Internal testing utilities
- **zOthers/**: Miscellaneous development resources

## Running the Tests

### Running All Tests
To run all tests with detailed output:
```bash
pytest -v
```

To run tests with parallel execution (faster):
```bash
pytest -v -n auto
```

To run tests with coverage report:
```bash
pytest -v --cov=. --cov-report=html
```

To run tests with HTML report:
```bash
pytest -v --html=report.html
```

### Running Tests by Directory
- Run API tests:
```bash
pytest tests/api -v
```

- Run data validation tests:
```bash
pytest tests/data_validation -v
```

### Running Tests with Allure Reports
1. Run tests and generate Allure results:
```bash
pytest --alluredir=./allure-results
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
- Database connectivity check
- AWS S3 connectivity check

### 2. Patient Information
- Successful patient details retrieval
- Patient not found handling
- Data validation and integrity checks
- Response time validation

### 3. Lab Results
- Successful lab results retrieval
- Date range filtering
- No results found handling
- Response format validation
- Data integrity checks
- S3 file storage validation

### 4. Performance
- Response time under load
- Average response time validation
- Individual request time validation
- S3 operation performance metrics

### 5. Data Validation
- Schema validation
- Data type verification
- Required field checks
- Cross-reference validation
- S3 data consistency checks

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
   - Make sure you're using Python 3.13+ (required for current dependencies)
   - Verify SQLAlchemy 2.0+ and psycopg v3 are properly installed

2. Slow Response Times
   - Check database indexes
   - Verify network connectivity
   - Monitor database performance
   - Check AWS connectivity for S3 operations

3. Authentication Failures
   - Verify AWS credentials in `.env`
   - Check database user permissions
   - Ensure all required environment variables are set

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

Copyright (c) 2024 Moshe Barda. All rights reserved.

This project and its contents are proprietary and confidential. Unauthorized copying, transfer, or reproduction of the contents of this project, via any medium, is strictly prohibited.