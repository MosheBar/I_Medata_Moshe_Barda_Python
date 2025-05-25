"""
Pytest configuration and fixtures for API testing.
"""
import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from core.api.app import app
from core.api.dependencies.database import get_db

@pytest.fixture(scope="session")
def client() -> TestClient:
    """Create a FastAPI test client."""
    # Create a fresh test client for each session
    test_client = TestClient(app)
    return test_client

@pytest.fixture
def mock_db():
    """Create a mock database session."""
    mock_session = MagicMock()
    return mock_session

@pytest.fixture
def client_with_mocked_db(mock_db):
    """Create a test client with mocked database."""
    # Create a fresh test client for each test
    test_client = TestClient(app)
    
    # Override the database dependency
    app.dependency_overrides[get_db] = lambda: mock_db
    
    yield test_client
    
    # Clean up after test
    app.dependency_overrides = {}
