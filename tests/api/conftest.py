"""
Pytest configuration and fixtures for API testing.
"""
import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from core.api.main import app, get_db

@pytest.fixture(scope="session")
def client() -> TestClient:
    """Create a FastAPI test client."""
    return TestClient(app)

@pytest.fixture
def mock_db():
    """Create a mock database session."""
    mock_session = MagicMock()
    return mock_session

@pytest.fixture
def client_with_mocked_db(client, mock_db):
    """Create a test client with mocked database."""
    app.dependency_overrides[get_db] = lambda: mock_db
    yield client
    app.dependency_overrides = {}
