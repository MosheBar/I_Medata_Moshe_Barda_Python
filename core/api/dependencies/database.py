"""
Database connection and dependencies.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os

# Use test database if running tests
if os.getenv("TESTING") == "true":
    POSTGRES_URL = "postgresql://postgres:postgres@localhost:5432/medical_test_db"
else:
    from config.config import config
    POSTGRES_URL = config.postgres_url

# Create database engine
engine = create_engine(POSTGRES_URL)

def get_db():
    """Get database session."""
    db = Session(engine)
    try:
        yield db
    finally:
        db.close() 