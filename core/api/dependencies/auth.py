"""
Authentication and authorization dependencies.
"""
from fastapi import Header

def verify_api_key(x_api_key: str = Header(...)) -> bool:
    """Verify API key from header."""
    # In production, this should be a secure comparison against stored keys
    return x_api_key == "test_api_key" 