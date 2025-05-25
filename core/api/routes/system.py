"""
System-related API endpoints.
"""
from fastapi import APIRouter
from datetime import datetime

router = APIRouter(
    prefix="",  # Explicitly set empty prefix for root level
    tags=["system"]
)

@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()} 