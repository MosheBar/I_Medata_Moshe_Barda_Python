"""
API routes package.
"""
from fastapi import APIRouter

# Import routers
from .lab_results import router as lab_results_router
from .patients import router as patients_router
from .system import router as system_router

# Create main router without a prefix
main_router = APIRouter()

# Include all route modules
# System router at root level
main_router.include_router(system_router)

# API v1 routes
main_router.include_router(patients_router)
main_router.include_router(lab_results_router) 