"""
FastAPI application configuration and initialization.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import main_router

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Medical Data API",
        description="API endpoints for accessing medical data",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json"
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, replace with specific origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include all routes
    app.include_router(main_router)

    return app

# Create the application instance
app = create_app() 