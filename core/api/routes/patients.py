"""
Patient information related API endpoints.
"""
from fastapi import APIRouter, HTTPException, Depends, Header
from typing import Dict, Any
from datetime import datetime
import time
from sqlalchemy.orm import Session

from core.api.dependencies import get_db, verify_api_key

router = APIRouter(
    prefix="/api/v1/patients",
    tags=["patients"]
)

@router.get("/{patient_id}")
async def get_patient(
    patient_id: str,
    db: Session = Depends(get_db),
    x_api_key: str = Header(...)
) -> Dict[str, Any]:
    """Get patient details by ID."""
    # Verify API key
    if not verify_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Record start time for performance monitoring
    start_time = time.time()
    
    try:
        # Query patient information
        result = db.execute("""
            SELECT *
            FROM medate_exam.patient_information
            WHERE patient_id = :patient_id
        """, {"patient_id": patient_id})
        
        patient = result.fetchone()
        if not patient:
            raise HTTPException(
                status_code=404,
                detail=f"Patient {patient_id} not found"
            )
        
        # Convert to dictionary
        patient_dict = dict(patient)
        
        # Add performance metrics
        response_time = time.time() - start_time
        return {
            "data": patient_dict,
            "metadata": {
                "response_time_ms": round(response_time * 1000, 2)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Log the error for debugging
        print(f"Error in get_patient: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error occurred while retrieving patient data"
        ) 