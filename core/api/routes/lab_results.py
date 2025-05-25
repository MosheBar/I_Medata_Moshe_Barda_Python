"""
Lab results related API endpoints.
"""
from fastapi import APIRouter, HTTPException, Depends, Header
from typing import Optional, Dict, Any
from datetime import datetime
import time
from sqlalchemy.orm import Session

from core.api.dependencies import get_db, verify_api_key

router = APIRouter(
    prefix="/api/v1/patients/{patient_id}/lab_results",
    tags=["lab_results"]
)

@router.get("")
async def get_patient_lab_results(
    patient_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    db: Session = Depends(get_db),
    x_api_key: str = Header(...)
) -> Dict[str, Any]:
    """Get lab results for a patient."""
    # Verify API key
    if not verify_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Record start time for performance monitoring
    start_time = time.time()
    
    try:
        # First check if patient exists
        patient_result = db.execute("""
            SELECT patient_id
            FROM medate_exam.patient_information
            WHERE patient_id = :patient_id
        """, {"patient_id": patient_id})
        
        if not patient_result.fetchone():
            raise HTTPException(
                status_code=404,
                detail=f"Patient {patient_id} not found"
            )
        
        # Build query based on date filters
        query = """
            SELECT lr.*, lt.test_name
            FROM medate_exam.lab_results lr
            JOIN medate_exam.lab_tests lt ON lr.test_id = lt.test_id
            JOIN medate_exam.admissions a ON a.patient_id = :patient_id
        """
        params = {"patient_id": patient_id}
        
        if from_date:
            query += " AND lr.performed_date >= :from_date"
            params["from_date"] = from_date
            
        if to_date:
            query += " AND lr.performed_date <= :to_date"
            params["to_date"] = to_date
            
        # Execute query
        result = db.execute(query, params)
        lab_results = [dict(row) for row in result.fetchall()]
        
        # Add performance metrics
        response_time = time.time() - start_time
        return {
            "data": lab_results,
            "metadata": {
                "response_time_ms": round(response_time * 1000, 2),
                "record_count": len(lab_results)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Log the error for debugging
        print(f"Error in get_patient_lab_results: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error occurred while retrieving lab results"
        ) 