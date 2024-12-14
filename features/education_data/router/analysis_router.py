"""
Education Data Analysis Router
This module provides API endpoints for education data analysis and visualization.
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import os
from pymongo import MongoClient

from features.education_data.analysis.service.analysis_service import EducationAnalysisService
from features.education_data.visualization.service.visualization_service import EducationVisualizationService

# MongoDB configuration
MONGODB_HOST = os.getenv('MONGODB_HOST')
MONGODB_PORT = int(os.getenv('MONGODB_PORT'))
MONGODB_USER = os.getenv('MONGODB_USER')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')
MONGODB_DB = os.getenv('MONGODB_DB')

# Create MongoDB client
client = MongoClient(
    host=MONGODB_HOST,
    port=MONGODB_PORT,
    username=MONGODB_USER,
    password=MONGODB_PASSWORD,
    authSource='admin'
)
db = client[MONGODB_DB]

router = APIRouter(prefix="/api/education", tags=["education"])

@router.get("/analysis/investment")
async def analyze_education_investment() -> Dict[str, Any]:
    """
    Analyze education investment data and return results
    """
    try:
        analysis_service = EducationAnalysisService()
        return await analysis_service.analyze_education_investment()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysis/quality")
async def analyze_education_quality() -> Dict[str, Any]:
    """
    Analyze education quality indicators
    """
    try:
        analysis_service = EducationAnalysisService()
        return await analysis_service.analyze_education_quality()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysis/resources")
async def analyze_resource_allocation() -> Dict[str, Any]:
    """
    Analyze education resource allocation patterns
    """
    try:
        analysis_service = EducationAnalysisService()
        return await analysis_service.analyze_resource_allocation()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysis/outcomes")
async def analyze_education_outcomes() -> Dict[str, Any]:
    """
    Analyze education outcomes and their relationships with various factors
    """
    try:
        analysis_service = EducationAnalysisService()
        return await analysis_service.analyze_education_outcomes()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysis/forecast")
async def forecast_education_metrics() -> Dict[str, Any]:
    """
    Perform time series forecasting on key education metrics
    """
    try:
        analysis_service = EducationAnalysisService()
        return await analysis_service.forecast_education_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visualization/investment-trends")
async def get_investment_trends_visualization() -> Dict[str, Any]:
    """
    Generate visualization for education investment trends
    """
    try:
        viz_service = EducationVisualizationService()
        return await viz_service.create_investment_trends_visualization()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visualization/correlation-heatmap")
async def get_correlation_heatmap() -> Dict[str, Any]:
    """
    Generate correlation heatmap visualization
    """
    try:
        viz_service = EducationVisualizationService()
        return await viz_service.create_correlation_heatmap()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visualization/country-rankings")
async def get_country_rankings_chart() -> Dict[str, Any]:
    """
    Generate country rankings visualization
    """
    try:
        viz_service = EducationVisualizationService()
        return await viz_service.create_country_rankings_chart()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
