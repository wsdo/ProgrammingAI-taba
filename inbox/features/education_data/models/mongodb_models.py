from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

class RawEducationData(BaseModel):
    """原始教育数据模型"""
    source: str
    collection_date: datetime
    data: Dict[str, Any]
    metadata: Dict[str, Any]

class ValidationReport(BaseModel):
    """数据验证报告模型"""
    data_id: str
    validation_date: datetime
    validation_results: List[Dict[str, Any]]
    issues_found: int
    is_valid: bool

class AnalysisReport(BaseModel):
    """分析报告模型"""
    report_id: str
    creation_date: datetime
    analysis_type: str
    results: Dict[str, Any]
    metadata: Dict[str, Any]

class VisualizationConfig(BaseModel):
    """可视化配置模型"""
    config_id: str
    name: str
    chart_type: str
    data_source: str
    configuration: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
