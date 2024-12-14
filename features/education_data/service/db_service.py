from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from pymongo.collection import Collection
from motor.motor_asyncio import AsyncIOMotorCollection

from ..models.postgresql_models import Country, EducationData
from ..database.db_manager import DatabaseManager

class DatabaseService:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def store_raw_data(self, data: pd.DataFrame, collection: AsyncIOMotorCollection) -> str:
        """存储原始数据到MongoDB"""
        raw_data = {
            "source": "data_import",
            "timestamp": datetime.now(),
            "data": data.to_dict(orient="records"),
            "metadata": {
                "columns": list(data.columns),
                "row_count": len(data),
                "data_types": {col: str(dtype) for col, dtype in data.dtypes.items()}
            }
        }
        result = await collection.insert_one(raw_data)
        return str(result.inserted_id)
    
    def store_processed_data(self, data: pd.DataFrame, session: Session) -> None:
        """存储处理后的数据到PostgreSQL"""
        # 存储国家信息
        for country_name in data["country"].unique():
            country = Country(
                name=country_name,
                code=country_name[:3].upper(),
                region="Europe"  # 示例数据
            )
            session.merge(country)
        session.commit()
        
        # 存储教育数据
        for _, row in data.iterrows():
            country = session.query(Country).filter_by(name=row["country"]).first()
            education_data = EducationData(
                country_id=country.id,
                year=pd.to_datetime(row["year"]).year,
                education_investment=row["education_investment"],
                student_teacher_ratio=row["student_teacher_ratio"],
                completion_rate=row["completion_rate"],
                literacy_rate=row["literacy_rate"],
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(education_data)
        session.commit()
    
    async def store_analysis_report(self, report: Dict[str, Any], collection: AsyncIOMotorCollection) -> str:
        """存储分析报告到MongoDB"""
        report_doc = {
            "timestamp": datetime.now(),
            "type": "education_analysis",
            "content": report
        }
        result = await collection.insert_one(report_doc)
        return str(result.inserted_id)
    
    def get_education_data(self, session: Session, country_name: Optional[str] = None,
                          start_year: Optional[int] = None, end_year: Optional[int] = None) -> List[Dict[str, Any]]:
        """从PostgreSQL获取教育数据"""
        query = session.query(EducationData).join(Country)
        
        if country_name:
            query = query.filter(Country.name == country_name)
        if start_year:
            query = query.filter(EducationData.year >= start_year)
        if end_year:
            query = query.filter(EducationData.year <= end_year)
        
        results = []
        for data in query.all():
            country = session.query(Country).get(data.country_id)
            results.append({
                "country": country.name,
                "year": data.year,
                "education_investment": data.education_investment,
                "student_teacher_ratio": data.student_teacher_ratio,
                "completion_rate": data.completion_rate,
                "literacy_rate": data.literacy_rate
            })
        
        return results
    
    async def get_analysis_reports(self, collection: AsyncIOMotorCollection,
                                 report_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """从MongoDB获取分析报告"""
        query = {"type": report_type} if report_type else {}
        cursor = collection.find(query).sort("timestamp", -1)
        return await cursor.to_list(length=None)
