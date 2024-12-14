import asyncio
import pandas as pd
from datetime import datetime

from ..database.db_manager import DatabaseManager
from ..service.db_service import DatabaseService

async def test_database_integration():
    """测试数据库集成"""
    # 创建示例数据
    data = pd.DataFrame({
        "country": ["France", "Germany", "Spain"],
        "year": [2020, 2020, 2020],
        "education_investment": [5.5, 4.9, 4.3],
        "student_teacher_ratio": [15.2, 12.8, 13.5],
        "completion_rate": [95.5, 96.2, 93.8],
        "literacy_rate": [99.0, 99.2, 98.5]
    })
    
    # 初始化数据库管理器和服务
    db_manager = DatabaseManager()
    db_service = DatabaseService(db_manager)
    
    try:
        # 存储原始数据到MongoDB
        print("Storing raw data in MongoDB...")
        mongo_collection = db_manager.get_async_mongo_collection("raw_education_data")
        raw_data_id = await db_service.store_raw_data(data, mongo_collection)
        print(f"Raw data stored with ID: {raw_data_id}")
        
        # 存储处理后的数据到PostgreSQL
        print("\nStoring processed data in PostgreSQL...")
        with next(db_manager.get_postgres_session()) as session:
            db_service.store_processed_data(data, session)
        print("Processed data stored successfully!")
        
        # 从PostgreSQL检索数据
        print("\nRetrieving data from PostgreSQL...")
        with next(db_manager.get_postgres_session()) as session:
            results = db_service.get_education_data(session)
            print("Retrieved education data:")
            for result in results:
                print(f"  {result['country']} ({result['year']}): "
                      f"Investment: {result['education_investment']}%, "
                      f"Student-Teacher Ratio: {result['student_teacher_ratio']}")
        
        # 存储分析报告到MongoDB
        print("\nStoring analysis report in MongoDB...")
        report = {
            "analysis_type": "basic_statistics",
            "timestamp": datetime.now().isoformat(),
            "metrics": {
                "avg_investment": data["education_investment"].mean(),
                "avg_completion_rate": data["completion_rate"].mean(),
                "countries_analyzed": len(data["country"].unique())
            }
        }
        
        analysis_collection = db_manager.get_async_mongo_collection("analysis_reports")
        report_id = await db_service.store_analysis_report(report, analysis_collection)
        print(f"Analysis report stored with ID: {report_id}")
        
        # 检索分析报告
        print("\nRetrieving analysis reports from MongoDB...")
        reports = await db_service.get_analysis_reports(analysis_collection, "education_analysis")
        print(f"Retrieved {len(reports)} analysis reports")
        
    finally:
        # 关闭数据库连接
        await db_manager.close_connections()
        print("\nDatabase connections closed.")

if __name__ == "__main__":
    asyncio.run(test_database_integration())
