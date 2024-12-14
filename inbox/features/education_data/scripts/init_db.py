import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from ..config.database import get_database_settings
from ..models.postgresql_models import Base
from ..database.db_manager import DatabaseManager

async def init_databases():
    """初始化数据库"""
    settings = get_database_settings()
    db_manager = DatabaseManager()
    
    # 初始化PostgreSQL
    print("Initializing PostgreSQL...")
    Base.metadata.create_all(bind=db_manager.postgres_engine)
    print("PostgreSQL tables created successfully!")
    
    # 初始化MongoDB集合
    print("Initializing MongoDB...")
    collections = [
        "raw_education_data",
        "validation_reports",
        "analysis_reports",
        "visualization_configs"
    ]
    
    for collection in collections:
        await db_manager.async_mongo_db.create_collection(collection)
        print(f"MongoDB collection '{collection}' created successfully!")
    
    await db_manager.close_connections()
    print("Database initialization completed!")

if __name__ == "__main__":
    asyncio.run(init_databases())
