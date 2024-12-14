from typing import Optional
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from ..config.database import get_database_settings

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        self.settings = get_database_settings()
        
        # PostgreSQL
        self.postgres_engine = create_engine(self.settings.postgres_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.postgres_engine)
        
        # MongoDB
        self.mongo_client = MongoClient(self.settings.mongo_url)
        self.mongo_db = self.mongo_client[self.settings.MONGO_DB]
        
        # Async MongoDB
        self.async_mongo_client = AsyncIOMotorClient(self.settings.mongo_url)
        self.async_mongo_db = self.async_mongo_client[self.settings.MONGO_DB]

    def get_postgres_session(self):
        """获取PostgreSQL会话"""
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def get_mongo_collection(self, collection_name: str):
        """获取MongoDB集合"""
        return self.mongo_db[collection_name]

    def get_async_mongo_collection(self, collection_name: str):
        """获取异步MongoDB集合"""
        return self.async_mongo_db[collection_name]

    async def close_connections(self):
        """关闭所有数据库连接"""
        self.mongo_client.close()
        self.async_mongo_client.close()
        self.postgres_engine.dispose()
