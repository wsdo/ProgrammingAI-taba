from typing import Optional, Dict, Any
import os
from functools import lru_cache
from pydantic_settings import BaseSettings

class DatabaseSettings(BaseSettings):
    """数据库配置"""
    
    # PostgreSQL
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "your_password"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "education_db"
    
    # MongoDB
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_DB: str = "education_db"
    
    @property
    def postgres_url(self) -> str:
        """获取PostgreSQL连接URL"""
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    @property
    def mongo_url(self) -> str:
        """获取MongoDB连接URL"""
        return f"mongodb://{self.MONGO_HOST}:{self.MONGO_PORT}"
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_database_settings() -> DatabaseSettings:
    """获取数据库配置单例"""
    return DatabaseSettings()
