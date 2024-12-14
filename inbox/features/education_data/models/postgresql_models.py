from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Country(Base):
    """国家信息表"""
    __tablename__ = 'countries'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    code = Column(String(3), unique=True, nullable=False)
    region = Column(String(50))
    
    # 关联
    education_data = relationship("EducationData", back_populates="country")

class EducationData(Base):
    """教育指标数据表"""
    __tablename__ = 'education_data'

    id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    year = Column(Integer, nullable=False)
    education_investment = Column(Float)
    student_teacher_ratio = Column(Float)
    completion_rate = Column(Float)
    literacy_rate = Column(Float)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # 关联
    country = relationship("Country", back_populates="education_data")

class DataUpdateLog(Base):
    """数据更新日志表"""
    __tablename__ = 'data_update_logs'

    id = Column(Integer, primary_key=True)
    table_name = Column(String(50), nullable=False)
    operation = Column(String(20), nullable=False)  # INSERT, UPDATE, DELETE
    record_id = Column(Integer)
    changes = Column(String)
    timestamp = Column(DateTime, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))

class User(Base):
    """用户表"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False)
    last_login = Column(DateTime)
