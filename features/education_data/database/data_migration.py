import pandas as pd
from datetime import datetime
from typing import Dict, Any
from sqlalchemy.orm import Session
from pymongo.collection import Collection

from ..models.postgresql_models import Country, EducationData, Base
from .db_manager import DatabaseManager

class DataMigration:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def init_postgres_tables(self):
        """初始化PostgreSQL表"""
        Base.metadata.create_all(bind=self.db_manager.postgres_engine)
    
    def migrate_csv_to_postgres(self, csv_path: str, session: Session):
        """将CSV数据迁移到PostgreSQL"""
        df = pd.read_csv(csv_path)
        
        # 添加国家信息
        for country_name in df['country'].unique():
            country = Country(
                name=country_name,
                code=country_name[:3].upper(),  # 简单示例，实际应该使用标准国家代码
                region='Europe'  # 示例数据都是欧洲国家
            )
            session.add(country)
        session.commit()
        
        # 添加教育数据
        for _, row in df.iterrows():
            country = session.query(Country).filter_by(name=row['country']).first()
            education_data = EducationData(
                country_id=country.id,
                year=pd.to_datetime(row['year']).year,
                education_investment=row['education_investment'],
                student_teacher_ratio=row['student_teacher_ratio'],
                completion_rate=row['completion_rate'],
                literacy_rate=row['literacy_rate'],
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(education_data)
        session.commit()
    
    def store_raw_data_mongodb(self, csv_path: str, collection: Collection):
        """将原始数据存储到MongoDB"""
        df = pd.read_csv(csv_path)
        raw_data = {
            'source': 'csv_import',
            'collection_date': datetime.now(),
            'data': df.to_dict(orient='records'),
            'metadata': {
                'columns': list(df.columns),
                'row_count': len(df),
                'file_path': csv_path
            }
        }
        collection.insert_one(raw_data)

def main():
    # 初始化数据库管理器
    db_manager = DatabaseManager()
    migration = DataMigration(db_manager)
    
    # 初始化PostgreSQL表
    migration.init_postgres_tables()
    
    # 迁移数据到PostgreSQL
    csv_path = '../data/processed/education_data.csv'
    with next(db_manager.get_postgres_session()) as session:
        migration.migrate_csv_to_postgres(csv_path, session)
    
    # 存储原始数据到MongoDB
    collection = db_manager.get_mongo_collection('raw_education_data')
    migration.store_raw_data_mongodb(csv_path, collection)

if __name__ == '__main__':
    main()
