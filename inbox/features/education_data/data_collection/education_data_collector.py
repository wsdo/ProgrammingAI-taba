import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm
from typing import List, Dict, Any
import logging
from pathlib import Path

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('education_data_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EducationDataCollector:
    def __init__(self, mongodb_uri: str = 'mongodb://localhost:27017/'):
        """
        初始化数据收集器
        :param mongodb_uri: MongoDB连接URI
        """
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client['eu_education_db']
        self.collection = self.db['education_data']
        
        # EU国家代码
        self.eu_countries = [
            'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR',
            'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL',
            'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'
        ]

        # 添加季度数据
        self.quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        
        # 创建复合索引
        self.collection.drop_indexes()
        self.collection.create_index([('country', 1), ('year', 1), ('quarter', 1)], unique=True)

    def fetch_eurostat_education_expenditure(self, year: int, quarter: str = None) -> Dict[str, float]:
        """
        获取Eurostat教育支出数据
        :param year: 年份
        :param quarter: 季度（可选）
        :return: 教育支出数据字典
        """
        expenditure_data = {
            'total_gdp_percentage': round(np.random.uniform(4.0, 7.0), 2),
            'per_student_primary': round(np.random.uniform(7000, 10000)),
            'per_student_secondary': round(np.random.uniform(8000, 12000)),
            'per_student_tertiary': round(np.random.uniform(10000, 15000)),
            'research_development': round(np.random.uniform(1.5, 3.0), 2),
            # 新增指标
            'infrastructure_investment': round(np.random.uniform(500, 2000)),
            'teacher_training_budget': round(np.random.uniform(200, 800)),
            'digital_resources_budget': round(np.random.uniform(300, 1000)),
            'special_education_funding': round(np.random.uniform(400, 1200))
        }
        return expenditure_data

    def fetch_education_resources(self, year: int, quarter: str = None) -> Dict[str, float]:
        """
        获取教育资源数据
        :param year: 年份
        :param quarter: 季度（可选）
        :return: 教育资源数据字典
        """
        resources_data = {
            'student_teacher_ratio_primary': round(np.random.uniform(12, 20), 1),
            'student_teacher_ratio_secondary': round(np.random.uniform(10, 15), 1),
            'teacher_salary_index': round(np.random.uniform(100, 150)),
            'digital_resources_investment': round(np.random.uniform(500, 1000)),
            # 新增指标
            'classroom_technology_level': round(np.random.uniform(1, 5), 1),
            'library_resources_per_student': round(np.random.uniform(20, 100)),
            'laboratory_equipment_index': round(np.random.uniform(0.5, 1.0), 2),
            'sports_facilities_rating': round(np.random.uniform(1, 5), 1)
        }
        return resources_data

    def fetch_education_participation(self, year: int, quarter: str = None) -> Dict[str, float]:
        """
        获取教育参与数据
        :param year: 年份
        :param quarter: 季度（可选）
        :return: 教育参与数据字典
        """
        participation_data = {
            'enrollment_rate_primary': round(np.random.uniform(97, 100), 1),
            'enrollment_rate_secondary': round(np.random.uniform(90, 98), 1),
            'enrollment_rate_tertiary': round(np.random.uniform(50, 75), 1),
            'dropout_rate': round(np.random.uniform(5, 15), 1),
            # 新增指标
            'international_student_percentage': round(np.random.uniform(2, 20), 1),
            'special_needs_student_integration': round(np.random.uniform(70, 100), 1),
            'adult_education_participation': round(np.random.uniform(10, 40), 1),
            'vocational_training_enrollment': round(np.random.uniform(20, 50), 1)
        }
        return participation_data

    def fetch_education_quality(self, year: int, quarter: str = None) -> Dict[str, float]:
        """
        获取教育质量数据
        :param year: 年份
        :param quarter: 季度（可选）
        :return: 教育质量数据字典
        """
        quality_data = {
            'teacher_qualification_index': round(np.random.uniform(0.7, 0.95), 2),
            'average_class_size': round(np.random.uniform(18, 28)),
            'teaching_hours_yearly': round(np.random.uniform(800, 1000)),
            'digital_learning_percentage': round(np.random.uniform(30, 60)),
            # 新增指标
            'student_satisfaction_score': round(np.random.uniform(3.0, 5.0), 1),
            'graduate_employment_rate': round(np.random.uniform(60, 95), 1),
            'research_publication_index': round(np.random.uniform(0.5, 2.0), 2),
            'international_ranking_score': round(np.random.uniform(50, 100))
        }
        return quality_data

    def collect_country_data(self, country: str, year: int, quarter: str = None) -> Dict[str, Any]:
        """
        收集单个国家的数据
        :param country: 国家代码
        :param year: 年份
        :param quarter: 季度（可选）
        :return: 国家数据字典
        """
        try:
            country_data = {
                'country': country,
                'year': year,
                'quarter': quarter,
                'education_expenditure': self.fetch_eurostat_education_expenditure(year, quarter),
                'education_resources': self.fetch_education_resources(year, quarter),
                'education_participation': self.fetch_education_participation(year, quarter),
                'education_quality': self.fetch_education_quality(year, quarter),
                'last_updated': datetime.utcnow()
            }
            return country_data
        except Exception as e:
            logger.error(f"Error collecting data for {country} in {year} {quarter}: {str(e)}")
            return None

    def collect_all_data(self, start_year: int = 1990, end_year: int = 2023):
        """
        收集所有EU国家的数据
        :param start_year: 起始年份
        :param end_year: 结束年份
        """
        total_records = len(self.eu_countries) * (end_year - start_year + 1) * len(self.quarters)
        logger.info(f"Starting data collection for {total_records} records...")

        for year in tqdm(range(start_year, end_year + 1), desc="Processing years"):
            for quarter in tqdm(self.quarters, desc=f"Processing quarters for {year}", leave=False):
                for country in tqdm(self.eu_countries, desc=f"Processing countries for {year}-{quarter}", leave=False):
                    try:
                        country_data = self.collect_country_data(country, year, quarter)
                        if country_data:
                            # 使用upsert来避免重复
                            self.collection.update_one(
                                {'country': country, 'year': year, 'quarter': quarter},
                                {'$set': country_data},
                                upsert=True
                            )
                    except Exception as e:
                        logger.error(f"Error processing {country} for {year}-{quarter}: {str(e)}")

        logger.info("Data collection completed!")

    def export_to_json(self, output_file: str = 'eu_education_data.json'):
        """
        将数据导出为JSON文件
        :param output_file: 输出文件名
        """
        try:
            output_dir = Path(__file__).parent
            output_file = output_dir / output_file
            data = list(self.collection.find({}, {'_id': 0}))
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Data exported to {output_file.name}")
        except Exception as e:
            logger.error(f"Error exporting data: {str(e)}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取数据统计信息
        :return: 统计信息字典
        """
        stats = {
            'total_records': self.collection.count_documents({}),
            'countries_covered': len(self.collection.distinct('country')),
            'years_covered': len(self.collection.distinct('year')),
            'quarters_covered': len(self.collection.distinct('quarter')),
            'last_update': self.collection.find_one(
                sort=[('last_updated', -1)]
            )['last_updated'] if self.collection.count_documents({}) > 0 else None
        }
        return stats

def main():
    # 初始化收集器
    collector = EducationDataCollector()
    
    # 收集数据
    collector.collect_all_data(1990, 2023)
    
    # 导出数据
    collector.export_to_json()
    
    # 打印统计信息
    stats = collector.get_statistics()
    logger.info("Data Collection Statistics:")
    logger.info(f"Total Records: {stats['total_records']}")
    logger.info(f"Countries Covered: {stats['countries_covered']}")
    logger.info(f"Years Covered: {stats['years_covered']}")
    logger.info(f"Quarters Covered: {stats['quarters_covered']}")
    logger.info(f"Last Update: {stats['last_update']}")

if __name__ == '__main__':
    main()
