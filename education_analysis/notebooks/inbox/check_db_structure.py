import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

def connect_to_mongodb():
    """连接到MongoDB数据库"""
    client = MongoClient(os.getenv('MONGODB_URI'))
    db = client[os.getenv('MONGODB_DB')]
    return db

try:
    # 连接数据库
    print("连接到数据库...")
    db = connect_to_mongodb()
    
    # 加载数据
    print("\n加载教育数据...")
    education_data = pd.DataFrame(list(db.education_data.find()))
    print("\n教育数据结构:")
    print("列名:", education_data.columns.tolist())
    print("\n前几行数据:")
    print(education_data.head())
    
    print("\n加载经济数据...")
    economic_data = pd.DataFrame(list(db.economic_indicators.find()))
    print("\n经济数据结构:")
    print("列名:", economic_data.columns.tolist())
    print("\n前几行数据:")
    print(economic_data.head())
    
except Exception as e:
    print(f"错误: {str(e)}")
    import traceback
    print(traceback.format_exc())
