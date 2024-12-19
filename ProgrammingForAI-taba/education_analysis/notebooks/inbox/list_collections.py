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
    
    # 列出所有集合
    print("\n数据库中的集合:")
    collections = db.list_collection_names()
    for collection in collections:
        print(f"- {collection}")
        # 显示每个集合的一个文档示例
        doc = db[collection].find_one()
        if doc:
            print("  示例文档:")
            print(f"  {doc}")
        else:
            print("  集合为空")
        print()
    
except Exception as e:
    print(f"错误: {str(e)}")
    import traceback
    print(traceback.format_exc())
