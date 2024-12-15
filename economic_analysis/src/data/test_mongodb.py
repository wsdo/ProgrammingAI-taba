"""
Test MongoDB Connection
"""
from pymongo import MongoClient
from dotenv import load_dotenv
import os

def test_mongodb_connection():
    """Test connection to MongoDB"""
    load_dotenv()
    
    # Print environment variables for debugging
    print("MongoDB Environment Variables:")
    print(f"MONGODB_USER: {os.getenv('MONGODB_USER')}")
    print(f"MONGODB_HOST: {os.getenv('MONGODB_HOST')}")
    print(f"MONGODB_PORT: {os.getenv('MONGODB_PORT')}")
    print(f"MONGODB_DB: {os.getenv('MONGODB_DB')}")
    
    # MongoDB connection string with authSource parameter
    mongo_uri = (
        f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
        f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
        "?authSource=admin"  # 使用 admin 作为认证数据库
    )
    
    print(f"\nTrying to connect with URI: {mongo_uri}")
    
    try:
        # Create a MongoDB client
        client = MongoClient(mongo_uri)
        
        # Test the connection
        db = client[os.getenv('MONGODB_DB')]
        collections = db.list_collection_names()
        
        print("Successfully connected to MongoDB!")
        print(f"Available collections: {collections}")
        
        # Test inserting a document
        test_collection = db['test_collection']
        test_doc = {"test": "Hello MongoDB!", "timestamp": "2024-12-15"}
        result = test_collection.insert_one(test_doc)
        
        print(f"Successfully inserted document with id: {result.inserted_id}")
        
        # Retrieve the document
        retrieved_doc = test_collection.find_one({"test": "Hello MongoDB!"})
        print(f"Retrieved document: {retrieved_doc}")
        
        # Clean up - delete the test document
        test_collection.delete_one({"test": "Hello MongoDB!"})
        print("Test document deleted")
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()
            print("MongoDB connection closed")

if __name__ == "__main__":
    test_mongodb_connection()
