"""
Test MongoDB Connection
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_connection():
    try:
        # MongoDB configuration
        MONGODB_HOST = os.getenv('MONGODB_HOST')
        MONGODB_PORT = int(os.getenv('MONGODB_PORT'))
        MONGODB_USER = os.getenv('MONGODB_USER')
        MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')
        MONGODB_DB = os.getenv('MONGODB_DB')

        # Create connection
        client = MongoClient(
            host=MONGODB_HOST,
            port=MONGODB_PORT,
            username=MONGODB_USER,
            password=MONGODB_PASSWORD,
            authSource='admin',
            serverSelectionTimeoutMS=5000  # 5 second timeout
        )

        # Test connection
        logger.info("Attempting to connect to MongoDB...")
        client.server_info()
        logger.info("Successfully connected to MongoDB")

        # Get database
        db = client[MONGODB_DB]
        
        # Test write operation
        test_collection = db['test_collection']
        result = test_collection.insert_one({'test': 'data'})
        logger.info(f"Successfully inserted test document with id: {result.inserted_id}")
        
        # Test read operation
        doc = test_collection.find_one({'test': 'data'})
        logger.info(f"Successfully read test document: {doc}")
        
        # Clean up
        test_collection.delete_one({'test': 'data'})
        logger.info("Successfully deleted test document")

        return True

    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()
