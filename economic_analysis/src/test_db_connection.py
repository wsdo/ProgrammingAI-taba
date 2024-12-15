"""
Test database connections
"""

from data.db_manager import DatabaseManager
import sys
from sqlalchemy import text

def test_connection():
    """Test database connections"""
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Test PostgreSQL connection
        with db_manager.pg_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            print("PostgreSQL connection successful!")
            
        # Test MongoDB connection
        db_manager.mongo_client.server_info()
        print("MongoDB connection successful!")
        
        return True
        
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
