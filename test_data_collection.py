"""
Test data collection and storage functionality
"""
from education_analysis_notebook import (
    get_postgres_connection,
    get_mongodb_connection,
    collect_and_store_education_data
)

def test_data_collection():
    """Test data collection and storage"""
    # Get database connections
    pg_conn = get_postgres_connection()
    mongo_db = get_mongodb_connection()
    
    if pg_conn is None or mongo_db is None:
        print("Failed to connect to databases")
        return
    
    try:
        # Collect and store data
        collect_and_store_education_data(pg_conn, mongo_db)
        print("Data collection and storage test completed")
        
    except Exception as e:
        print(f"Error in data collection test: {str(e)}")
    
    finally:
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    test_data_collection()
