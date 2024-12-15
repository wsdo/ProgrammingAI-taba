"""Test configuration file for pytest"""
import os
import sys
import pytest
import sqlite3
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Test database configuration
TEST_DB_PATH = project_root / 'tests' / 'test.db'

@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Set up test database before running tests"""
    # Create test database
    try:
        conn = sqlite3.connect(TEST_DB_PATH)
        cursor = conn.cursor()
        
        # Create education_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS education_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                geo_time_period VARCHAR(10),
                year INTEGER,
                value FLOAT,
                source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create economic_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS economic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country VARCHAR(10),
                year INTEGER,
                gdp_growth FLOAT,
                employment_rate FLOAT,
                source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error setting up test database: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

    # Set environment variables for tests
    os.environ.update({
        'TESTING': 'true',
        'DB_TYPE': 'sqlite',
        'SQLITE_DB_PATH': str(TEST_DB_PATH)
    })
    
    yield
    
    # Cleanup after tests
    try:
        if TEST_DB_PATH.exists():
            TEST_DB_PATH.unlink()
    except Exception as e:
        print(f"Error cleaning up test database: {e}")
