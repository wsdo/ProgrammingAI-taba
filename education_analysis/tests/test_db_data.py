import os
import sys
import unittest
from pathlib import Path
from dotenv import load_dotenv

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_processing.db_manager import DatabaseManager

class TestDatabaseData(unittest.TestCase):
    """Test cases for verifying database data integrity."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment before running tests."""
        # Load environment variables
        env_path = project_root / '.env'
        load_dotenv(env_path)
        
        # Initialize database manager
        cls.db_manager = DatabaseManager()
    
    def test_education_data_exists(self):
        """Test if education data exists in PostgreSQL."""
        try:
            query = "SELECT COUNT(*) as count FROM education_data"
            result = self.db_manager.query_postgres(query)
            count = result['count'].iloc[0]
            
            # Assert that there are records in the table
            self.assertGreater(count, 0, "Education data table should not be empty")
            print(f"Education data count: {count}")
        except Exception as e:
            self.fail(f"Failed to query education data: {str(e)}")
    
    def test_economic_data_exists(self):
        """Test if economic data exists in PostgreSQL."""
        try:
            query = "SELECT COUNT(*) as count FROM economic_data"
            result = self.db_manager.query_postgres(query)
            count = result['count'].iloc[0]
            
            # Assert that there are records in the table
            self.assertGreater(count, 0, "Economic data table should not be empty")
            print(f"Economic data count: {count}")
        except Exception as e:
            self.fail(f"Failed to query economic data: {str(e)}")
    
    def test_policy_data_exists(self):
        """Test if policy documents exist in MongoDB."""
        try:
            documents = list(self.db_manager.query_mongo('education_policies', {}))
            count = len(documents)
            
            # Assert that there are documents in the collection
            self.assertGreater(count, 0, "Policy documents collection should not be empty")
            print(f"Policy documents count: {count}")
        except Exception as e:
            self.fail(f"Failed to query policy documents: {str(e)}")
    
    def test_data_consistency(self):
        """Test data consistency across tables."""
        try:
            # Get unique years from education data
            edu_years_query = """
                SELECT DISTINCT year 
                FROM education_data 
                ORDER BY year
            """
            edu_years = set(self.db_manager.query_postgres(edu_years_query)['year'])
            
            # Get unique years from economic data
            eco_years_query = """
                SELECT DISTINCT year 
                FROM economic_data 
                ORDER BY year
            """
            eco_years = set(self.db_manager.query_postgres(eco_years_query)['year'])
            
            # Check if there's an overlap in years
            common_years = edu_years.intersection(eco_years)
            self.assertGreater(
                len(common_years), 
                0, 
                "There should be overlapping years between education and economic data"
            )
            print(f"Number of overlapping years: {len(common_years)}")
            
        except Exception as e:
            self.fail(f"Failed to check data consistency: {str(e)}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests are run."""
        cls.db_manager.close_connections()

if __name__ == '__main__':
    unittest.main(verbosity=2)
