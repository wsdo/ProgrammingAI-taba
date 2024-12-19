import unittest
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_processing.db_manager import DatabaseManager
from src.data_processing.imf_data_processor import IMFDataProcessor

class TestDataProcessing(unittest.TestCase):
    """Test cases for data processing components."""

    def setUp(self):
        """Set up test fixtures"""
        # Set environment variables for testing
        os.environ['TESTING'] = 'true'
        os.environ['DB_TYPE'] = 'sqlite'
        os.environ['SQLITE_DB_PATH'] = ':memory:'  # Use in-memory SQLite database for testing

        # Initialize database manager
        self.db_manager = DatabaseManager()
        self.db_manager.connect_postgres()  # This will actually connect to SQLite in test mode
        self.db_manager.create_tables()

        # Initialize IMF data processor
        self.imf_processor = IMFDataProcessor()
        
        # Sample test data
        self.sample_gdp_data = pd.DataFrame({
            'country': ['DEU', 'FRA', 'ITA'],
            'year': [2020, 2020, 2020],
            'gdp_growth': [1.5, 1.2, 0.8]
        })
        
        self.sample_employment_data = pd.DataFrame({
            'country': ['DEU', 'FRA', 'ITA'],
            'year': [2020, 2020, 2020],
            'employment_rate': [75.0, 71.0, 68.0]
        })
        
        self.sample_education_data = pd.DataFrame({
            'geo_time_period': ['DE', 'FR', 'IT'],
            'year': [2020, 2020, 2020],
            'value': [4.5, 4.2, 3.8],
            'source': ['TEST', 'TEST', 'TEST']
        })

    def test_db_connection(self):
        """Test database connection"""
        if self.db_manager.use_sqlite:
            self.assertIsNotNone(self.db_manager.sqlite_conn)
        else:
            self.assertIsNotNone(self.db_manager.pg_conn)
        
    def test_create_tables(self):
        """Test table creation"""
        self.db_manager.create_tables()
        
        # Verify tables exist
        if self.db_manager.use_sqlite:
            cursor = self.db_manager.sqlite_conn.cursor()
        else:
            cursor = self.db_manager.pg_conn.cursor()

        # Check education_data table
        if self.db_manager.use_sqlite:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='education_data'
            """)
            self.assertTrue(cursor.fetchone() is not None)
        else:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'education_data'
                );
            """)
            self.assertTrue(cursor.fetchone()[0])
        
        # Check economic_data table
        if self.db_manager.use_sqlite:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='economic_data'
            """)
            self.assertTrue(cursor.fetchone() is not None)
        else:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'economic_data'
                );
            """)
            self.assertTrue(cursor.fetchone()[0])

    @patch('src.data_processing.imf_data_processor.IMFDataProcessor.fetch_gdp_data')
    @patch('src.data_processing.imf_data_processor.IMFDataProcessor.fetch_employment_data')
    def test_imf_data_processing(self, mock_employment, mock_gdp):
        """Test IMF data processing"""
        # Setup mock returns
        mock_gdp.return_value = self.sample_gdp_data
        mock_employment.return_value = self.sample_employment_data
        
        # Test data fetching
        gdp_data = self.imf_processor.fetch_gdp_data()
        employment_data = self.imf_processor.fetch_employment_data()
        
        self.assertEqual(len(gdp_data), 3)
        self.assertEqual(len(employment_data), 3)
        
        # Test data merging
        merged_data = self.imf_processor.merge_economic_data(gdp_data, employment_data)
        self.assertEqual(len(merged_data), 3)
        self.assertTrue('gdp_growth' in merged_data.columns)
        self.assertTrue('employment_rate' in merged_data.columns)

    def test_data_insertion_and_retrieval(self):
        """Test database insertion and retrieval operations"""
        # Insert test education data
        self.db_manager.insert_education_data(self.sample_education_data)
        
        # Insert test economic data
        merged_economic_data = pd.merge(
            self.sample_gdp_data,
            self.sample_employment_data,
            on=['country', 'year']
        )
        self.db_manager.insert_economic_data(merged_economic_data)
        
        # Retrieve and verify education data
        retrieved_education = self.db_manager.get_education_data()
        self.assertGreater(len(retrieved_education), 0)
        
        # Retrieve and verify economic data
        retrieved_economic = self.db_manager.get_economic_data()
        self.assertGreater(len(retrieved_economic), 0)
        
        # Verify data integrity
        test_country = retrieved_economic.iloc[0]
        self.assertIsNotNone(test_country['gdp_growth'])
        self.assertIsNotNone(test_country['employment_rate'])

    def test_data_validation(self):
        """Test data validation and integrity"""
        merged_data = pd.merge(
            self.sample_education_data,
            pd.merge(
                self.sample_gdp_data.assign(
                    country=lambda x: x['country'].map({'DEU': 'DE', 'FRA': 'FR', 'ITA': 'IT'})
                ),
                self.sample_employment_data.assign(
                    country=lambda x: x['country'].map({'DEU': 'DE', 'FRA': 'FR', 'ITA': 'IT'})
                ),
                on=['country', 'year']
            ),
            left_on=['geo_time_period', 'year'],
            right_on=['country', 'year']
        )
        
        # Test data completeness
        self.assertEqual(len(merged_data), 3)
        
        # Test value ranges
        self.assertTrue(all(merged_data['gdp_growth'].between(-100, 100)))
        self.assertTrue(all(merged_data['employment_rate'].between(0, 100)))
        self.assertTrue(all(merged_data['value'].between(0, 100)))

    def tearDown(self):
        """Clean up test fixtures"""
        try:
            # Clean up test data
            if self.db_manager.use_sqlite and self.db_manager.sqlite_conn:
                cursor = self.db_manager.sqlite_conn.cursor()
                cursor.execute("DELETE FROM education_data WHERE source = 'TEST'")
                cursor.execute("DELETE FROM economic_data WHERE source = 'IMF'")
                self.db_manager.sqlite_conn.commit()
            elif self.db_manager.pg_conn:
                cursor = self.db_manager.pg_conn.cursor()
                cursor.execute("DELETE FROM education_data WHERE source = 'TEST'")
                cursor.execute("DELETE FROM economic_data WHERE source = 'IMF'")
                self.db_manager.pg_conn.commit()
        except Exception as e:
            print(f"Warning: Error during test cleanup: {e}")
        finally:
            self.db_manager.close_connections()
            # Clean up environment variables
            os.environ.pop('TESTING', None)
            os.environ.pop('DB_TYPE', None)
            os.environ.pop('SQLITE_DB_PATH', None)

if __name__ == '__main__':
    unittest.main()
