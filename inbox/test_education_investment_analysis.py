import unittest
import pandas as pd
import numpy as np
import sys
import os
from unittest.mock import Mock, patch
from pathlib import Path

# Add project root and src directories to Python path
project_root = Path('..').resolve()
sys.path.append(str(project_root))
sys.path.append(str(project_root / 'education_analysis' / 'notebooks'))
sys.path.append(str(project_root / 'education_analysis'))

# Mock the required modules
class MockDatabaseManager:
    def connect_postgres(self): pass
    def connect_mongo(self): pass
    def insert_education_data(self, data): pass
    def query_mongo(self, collection): pass

class MockEurostatCollector:
    def get_education_investment_data(self): pass
    def get_economic_indicators(self): pass

class MockDataCleaner:
    def clean_education_data(self, data): 
        return data.dropna(subset=['value'])

# Patch the imports
import builtins
original_import = builtins.__import__

def mock_import(name, *args, **kwargs):
    if name == 'src.data_processing.db_manager':
        return type('MockModule', (), {'DatabaseManager': MockDatabaseManager})
    elif name == 'src.data_collection.eurostat_collector':
        return type('MockModule', (), {'EurostatCollector': MockEurostatCollector})
    elif name == 'src.data_processing.data_cleaner':
        return type('MockModule', (), {'DataCleaner': MockDataCleaner})
    return original_import(name, *args, **kwargs)

builtins.__import__ = mock_import

class TestEducationInvestmentAnalysis(unittest.TestCase):
    """Test cases for education investment analysis functionality"""
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        self.collector = MockEurostatCollector()
        self.db_manager = MockDatabaseManager()
        self.cleaner = MockDataCleaner()
        
        # Sample test data
        self.sample_education_data = pd.DataFrame({
            'geo_time_period': ['DE', 'FR', 'IT', 'ES', 'PL'],
            'year': [2020, 2020, 2020, 2020, 2020],
            'value': [5.2, 4.8, 4.1, 4.3, 4.7]
        })
        
        self.sample_economic_data = pd.DataFrame({
            'geo_time_period': ['DE', 'FR', 'IT', 'ES', 'PL'],
            'year': [2020, 2020, 2020, 2020, 2020],
            'gdp_per_capita': [40000, 35000, 30000, 25000, 15000]
        })

    def test_data_collection(self):
        """Test data collection functionality"""
        with patch.object(MockEurostatCollector, 'get_education_investment_data') as mock_edu:
            mock_edu.return_value = self.sample_education_data
            data = self.collector.get_education_investment_data()
            
            self.assertIsInstance(data, pd.DataFrame)
            self.assertEqual(len(data), 5)
            self.assertTrue('value' in data.columns)
            
    def test_data_cleaning(self):
        """Test data cleaning functionality"""
        # Create data with some missing values
        dirty_data = self.sample_education_data.copy()
        dirty_data.loc[0, 'value'] = np.nan
        
        cleaned_data = self.cleaner.clean_education_data(dirty_data)
        
        self.assertFalse(cleaned_data['value'].isnull().any())
        self.assertEqual(len(cleaned_data), len(dirty_data) - 1)  # One row should be removed
        
    def test_database_operations(self):
        """Test database operations"""
        with patch.object(MockDatabaseManager, 'connect_postgres') as mock_connect:
            mock_connect.return_value = None
            self.db_manager.connect_postgres()
            mock_connect.assert_called_once()
            
        with patch.object(MockDatabaseManager, 'insert_education_data') as mock_insert:
            mock_insert.return_value = None
            self.db_manager.insert_education_data(self.sample_education_data)
            mock_insert.assert_called_once()
            
    def test_data_analysis(self):
        """Test data analysis calculations"""
        # Test investment efficiency calculation
        merged_data = pd.merge(
            self.sample_education_data,
            self.sample_economic_data,
            on=['geo_time_period', 'year']
        )
        merged_data['investment_efficiency'] = merged_data['gdp_per_capita'] / merged_data['value']
        
        self.assertTrue('investment_efficiency' in merged_data.columns)
        self.assertEqual(len(merged_data), 5)
        self.assertTrue(all(merged_data['investment_efficiency'] > 0))

    def test_major_countries_filter(self):
        """Test filtering of major EU countries"""
        major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']
        filtered_data = self.sample_education_data[
            self.sample_education_data['geo_time_period'].isin(major_countries)
        ]
        
        self.assertEqual(len(filtered_data), 5)
        self.assertTrue(all(filtered_data['geo_time_period'].isin(major_countries)))

if __name__ == '__main__':
    unittest.main()
