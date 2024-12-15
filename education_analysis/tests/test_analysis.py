import unittest
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

class TestAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are shared across all tests"""
        # Create sample data for testing
        cls.sample_data = pd.DataFrame({
            'country': ['DE', 'FR', 'IT', 'DE', 'FR', 'IT'],
            'year': [2019, 2019, 2019, 2020, 2020, 2020],
            'value': [4.5, 4.2, 3.8, 4.6, 4.3, 3.9],
            'gdp_growth': [1.5, 1.2, 0.8, -4.5, -7.2, -8.8],
            'employment_rate': [75.0, 71.0, 68.0, 74.0, 70.0, 67.0]
        })

    def test_data_structure(self):
        """Test the structure of the analysis data"""
        required_columns = ['country', 'year', 'value', 'gdp_growth', 'employment_rate']
        for column in required_columns:
            self.assertIn(column, self.sample_data.columns)

    def test_data_types(self):
        """Test data types of each column"""
        self.assertEqual(self.sample_data['country'].dtype, object)
        self.assertEqual(self.sample_data['year'].dtype, np.int64)
        self.assertTrue(np.issubdtype(self.sample_data['value'].dtype, np.number))
        self.assertTrue(np.issubdtype(self.sample_data['gdp_growth'].dtype, np.number))
        self.assertTrue(np.issubdtype(self.sample_data['employment_rate'].dtype, np.number))

    def test_correlation_calculation(self):
        """Test correlation calculations"""
        correlations = self.sample_data[['value', 'gdp_growth', 'employment_rate']].corr()
        
        # Check correlation matrix properties
        self.assertEqual(correlations.shape, (3, 3))
        self.assertTrue(np.allclose(correlations.values, correlations.values.T))
        self.assertTrue(np.allclose(np.diag(correlations), 1.0))

    def test_data_aggregation(self):
        """Test data aggregation by country"""
        country_stats = self.sample_data.groupby('country').agg({
            'value': 'mean',
            'gdp_growth': 'mean',
            'employment_rate': 'mean'
        })
        
        self.assertEqual(len(country_stats), 3)
        self.assertTrue(all(country_stats['value'] > 0))
        self.assertTrue(all(country_stats['employment_rate'] > 0))

    def test_time_series_consistency(self):
        """Test time series data consistency"""
        years = self.sample_data['year'].unique()
        self.assertEqual(len(years), 2)
        
        # Check if each country has data for all years
        for country in self.sample_data['country'].unique():
            country_years = self.sample_data[self.sample_data['country'] == country]['year']
            self.assertEqual(len(country_years), 2)

    def test_value_ranges(self):
        """Test if values are within expected ranges"""
        # Education investment should be between 0 and 100%
        self.assertTrue(all(self.sample_data['value'].between(0, 100)))
        
        # GDP growth can be negative but should be within reasonable bounds
        self.assertTrue(all(self.sample_data['gdp_growth'].between(-50, 50)))
        
        # Employment rate should be between 0 and 100%
        self.assertTrue(all(self.sample_data['employment_rate'].between(0, 100)))

    def test_visualization_data(self):
        """Test if data is suitable for visualization"""
        # Test if there are enough data points for meaningful visualization
        self.assertGreaterEqual(len(self.sample_data), 6)
        
        # Test if there are no missing values
        self.assertTrue(self.sample_data['value'].notna().all())
        self.assertTrue(self.sample_data['gdp_growth'].notna().all())
        self.assertTrue(self.sample_data['employment_rate'].notna().all())

    def test_country_codes(self):
        """Test country code consistency"""
        valid_countries = {'DE', 'FR', 'IT', 'ES', 'PL'}
        self.assertTrue(set(self.sample_data['country']).issubset(valid_countries))

    def test_trend_analysis(self):
        """Test trend analysis calculations"""
        for country in self.sample_data['country'].unique():
            country_data = self.sample_data[self.sample_data['country'] == country].sort_values('year')
            
            # Calculate year-over-year changes
            education_change = country_data['value'].diff()
            gdp_change = country_data['gdp_growth'].diff()
            employment_change = country_data['employment_rate'].diff()
            
            # Verify calculations
            self.assertEqual(len(education_change.dropna()), 1)
            self.assertEqual(len(gdp_change.dropna()), 1)
            self.assertEqual(len(employment_change.dropna()), 1)

if __name__ == '__main__':
    unittest.main()
