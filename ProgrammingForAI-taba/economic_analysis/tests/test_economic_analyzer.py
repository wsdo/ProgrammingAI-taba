"""
Tests for Economic Analyzer
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys
import os

# Add src to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.analysis.economic_analyzer import EconomicAnalyzer

class TestEconomicAnalyzer(unittest.TestCase):
    """Test cases for EconomicAnalyzer class"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test data"""
        # Create test data
        dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='Q')
        countries = ['DE', 'FR', 'IT']
        
        # GDP test data
        gdp_data = []
        for country in countries:
            for date in dates:
                gdp_data.append({
                    'country': country,
                    'date': date,
                    'value': np.random.uniform(80000, 120000)
                })
        cls.gdp_df = pd.DataFrame(gdp_data)
        
        # Employment test data
        emp_data = []
        for country in countries:
            for date in dates:
                emp_data.append({
                    'country': country,
                    'date': date,
                    'value': np.random.uniform(60, 80)
                })
        cls.emp_df = pd.DataFrame(emp_data)
        
        # Inflation test data
        inf_data = []
        for country in countries:
            for date in dates:
                inf_data.append({
                    'country': country,
                    'date': date,
                    'value': np.random.uniform(0, 5)
                })
        cls.inf_df = pd.DataFrame(inf_data)
        
        # Initialize analyzer
        cls.analyzer = EconomicAnalyzer()
        
    def test_analyze_gdp_trends(self):
        """Test GDP trends analysis"""
        results = self.analyzer.analyze_gdp_trends(self.gdp_df)
        
        # Check results structure
        self.assertIn('average_gdp', results)
        self.assertIn('growth_rates', results)
        self.assertIn('peak_gdp', results)
        self.assertIn('trough_gdp', results)
        
        # Check countries
        for country in ['DE', 'FR', 'IT']:
            self.assertIn(country, results['average_gdp'])
            self.assertIn(country, results['peak_gdp'])
            self.assertIn(country, results['trough_gdp'])
            
    def test_analyze_employment(self):
        """Test employment analysis"""
        results = self.analyzer.analyze_employment(self.emp_df)
        
        # Check results structure
        self.assertIn('average_employment', results)
        self.assertIn('annual_changes', results)
        self.assertIn('peak_employment', results)
        self.assertIn('trough_employment', results)
        
        # Check employment rate ranges
        for country in ['DE', 'FR', 'IT']:
            self.assertGreater(results['average_employment'][country], 0)
            self.assertLess(results['average_employment'][country], 100)
            
    def test_analyze_inflation(self):
        """Test inflation analysis"""
        results = self.analyzer.analyze_inflation(self.inf_df)
        
        # Check results structure
        self.assertIn('average_inflation', results)
        self.assertIn('inflation_volatility', results)
        self.assertIn('peak_inflation', results)
        self.assertIn('trough_inflation', results)
        
        # Check inflation calculations
        for country in ['DE', 'FR', 'IT']:
            self.assertGreaterEqual(results['inflation_volatility'][country], 0)
            
    def test_plot_gdp_trends(self):
        """Test GDP visualization"""
        fig = self.analyzer.plot_gdp_trends(self.gdp_df, save=False)
        self.assertIsNotNone(fig)
        
    def test_plot_employment_trends(self):
        """Test employment visualization"""
        fig = self.analyzer.plot_employment_trends(self.emp_df, save=False)
        self.assertIsNotNone(fig)
        
    def test_plot_inflation_trends(self):
        """Test inflation visualization"""
        fig = self.analyzer.plot_inflation_trends(self.inf_df, save=False)
        self.assertIsNotNone(fig)
        
    def test_create_correlation_matrix(self):
        """Test correlation matrix visualization"""
        fig = self.analyzer.create_correlation_matrix(
            self.gdp_df, self.emp_df, self.inf_df, save=False
        )
        self.assertIsNotNone(fig)

if __name__ == '__main__':
    unittest.main()
