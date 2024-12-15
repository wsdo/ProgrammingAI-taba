"""
EU Education Investment Analysis

This script analyzes education investment data from EU countries, including:
1. Data overview and basic statistics
2. Time series analysis
3. Inter-country comparison
4. Investment trend analysis
5. Visualization
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path().absolute().parent
sys.path.append(str(project_root))

# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_processor import DataProcessor

# Set plotting style and parameters
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class EUEducationAnalysis:
    def __init__(self):
        """Initialize analysis components"""
        self.db_manager = DatabaseManager()
        self.processor = DataProcessor()
        self.data = None
        self.data_cleaned = None
        
    def load_data(self):
        """Load and clean education data"""
        self.data = self.db_manager.get_education_data()
        self.data_cleaned = self.processor.clean_data(self.data)
        return self.data_cleaned
    
    def get_basic_stats(self):
        """Get basic statistics of the data"""
        stats = {
            'total_records': len(self.data_cleaned),
            'value_stats': self.data_cleaned['value'].describe(),
            'countries': self.data_cleaned['geo_time_period'].nunique(),
            'year_range': (self.data_cleaned['year'].min(), 
                          self.data_cleaned['year'].max())
        }
        return stats
    
    def analyze_time_series(self, countries=None):
        """Analyze education investment time series"""
        if countries is None:
            countries = ['DE', 'FR', 'IT', 'ES', 'PL']  # Default major EU countries
            
        time_series = self.data_cleaned[
            self.data_cleaned['geo_time_period'].isin(countries)
        ].pivot_table(
            index='year',
            columns='geo_time_period',
            values='value',
            aggfunc='mean'
        )
        return time_series
    
    def compare_countries(self, year=None):
        """Compare education investment between countries"""
        if year is None:
            year = self.data_cleaned['year'].max()
            
        comparison = self.data_cleaned[
            self.data_cleaned['year'] == year
        ].groupby('geo_time_period')['value'].mean().sort_values(ascending=False)
        
        return comparison
    
    def analyze_trends(self, country=None):
        """Analyze investment trends"""
        if country is None:
            # Analyze overall EU trend
            trend = self.data_cleaned.groupby('year')['value'].mean()
        else:
            trend = self.data_cleaned[
                self.data_cleaned['geo_time_period'] == country
            ].groupby('year')['value'].mean()
            
        return trend
    
    def visualize_data(self, plot_type='time_series', **kwargs):
        """Create visualizations for the analysis"""
        plt.figure(figsize=(12, 6))
        
        if plot_type == 'time_series':
            countries = kwargs.get('countries', ['DE', 'FR', 'IT', 'ES', 'PL'])
            data = self.analyze_time_series(countries)
            data.plot(marker='o')
            plt.title('Education Investment Over Time')
            plt.xlabel('Year')
            plt.ylabel('Investment (PPS)')
            
        elif plot_type == 'country_comparison':
            year = kwargs.get('year', self.data_cleaned['year'].max())
            data = self.compare_countries(year)
            data.plot(kind='bar')
            plt.title(f'Education Investment by Country ({year})')
            plt.xlabel('Country')
            plt.ylabel('Investment (PPS)')
            plt.xticks(rotation=45)
            
        elif plot_type == 'trend':
            country = kwargs.get('country', None)
            data = self.analyze_trends(country)
            data.plot(marker='o')
            plt.title('Education Investment Trend')
            plt.xlabel('Year')
            plt.ylabel('Investment (PPS)')
            
        plt.tight_layout()
        return plt.gcf()

def main():
    # Initialize analysis
    analyzer = EUEducationAnalysis()
    
    # Load data
    data = analyzer.load_data()
    print("\nData loaded successfully!")
    
    # Get basic statistics
    stats = analyzer.get_basic_stats()
    print("\nBasic Statistics:")
    print(f"Total records: {stats['total_records']}")
    print(f"Number of countries: {stats['countries']}")
    print(f"Year range: {stats['year_range']}")
    print("\nValue statistics:")
    print(stats['value_stats'])
    
    # Analyze time series
    time_series = analyzer.analyze_time_series()
    print("\nTime series analysis completed")
    
    # Compare countries
    comparison = analyzer.compare_countries()
    print("\nCountry comparison completed")
    
    # Analyze trends
    trend = analyzer.analyze_trends()
    print("\nTrend analysis completed")
    
    # Create visualizations
    analyzer.visualize_data(plot_type='time_series')
    plt.show()
    
    analyzer.visualize_data(plot_type='country_comparison')
    plt.show()
    
    analyzer.visualize_data(plot_type='trend')
    plt.show()

if __name__ == '__main__':
    main()
