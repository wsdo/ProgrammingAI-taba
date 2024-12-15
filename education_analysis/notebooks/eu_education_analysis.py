"""
EU Education Investment Analysis

This script analyzes education investment data from EU countries, including:
1. Data overview and basic statistics
2. Time series analysis
3. Inter-country comparison
4. Investment trend analysis
5. Visualization
6. Economic correlation analysis
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import logging

# Add project root to Python path
project_root = Path().absolute().parent
sys.path.append(str(project_root))

# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_processor import DataProcessor
from src.data_processing.imf_data_processor import IMFDataProcessor

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
        self.imf_processor = IMFDataProcessor()
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
    
    def analyze_economic_correlation(self, start_year=None, end_year=None):
        """Analyze correlation between education investment and economic indicators"""
        if start_year is None:
            start_year = self.data_cleaned['year'].min()
        if end_year is None:
            end_year = self.data_cleaned['year'].max()
            
        # Get economic data
        countries = self.data_cleaned['geo_time_period'].unique().tolist()
        
        # Fetch GDP and employment data
        gdp_data = self.imf_processor.fetch_gdp_data(countries, start_year, end_year)
        employment_data = self.imf_processor.fetch_employment_data(countries, start_year, end_year)
        
        # Calculate correlations
        correlations = {}
        
        # Group education data by country and year
        edu_data = self.data_cleaned.groupby(['geo_time_period', 'year'])['value'].mean().reset_index()
        
        # Calculate correlation with GDP
        if not gdp_data.empty:
            gdp_correlations = {}
            for country in countries:
                country_edu = edu_data[edu_data['geo_time_period'] == country].set_index('year')['value']
                country_gdp = gdp_data[gdp_data['country'] == country].set_index('year')['value']
                if not country_edu.empty and not country_gdp.empty:
                    correlation = country_edu.corr(country_gdp)
                    gdp_correlations[country] = correlation
            correlations['gdp'] = gdp_correlations
            
        # Calculate correlation with employment
        if not employment_data.empty:
            emp_correlations = {}
            for country in countries:
                country_edu = edu_data[edu_data['geo_time_period'] == country].set_index('year')['value']
                country_emp = employment_data[employment_data['country'] == country].set_index('year')['value']
                if not country_edu.empty and not country_emp.empty:
                    correlation = country_edu.corr(country_emp)
                    emp_correlations[country] = correlation
            correlations['employment'] = emp_correlations
            
        return correlations
        
    def visualize_economic_correlation(self, correlation_type='gdp'):
        """Visualize economic correlations"""
        correlations = self.analyze_economic_correlation()
        
        if correlation_type in correlations:
            plt.figure(figsize=(12, 6))
            data = pd.Series(correlations[correlation_type])
            data.plot(kind='bar')
            plt.title(f'Education Investment vs {correlation_type.upper()} Correlation by Country')
            plt.xlabel('Country')
            plt.ylabel('Correlation Coefficient')
            plt.xticks(rotation=45)
            plt.tight_layout()
            return plt.gcf()
        else:
            logging.warning(f"No correlation data available for {correlation_type}")
            return None
    
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
    
    # Analyze economic correlation
    correlations = analyzer.analyze_economic_correlation()
    print("\nEconomic correlation analysis completed")
    
    # Create visualizations
    analyzer.visualize_data(plot_type='time_series')
    plt.show()
    
    analyzer.visualize_data(plot_type='country_comparison')
    plt.show()
    
    analyzer.visualize_data(plot_type='trend')
    plt.show()
    
    analyzer.visualize_economic_correlation(correlation_type='gdp')
    plt.show()
    
    analyzer.visualize_economic_correlation(correlation_type='employment')
    plt.show()

if __name__ == '__main__':
    main()
