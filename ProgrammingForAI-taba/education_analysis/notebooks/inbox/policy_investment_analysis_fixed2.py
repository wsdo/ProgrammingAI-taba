import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_and_process_data():
    """Load and process education and economic data"""
    # Load raw data
    education_data = pd.read_csv('../data/cache/education_investment.csv')
    economic_data = pd.read_csv('../data/cache/economic_indicators.csv')
    
    # Convert education data from wide to long format
    id_vars = ['index', 'freq', 'unit', 'isced11', 'geo\\TIME_PERIOD', 'collected_at', 'source']
    value_vars = [str(year) for year in range(2012, 2022)]  # Years 2012-2021
    
    education_data_long = pd.melt(
        education_data,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='year',
        value_name='value'
    )
    
    # Clean and prepare education data
    education_data_cleaned = education_data_long.copy()
    education_data_cleaned['year'] = pd.to_numeric(education_data_cleaned['year'])
    education_data_cleaned['value'] = pd.to_numeric(education_data_cleaned['value'], errors='coerce')
    education_data_cleaned = education_data_cleaned.rename(columns={'geo\\TIME_PERIOD': 'geo_time_period'})
    
    # Clean economic data
    economic_data['year'] = pd.to_numeric(economic_data['year'])
    economic_data['gdp_per_capita'] = pd.to_numeric(economic_data['gdp_per_capita'], errors='coerce')
    
    # Create country code mapping
    country_mapping = {
        'AT': 'AUT', 'BE': 'BEL', 'BG': 'BGR', 'CY': 'CYP', 'CZ': 'CZE',
        'DE': 'DEU', 'DK': 'DNK', 'EE': 'EST', 'ES': 'ESP', 'FI': 'FIN',
        'FR': 'FRA', 'GR': 'GRC', 'HR': 'HRV', 'HU': 'HUN', 'IE': 'IRL',
        'IT': 'ITA', 'LT': 'LTU', 'LU': 'LUX', 'LV': 'LVA', 'MT': 'MLT',
        'NL': 'NLD', 'PL': 'POL', 'PT': 'PRT', 'RO': 'ROU', 'SE': 'SWE',
        'SI': 'SVN', 'SK': 'SVK'
    }
    
    # Map country codes in education data
    education_data_cleaned['country_code'] = education_data_cleaned['geo_time_period'].map(country_mapping)
    
    # Merge datasets
    merged_data = pd.merge(
        education_data_cleaned,
        economic_data[['country_code', 'year', 'gdp_per_capita']],
        on=['country_code', 'year'],
        how='inner'
    )
    
    return education_data_cleaned, merged_data

def debug_print_data(data, name):
    """Helper function to print debug information about dataframes"""
    print(f"\nDebugging {name}:")
    print(f"Type: {type(data)}")
    if isinstance(data, pd.DataFrame):
        print(f"Shape: {data.shape}")
        print(f"Columns: {data.columns.tolist()}")
        print("\nFirst few rows:")
        print(data.head())
        print("\nMissing values:")
        print(data.isnull().sum())
    elif isinstance(data, list):
        print(f"Number of items: {len(data)}")
        if data:
            print("First item sample:")
            print(data[0])

try:
    # Load and process data
    print("Loading and processing data...")
    education_data_cleaned, merged_data = load_and_process_data()
    
    # Debug print data
    debug_print_data(education_data_cleaned, "education_data_cleaned")
    debug_print_data(merged_data, "merged_data")
    
    print("\nAnalyzing Investment Efficiency...")
    print("-" * 40)
    
    if not merged_data.empty:
        # Calculate efficiency
        merged_data['investment_efficiency'] = merged_data['gdp_per_capita'] / merged_data['value']
        
        # Remove any infinite or null values
        merged_data = merged_data.replace([np.inf, -np.inf], np.nan).dropna(subset=['investment_efficiency'])
        
        latest_year = merged_data['year'].max()
        print(f"\nAnalyzing data for year: {latest_year}")
        
        latest_efficiency = merged_data[merged_data['year'] == latest_year]
        print(f"Number of countries in latest year: {len(latest_efficiency)}")
        
        if not latest_efficiency.empty:
            top_efficient = latest_efficiency.nlargest(5, 'investment_efficiency')
            
            plt.figure(figsize=(12, 6))
            sns.barplot(data=top_efficient, x='geo_time_period', y='investment_efficiency')
            plt.title(f'Top 5 Countries by Investment Efficiency ({latest_year})')
            plt.xlabel('Country')
            plt.ylabel('Efficiency Ratio (GDP per capita / Investment)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('investment_efficiency.png')
            plt.close()
            
            print("\nTop 5 Countries by Investment Efficiency:")
            for _, row in top_efficient.iterrows():
                print(f"{row['geo_time_period']}: {row['investment_efficiency']:.2f}")
                print(f"  GDP per capita: {row['gdp_per_capita']:.2f}")
                print(f"  Education Investment: {row['value']:.2f}")
        else:
            print("No data available for the latest year")
    else:
        print("Merged data is empty")
        
except Exception as e:
    print(f"Error: {str(e)}")
    import traceback
    print(traceback.format_exc())
