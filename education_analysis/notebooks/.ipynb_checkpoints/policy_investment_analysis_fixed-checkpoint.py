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
    
    # Clean education data
    education_data_cleaned = education_data.copy()
    education_data_cleaned['year'] = pd.to_numeric(education_data_cleaned['year'], errors='coerce')
    education_data_cleaned['value'] = pd.to_numeric(education_data_cleaned['value'], errors='coerce')
    
    # Clean economic data
    economic_data['year'] = pd.to_numeric(economic_data['year'], errors='coerce')
    economic_data['gdp_per_capita'] = pd.to_numeric(economic_data['gdp_per_capita'], errors='coerce')
    
    # Merge datasets
    merged_data = pd.merge(
        education_data_cleaned,
        economic_data[['geo_time_period', 'year', 'gdp_per_capita']],
        on=['geo_time_period', 'year'],
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
    
    # Connect to MongoDB
    client = MongoClient(os.getenv('MONGODB_URI'))
    db = client[os.getenv('MONGODB_DB')]
    collection = db['education_policies']
    
    # Fetch policy documents
    policy_docs = list(collection.find())
    print("\nFetched policy documents from MongoDB")
    debug_print_data(policy_docs, "policy_docs")
    
    if policy_docs:
        print("\nAnalyzing Policy Impact...")
        print("-" * 40)
        
        policy_years = []
        policy_countries = []
        
        for doc in policy_docs:
            if 'year' in doc and 'country' in doc:
                policy_years.append(doc['year'])
                policy_countries.append(doc['country'])
        
        print(f"\nFound {len(policy_years)} policies to analyze")
        
        if policy_years:
            for country, year in zip(policy_countries, policy_years):
                country_data = education_data_cleaned[
                    education_data_cleaned['geo_time_period'] == country
                ]
                
                if not country_data.empty:
                    before_policy = country_data[country_data['year'] < year]['value'].mean()
                    after_policy = country_data[country_data['year'] >= year]['value'].mean()
                    
                    if not (np.isnan(before_policy) or np.isnan(after_policy)):
                        change_pct = ((after_policy - before_policy) / before_policy) * 100
                        print(f"\nCountry: {country}")
                        print(f"Policy Year: {year}")
                        print(f"Average Investment Change: {change_pct:.2f}%")
                    else:
                        print(f"\nWarning: Insufficient data for {country} around year {year}")
    
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
