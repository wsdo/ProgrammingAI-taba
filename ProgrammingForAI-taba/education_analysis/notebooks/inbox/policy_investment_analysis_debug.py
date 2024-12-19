import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

# Connect to MongoDB
try:
    client = MongoClient(os.getenv('MONGODB_URI'))
    db = client[os.getenv('MONGODB_DB')]
    collection = db['education_policies']
    
    # Fetch policy documents
    policy_docs = list(collection.find())
    print("\nFetched policy documents from MongoDB")
    debug_print_data(policy_docs, "policy_docs")
    
    # Load education and GDP data
    try:
        education_data_cleaned = pd.read_csv('education_data_cleaned.csv')
        merged_data = pd.read_csv('merged_education_gdp_data.csv')
        
        print("\nLoaded CSV data files")
        debug_print_data(education_data_cleaned, "education_data_cleaned")
        debug_print_data(merged_data, "merged_data")
        
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
            # Ensure numeric types
            merged_data['gdp_per_capita'] = pd.to_numeric(merged_data['gdp_per_capita'], errors='coerce')
            merged_data['value'] = pd.to_numeric(merged_data['value'], errors='coerce')
            
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
            
    except FileNotFoundError as e:
        print(f"Error: Could not find data file: {str(e)}")
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
except Exception as e:
    print(f"Error connecting to MongoDB: {str(e)}")
    import traceback
    print(traceback.format_exc())
