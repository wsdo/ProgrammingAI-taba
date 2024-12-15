"""
Generate test data for education and economic analysis
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_mongodb():
    """Connect to MongoDB"""
    client = MongoClient(
        f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
        f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
        "?authSource=admin"
    )
    return client[os.getenv('MONGODB_DB')]

def generate_test_data():
    """Generate test data for education and economic indicators"""
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Generate years instead of dates
    years = range(2010, 2020)
    
    # Generate country codes
    countries = ['USA', 'GBR', 'DEU', 'FRA', 'ITA']
    
    # Create test data
    test_data = []
    for country in countries:
        for year in years:
            # Create date as January 1st of the year
            date = datetime(year, 1, 1)
            
            # Education investment data (% of GDP)
            education_data = {
                'country_code': country,
                'date': date,
                'year': year,
                'value': np.random.uniform(4, 8),  # Education spending as % of GDP
                'indicator': 'education_investment'
            }
            
            # GDP growth data
            gdp_data = {
                'country_code': country,
                'date': date,
                'year': year,
                'value': np.random.uniform(1, 5),  # GDP growth rate
                'indicator': 'gdp_growth'
            }
            
            # Employment rate data
            employment_data = {
                'country_code': country,
                'date': date,
                'year': year,
                'value': np.random.uniform(60, 80),  # Employment rate
                'indicator': 'employment_rate'
            }
            
            test_data.extend([education_data, gdp_data, employment_data])
    
    return pd.DataFrame(test_data)

def setup_mongodb_collections(db):
    """Set up MongoDB collections with proper indexes"""
    try:
        # Drop existing collections
        db.education_investment.drop()
        db.preprocessed_gdp.drop()
        db.preprocessed_employment.drop()
        
        # Create new collections with indexes
        db.create_collection('education_investment')
        db.create_collection('preprocessed_gdp')
        db.create_collection('preprocessed_employment')
        
        # Create indexes
        db.education_investment.create_index([('country_code', ASCENDING), ('year', ASCENDING)], unique=True)
        db.preprocessed_gdp.create_index([('country_code', ASCENDING), ('year', ASCENDING)], unique=True)
        db.preprocessed_employment.create_index([('country_code', ASCENDING), ('year', ASCENDING)], unique=True)
        
        print("MongoDB collections and indexes set up successfully")
        
    except Exception as e:
        print(f"Error setting up MongoDB collections: {str(e)}")
        raise

def save_to_mongodb(db, data):
    """Save test data to MongoDB collections"""
    # Split data by indicator
    education_data = data[data['indicator'] == 'education_investment'].copy()
    gdp_data = data[data['indicator'] == 'gdp_growth'].copy()
    employment_data = data[data['indicator'] == 'employment_rate'].copy()
    
    # Drop indicator column
    for df in [education_data, gdp_data, employment_data]:
        df.drop('indicator', axis=1, inplace=True)
    
    # Save to MongoDB
    try:
        # Set up collections
        setup_mongodb_collections(db)
        
        # Insert new data
        db.education_investment.insert_many(education_data.to_dict('records'))
        db.preprocessed_gdp.insert_many(gdp_data.to_dict('records'))
        db.preprocessed_employment.insert_many(employment_data.to_dict('records'))
        
        print("Test data successfully saved to MongoDB")
        
        # Print sample counts
        print("\nData counts in MongoDB:")
        print(f"Education investment: {db.education_investment.count_documents({})}")
        print(f"GDP: {db.preprocessed_gdp.count_documents({})}")
        print(f"Employment: {db.preprocessed_employment.count_documents({})}")
        
        # Print sample data
        print("\nSample education investment data:")
        print(list(db.education_investment.find().limit(2)))
        
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")
        raise

def main():
    """Main function to generate and save test data"""
    try:
        print("Connecting to MongoDB...")
        db = connect_mongodb()
        
        print("\nGenerating test data...")
        test_data = generate_test_data()
        
        print("\nSaving test data to MongoDB...")
        save_to_mongodb(db, test_data)
        
        print("\nTest data generation completed successfully!")
        
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
