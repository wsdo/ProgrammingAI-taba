"""
Test script for education investment analysis
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
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / '.env')

# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner
from src.data_collection.eurostat_collector import EurostatCollector

def main():
    print("Testing database connections and data processing...")
    print("-" * 50)
    
    try:
        # Initialize components
        db_manager = DatabaseManager()
        data_cleaner = DataCleaner()
        eurostat_collector = EurostatCollector()
        
        # Test database connections
        print("\nTesting PostgreSQL connection...")
        db_manager.connect_postgres()
        print("PostgreSQL connection successful!")
        
        print("\nTesting MongoDB connection...")
        db_manager.connect_mongo()
        print("MongoDB connection successful!")
        
        # Test data retrieval
        print("\nRetrieving education data...")
        education_data = db_manager.get_education_data()
        print(f"Retrieved education data shape: {education_data.shape if education_data is not None else 'No data'}")
        
        if education_data is not None and not education_data.empty:
            # Test data cleaning
            print("\nCleaning education data...")
            education_data_cleaned = data_cleaner.clean_education_data(education_data)
            print(f"Cleaned education data shape: {education_data_cleaned.shape}")
            
            # Display sample data
            print("\nSample of cleaned data:")
            print(education_data_cleaned.head())
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise
    finally:
        # Close connections
        if db_manager:
            db_manager.close_connections()

if __name__ == "__main__":
    main()
