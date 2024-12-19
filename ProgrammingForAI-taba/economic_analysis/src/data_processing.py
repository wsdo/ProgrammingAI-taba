"""
Data Processing Script
Process and store economic data in PostgreSQL database
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from data.db_manager import DatabaseManager
from data.imf_processor import IMFDataProcessor

def main():
    """Main function to process and store data"""
    # Initialize processors
    db_manager = DatabaseManager()
    imf_processor = IMFDataProcessor()
    
    # Initialize database schema
    db_manager.init_database()
    
    # Process data for 2023
    year = 2023
    countries = ['DE', 'FR', 'IT', 'ES', 'NL', 'BE']
    
    try:
        # 1. Fetch and process GDP data
        gdp_data = imf_processor.fetch_gdp_data(countries, year, year)
        if not gdp_data.empty:
            db_manager.save_processed_data(gdp_data, 'gdp', year)
            db_manager.save_to_postgres(gdp_data, 'gdp')
        
        # 2. Fetch and process employment data
        employment_data = imf_processor.fetch_employment_data(countries, year, year)
        if not employment_data.empty:
            db_manager.save_processed_data(employment_data, 'employment', year)
            db_manager.save_to_postgres(employment_data, 'employment')
        
        # 3. Fetch and process inflation data
        inflation_data = imf_processor.fetch_inflation_data(countries, year, year)
        if not inflation_data.empty:
            db_manager.save_processed_data(inflation_data, 'inflation', year)
            db_manager.save_to_postgres(inflation_data, 'inflation')
        
        print("Data processing and storage completed successfully!")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise

if __name__ == "__main__":
    main()
