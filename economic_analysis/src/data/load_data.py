"""
Data Loading Script
Loads economic data from IMF API and stores it in databases
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from db_manager import DatabaseManager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class IMFDataLoader:
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def generate_mock_data(self, countries, start_date, end_date):
        """Generate mock economic data for testing"""
        # Create date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='Q')
        
        # Generate data for each country
        data = []
        for country in countries:
            # Base values for each country
            base_gdp = np.random.uniform(800000, 1200000)
            base_emp = np.random.uniform(60, 80)
            base_inf = np.random.uniform(1, 3)
            
            # Add some trend and seasonality
            for date in date_range:
                # GDP with trend and seasonal variation
                gdp = base_gdp * (1 + 0.01 * np.sin(date.month/12 * 2 * np.pi))
                gdp *= (1 + np.random.normal(0.005, 0.002))  # Add some noise
                
                # Employment with seasonal variation
                emp = base_emp + 2 * np.sin(date.month/12 * 2 * np.pi)
                emp += np.random.normal(0, 0.5)
                
                # Inflation with some randomness
                inf = base_inf + np.random.normal(0, 0.2)
                
                data.append({
                    'country': country,
                    'date': date,
                    'gdp': gdp,
                    'employment': emp,
                    'inflation': inf
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Split into separate DataFrames
        gdp_data = df[['country', 'date', 'gdp']].rename(columns={'gdp': 'value'})
        emp_data = df[['country', 'date', 'employment']].rename(columns={'employment': 'value'})
        inf_data = df[['country', 'date', 'inflation']].rename(columns={'inflation': 'value'})
        
        return gdp_data, emp_data, inf_data
            
    def load_and_store_data(self):
        """Main function to load and store all economic data"""
        try:
            # Configuration
            countries = ['BE', 'FR', 'DE', 'IT', 'NL', 'ES']
            start_date = '2019-01-01'
            end_date = '2023-12-31'
            
            logger.info("Starting data loading process...")
            
            # Generate mock data
            gdp_data, emp_data, inf_data = self.generate_mock_data(
                countries, start_date, end_date
            )
            
            # Store in PostgreSQL
            logger.info("Storing data in PostgreSQL...")
            self.db_manager.save_to_postgres(gdp_data, 'gdp')
            self.db_manager.save_to_postgres(emp_data, 'employment')
            self.db_manager.save_to_postgres(inf_data, 'inflation')
            
            # Store in MongoDB
            logger.info("Storing data in MongoDB...")
            self.db_manager.save_to_mongodb(
                gdp_df=gdp_data,
                emp_df=emp_data,
                inf_df=inf_data
            )
            
            logger.info("Data loading and storage completed successfully")
            
            # Return the data for analysis
            return gdp_data, emp_data, inf_data
            
        except Exception as e:
            logger.error(f"Error in load_and_store_data: {str(e)}")
            raise

if __name__ == "__main__":
    loader = IMFDataLoader()
    gdp_data, emp_data, inf_data = loader.load_and_store_data()
