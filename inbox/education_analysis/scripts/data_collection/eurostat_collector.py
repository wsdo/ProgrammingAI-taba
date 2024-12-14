"""
Module for collecting education data from Eurostat API.
"""

import os
import pandas as pd
import eurostat
from typing import List, Dict, Optional
import logging
import sys
import os
import time

# Add parent directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_processing.db_manager import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EurostatCollector:
    """Class to handle data collection from Eurostat."""
    
    def __init__(self):
        """Initialize the collector with default parameters."""
        # Updated indicator codes based on Eurostat's education database
        self.base_indicators = {
            'educ_uoe_enrt01': 'Students by education level',
            'educ_uoe_perp01': 'Teaching staff',
            'educ_uoe_fina01': 'Education finance'
        }
        self.db_manager = DatabaseManager()
    
    def get_education_data(self, indicator: str, start_year: int = 2010) -> Optional[pd.DataFrame]:
        """
        Retrieve education data for a specific indicator.
        
        Args:
            indicator (str): Eurostat indicator code
            start_year (int): Starting year for data collection
            
        Returns:
            pd.DataFrame: Collected data or None if collection fails
        """
        try:
            logger.info(f"Collecting data for indicator: {indicator}")
            
            # Add retry mechanism
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Get data with specific parameters
                    df = eurostat.get_data_df(indicator, flags=False)
                    if df is not None and not df.empty:
                        break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                        time.sleep(2)  # Wait before retry
                    else:
                        raise e
            
            if df is None or df.empty:
                logger.warning(f"No data found for indicator: {indicator}")
                return None
            
            # Reset index to convert time from index to column
            df = df.reset_index()
            
            # Ensure 'time' column exists and is numeric
            if 'time' not in df.columns:
                df['time'] = df.index
            
            # Convert time to numeric and filter by start year
            df['time'] = pd.to_numeric(df['time'], errors='coerce')
            df = df[df['time'] >= start_year]
            
            # Basic data cleaning
            df = df.dropna(subset=['time'])
            
            # Ensure required columns exist
            if 'geo' not in df.columns:
                logger.error("Missing 'geo' column in dataset")
                return None
            
            # Create 'values' column if not present
            if 'values' not in df.columns:
                # Try to find a numeric column for values
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                if len(numeric_cols) > 0:
                    df['values'] = df[numeric_cols[0]]
                else:
                    logger.error("No numeric column found for values")
                    return None
            
            # Store data in databases
            if self.db_manager.connect_postgres():
                self.db_manager.store_in_postgres(df, indicator)
            
            # Store metadata in MongoDB
            metadata = {
                '_id': indicator,
                'indicator_name': self.base_indicators.get(indicator, ''),
                'start_year': start_year,
                'countries': df['geo'].unique().tolist(),
                'last_updated': pd.Timestamp.now().isoformat()
            }
            self.db_manager.store_in_mongodb(metadata, 'education_metadata')
            
            logger.info(f"Successfully collected and stored data for {indicator}")
            return df
            
        except Exception as e:
            logger.error(f"Error collecting data for {indicator}: {str(e)}")
            return None
    
    def collect_all_indicators(self, start_year: int = 2010) -> Dict[str, pd.DataFrame]:
        """
        Collect data for all base indicators.
        
        Args:
            start_year (int): Starting year for data collection
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of collected datasets
        """
        collected_data = {}
        
        for code, name in self.base_indicators.items():
            logger.info(f"Processing {name} (Code: {code})")
            df = self.get_education_data(code, start_year)
            
            if df is not None:
                collected_data[code] = df
            else:
                logger.warning(f"Skipping {code} due to collection failure")
                
        return collected_data
    
    def __del__(self):
        """Clean up database connections."""
        if hasattr(self, 'db_manager'):
            self.db_manager.close_connections()

def main():
    """Main function to demonstrate usage."""
    collector = EurostatCollector()
    
    # Test database connections
    if collector.db_manager.connect_postgres() and collector.db_manager.connect_mongodb():
        print("Database connections successful")
        
        # Set up PostgreSQL tables
        collector.db_manager.setup_postgres_tables()
        
        # Collect and store data
        data = collector.collect_all_indicators()
        
        for code, df in data.items():
            print(f"\nData for {collector.base_indicators[code]}:")
            print(f"Shape: {df.shape}")
            print("Sample data:")
            print(df.head())

if __name__ == "__main__":
    main()
