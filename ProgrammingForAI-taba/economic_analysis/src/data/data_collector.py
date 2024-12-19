"""
Data Collection Module
Responsible for collecting data from IMF and saving it to local storage and databases
"""

import os
import logging
from typing import List
from datetime import datetime
from dotenv import load_dotenv

from imf_processor import IMFDataProcessor
from data_cleaner import DataCleaner
from db_manager import DatabaseManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollector:
    """Data collection class"""
    
    def __init__(self):
        """Initialize data collector"""
        self.imf_processor = IMFDataProcessor()
        self.data_cleaner = DataCleaner()
        self.db_manager = DatabaseManager()
        
        # List of major EU economies
        self.eu_countries = [
            'DE',  # Germany
            'FR',  # France
            'IT',  # Italy
            'ES',  # Spain
            'NL',  # Netherlands
            'BE'   # Belgium
        ]
        
    def collect_all_data(self, start_year: int, end_year: int):
        """Collect all data
        
        Args:
            start_year: Start year
            end_year: End year
        """
        try:
            # 1. Collect GDP data
            self._collect_gdp_data(start_year, end_year)
            
            # 2. Collect employment rate data
            self._collect_employment_data(start_year, end_year)
            
            # 3. Collect inflation rate data
            self._collect_inflation_data(start_year, end_year)
            
            logger.info("All data collection completed")
            
        except Exception as e:
            logger.error(f"Error occurred during data collection: {str(e)}")
            raise
            
    def _collect_gdp_data(self, start_year: int, end_year: int):
        """Collect GDP data"""
        try:
            # Get data
            raw_data = self.imf_processor.fetch_gdp_data(
                self.eu_countries, start_year, end_year
            )
            
            if raw_data.empty:
                logger.warning("No GDP data retrieved")
                return
                
            # Save raw data locally
            self.db_manager.save_raw_data(raw_data, 'gdp', end_year)
            
            # Clean data
            cleaned_data = self.data_cleaner.clean_data(raw_data)
            
            # Save processed data locally
            self.db_manager.save_processed_data(cleaned_data, 'gdp', end_year)
            
            # Save to PostgreSQL
            self.db_manager.save_to_postgres(cleaned_data, 'economic_gdp_data')
            
            # Save to MongoDB
            self.db_manager.save_to_mongodb(cleaned_data, 'economic_gdp_data')
            
            logger.info("GDP data collection and storage completed")
            
        except Exception as e:
            logger.error(f"Error occurred while collecting GDP data: {str(e)}")
            raise
            
    def _collect_employment_data(self, start_year: int, end_year: int):
        """Collect employment rate data"""
        try:
            # Get data
            raw_data = self.imf_processor.fetch_employment_data(
                self.eu_countries, start_year, end_year
            )
            
            if raw_data.empty:
                logger.warning("No employment rate data retrieved")
                return
                
            # Save raw data locally
            self.db_manager.save_raw_data(raw_data, 'employment', end_year)
            
            # Clean data
            cleaned_data = self.data_cleaner.clean_data(raw_data)
            
            # Save processed data locally
            self.db_manager.save_processed_data(cleaned_data, 'employment', end_year)
            
            # Save to PostgreSQL
            self.db_manager.save_to_postgres(cleaned_data, 'economic_employment_data')
            
            # Save to MongoDB
            self.db_manager.save_to_mongodb(cleaned_data, 'economic_employment_data')
            
            logger.info("Employment rate data collection and storage completed")
            
        except Exception as e:
            logger.error(f"Error occurred while collecting employment rate data: {str(e)}")
            raise
            
    def _collect_inflation_data(self, start_year: int, end_year: int):
        """Collect inflation rate data"""
        try:
            # Get data
            raw_data = self.imf_processor.fetch_inflation_data(
                self.eu_countries, start_year, end_year
            )
            
            if raw_data.empty:
                logger.warning("No inflation rate data retrieved")
                return
                
            # Save raw data locally
            self.db_manager.save_raw_data(raw_data, 'inflation', end_year)
            
            # Clean data
            cleaned_data = self.data_cleaner.clean_data(raw_data)
            
            # Save processed data locally
            self.db_manager.save_processed_data(cleaned_data, 'inflation', end_year)
            
            # Save to PostgreSQL
            self.db_manager.save_to_postgres(cleaned_data, 'economic_inflation_data')
            
            # Save to MongoDB
            self.db_manager.save_to_mongodb(cleaned_data, 'economic_inflation_data')
            
            logger.info("Inflation rate data collection and storage completed")
            
        except Exception as e:
            logger.error(f"Error occurred while collecting inflation rate data: {str(e)}")
            raise

if __name__ == "__main__":
    # Create data collector instance
    collector = DataCollector()
    
    # Set data collection time range
    START_YEAR = 2010
    END_YEAR = 2023
    
    # Collect data
    collector.collect_all_data(START_YEAR, END_YEAR)
