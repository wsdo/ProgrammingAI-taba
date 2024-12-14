"""
Module for processing and cleaning education data.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from .db_manager import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EducationDataProcessor:
    """Class to handle education data processing and cleaning."""
    
    def __init__(self):
        """Initialize the processor with default parameters."""
        self.required_columns = ['time', 'geo', 'values']
        self.db_manager = DatabaseManager()
    
    def clean_dataset(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Clean and preprocess the dataset.
        
        Args:
            df (pd.DataFrame): Raw dataset
            
        Returns:
            pd.DataFrame: Cleaned dataset or None if cleaning fails
        """
        try:
            if df is None or df.empty:
                logger.warning("Empty dataset provided")
                return None
            
            # Create copy to avoid modifying original data
            cleaned_df = df.copy()
            
            # Handle missing values
            cleaned_df = cleaned_df.replace(':', np.nan)
            cleaned_df = cleaned_df.dropna(subset=['values'])
            
            # Convert types
            cleaned_df['time'] = pd.to_numeric(cleaned_df['time'])
            cleaned_df['values'] = pd.to_numeric(cleaned_df['values'])
            
            # Sort by time and geo
            cleaned_df = cleaned_df.sort_values(['time', 'geo'])
            
            logger.info(f"Successfully cleaned dataset. Shape: {cleaned_df.shape}")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error cleaning dataset: {str(e)}")
            return None
    
    def process_indicators(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Process all indicator datasets and store in database.
        
        Args:
            data_dict (Dict[str, pd.DataFrame]): Dictionary of raw datasets
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of processed datasets
        """
        processed_data = {}
        
        for code, df in data_dict.items():
            logger.info(f"Processing dataset: {code}")
            processed_df = self.clean_dataset(df)
            
            if processed_df is not None:
                processed_data[code] = processed_df
                
                # Store processed data in PostgreSQL
                if self.db_manager.connect_postgres():
                    self.db_manager.store_in_postgres(processed_df, f"{code}_processed")
                
                # Store processing metadata in MongoDB
                metadata = {
                    '_id': f"{code}_processed",
                    'original_indicator': code,
                    'processing_steps': ['cleaning', 'type_conversion', 'sorting'],
                    'rows_before': len(df),
                    'rows_after': len(processed_df),
                    'last_processed': pd.Timestamp.now().isoformat()
                }
                self.db_manager.store_in_mongodb(metadata, 'processing_metadata')
                
        return processed_data
    
    def calculate_statistics(self, df: pd.DataFrame, indicator_code: str) -> Dict[str, float]:
        """
        Calculate basic statistics for a dataset and store in MongoDB.
        
        Args:
            df (pd.DataFrame): Input dataset
            indicator_code (str): Indicator code
            
        Returns:
            Dict[str, float]: Dictionary of statistics
        """
        stats = {
            '_id': f"{indicator_code}_stats",
            'indicator_code': indicator_code,
            'mean': df['values'].mean(),
            'median': df['values'].median(),
            'std': df['values'].std(),
            'min': df['values'].min(),
            'max': df['values'].max(),
            'last_calculated': pd.Timestamp.now().isoformat()
        }
        
        # Store statistics in MongoDB
        self.db_manager.store_in_mongodb(stats, 'education_statistics')
        
        return stats
    
    def get_processed_data(self, indicator_code: str, 
                          countries: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Retrieve processed data from database.
        
        Args:
            indicator_code (str): Indicator code to retrieve
            countries (List[str], optional): List of country codes to filter
            
        Returns:
            pd.DataFrame: Retrieved data or None if retrieval fails
        """
        return self.db_manager.fetch_from_postgres(f"{indicator_code}_processed", countries)
    
    def get_statistics(self, indicator_code: str) -> Optional[Dict[str, float]]:
        """
        Retrieve statistics from MongoDB.
        
        Args:
            indicator_code (str): Indicator code to retrieve
            
        Returns:
            Dict[str, float]: Retrieved statistics or None if retrieval fails
        """
        return self.db_manager.fetch_from_mongodb('education_statistics', 
                                                {'indicator_code': indicator_code})
    
    def __del__(self):
        """Clean up database connections."""
        if hasattr(self, 'db_manager'):
            self.db_manager.close_connections()

def main():
    """Main function to demonstrate usage."""
    # Example dataset
    example_data = {
        'time': range(2010, 2021),
        'geo': ['EU'] * 11,
        'values': np.random.rand(11) * 100
    }
    df = pd.DataFrame(example_data)
    
    processor = EducationDataProcessor()
    
    # Test database connections
    if processor.db_manager.connect_postgres() and processor.db_manager.connect_mongodb():
        print("Database connections successful")
        
        # Process and store data
        cleaned_df = processor.clean_dataset(df)
        if cleaned_df is not None:
            print("\nCleaned Dataset:")
            print(cleaned_df.head())
            
            # Calculate and store statistics
            stats = processor.calculate_statistics(cleaned_df, 'example_indicator')
            print("\nCalculated Statistics:")
            for stat, value in stats.items():
                if isinstance(value, (int, float)):
                    print(f"{stat}: {value:.2f}")

if __name__ == "__main__":
    main()
