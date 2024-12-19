"""
Module for processing education data.
This module handles data cleaning, transformation, and preparation for analysis.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    """Class to handle data processing and cleaning."""
    
    def __init__(self):
        """Initialize the data processor."""
        self.required_columns = ['country_code', 'year', 'indicator_code']

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate that DataFrame has required columns and proper data types.
        
        Args:
            df (pd.DataFrame): Data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Check required columns
            missing_cols = [col for col in self.required_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return False

            # Validate data types
            if not pd.api.types.is_numeric_dtype(df['year']):
                logger.error("Year column must be numeric")
                return False

            return True
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}")
            return False

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the DataFrame by handling missing values and outliers.
        
        Args:
            df (pd.DataFrame): Data to clean
            
        Returns:
            pd.DataFrame: Cleaned data
        """
        try:
            # Create a copy to avoid modifying original data
            cleaned_df = df.copy()

            # Handle missing values
            cleaned_df = self._handle_missing_values(cleaned_df)

            # Remove duplicates
            cleaned_df = cleaned_df.drop_duplicates()

            # Sort by year and country
            cleaned_df = cleaned_df.sort_values(['year', 'country_code'])

            return cleaned_df
        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}")
            raise

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame with missing values
            
        Returns:
            pd.DataFrame: DataFrame with handled missing values
        """
        # For numeric columns, fill missing values with median of the same country
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            df[col] = df.groupby('country_code')[col].transform(
                lambda x: x.fillna(x.median())
            )

        # For categorical columns, fill with mode
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            df[col] = df[col].fillna(df[col].mode()[0])

        return df

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived indicators from the raw data.
        
        Args:
            df (pd.DataFrame): Raw data
            
        Returns:
            pd.DataFrame: Data with calculated indicators
        """
        try:
            # Create a copy for calculations
            result_df = df.copy()

            # Calculate year-over-year change
            if 'value' in result_df.columns:
                result_df['yoy_change'] = result_df.groupby('country_code')['value'].pct_change()

            # Calculate moving averages
            result_df['ma_3year'] = result_df.groupby('country_code')['value'].transform(
                lambda x: x.rolling(window=3, min_periods=1).mean()
            )

            return result_df
        except Exception as e:
            logger.error(f"Error calculating indicators: {str(e)}")
            raise

    def prepare_for_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data for analysis by structuring it appropriately.
        
        Args:
            df (pd.DataFrame): Raw data
            
        Returns:
            pd.DataFrame: Prepared data
        """
        try:
            # Validate data first
            if not self.validate_data(df):
                raise ValueError("Data validation failed")

            # Clean the data
            cleaned_df = self.clean_data(df)

            # Calculate indicators
            prepared_df = self.calculate_indicators(cleaned_df)

            # Add metadata
            prepared_df['processed_at'] = datetime.now()

            return prepared_df
        except Exception as e:
            logger.error(f"Error preparing data for analysis: {str(e)}")
            raise

    def aggregate_by_country(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate data by country.
        
        Args:
            df (pd.DataFrame): Data to aggregate
            
        Returns:
            pd.DataFrame: Aggregated data
        """
        try:
            return df.groupby(['country_code', 'year']).agg({
                'value': ['mean', 'std', 'count'],
                'yoy_change': 'mean',
                'ma_3year': 'last'
            }).reset_index()
        except Exception as e:
            logger.error(f"Error aggregating data: {str(e)}")
            raise
