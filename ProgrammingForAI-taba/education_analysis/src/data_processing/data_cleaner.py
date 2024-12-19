"""
Module for cleaning education investment data.
"""

import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    """Class for cleaning education investment data."""
    
    def __init__(self):
        """Initialize the data cleaner."""
        pass
    
    def clean_education_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean education investment data.
        
        Args:
            data: Raw education investment data
            
        Returns:
            Cleaned DataFrame
        """
        try:
            # Make a copy to avoid modifying original data
            cleaned_data = data.copy()
            
            # Remove rows with missing values
            cleaned_data = cleaned_data.dropna(subset=['value'])
            
            # Convert value column to numeric
            cleaned_data['value'] = pd.to_numeric(cleaned_data['value'], errors='coerce')
            
            # Remove outliers using IQR method with a more lenient multiplier (3 instead of 1.5)
            Q1 = cleaned_data['value'].quantile(0.25)
            Q3 = cleaned_data['value'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            cleaned_data = cleaned_data[
                (cleaned_data['value'] >= lower_bound) & 
                (cleaned_data['value'] <= upper_bound)
            ]
            
            # Convert year to integer
            cleaned_data['year'] = pd.to_numeric(cleaned_data['year'], errors='coerce')
            cleaned_data = cleaned_data.dropna(subset=['year'])
            cleaned_data['year'] = cleaned_data['year'].astype(int)
            
            # Sort by year and country
            cleaned_data = cleaned_data.sort_values(['year', 'geo_time_period'])
            
            logger.info(f"Successfully cleaned education data: {len(cleaned_data)} rows remaining")
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Error cleaning education data: {str(e)}")
            raise
    
    def clean_economic_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean economic data.
        
        Args:
            data: Raw economic data
            
        Returns:
            Cleaned DataFrame
        """
        try:
            # Make a copy to avoid modifying original data
            cleaned_data = data.copy()
            
            # Remove rows with all missing values
            cleaned_data = cleaned_data.dropna(how='all')
            
            # Convert numeric columns
            numeric_cols = ['gdp_growth', 'employment_rate', 'gdp_per_capita', 'industry_value']
            for col in numeric_cols:
                if col in cleaned_data.columns:
                    cleaned_data[col] = pd.to_numeric(cleaned_data[col], errors='coerce')
            
            # Remove outliers from numeric columns
            for col in numeric_cols:
                if col in cleaned_data.columns:
                    Q1 = cleaned_data[col].quantile(0.25)
                    Q3 = cleaned_data[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    cleaned_data = cleaned_data[
                        (cleaned_data[col] >= lower_bound) & 
                        (cleaned_data[col] <= upper_bound)
                    ]
            
            # Convert year to integer
            cleaned_data['year'] = pd.to_numeric(cleaned_data['year'], errors='coerce')
            cleaned_data = cleaned_data.dropna(subset=['year'])
            cleaned_data['year'] = cleaned_data['year'].astype(int)
            
            # Sort by year and country
            cleaned_data = cleaned_data.sort_values(['year', 'country_code'])
            
            logger.info(f"Successfully cleaned economic data: {len(cleaned_data)} rows remaining")
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Error cleaning economic data: {str(e)}")
            raise
