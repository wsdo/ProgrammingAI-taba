import pandas as pd
import requests
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IMFDataProcessor:
    def __init__(self):
        self.base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc"
        
    def fetch_gdp_data(self, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """
        Fetch GDP growth rate data from IMF
        """
        try:
            # IMF API endpoint for GDP growth
            endpoint = f"/CompactData/IFS/Q..NGDP_R_K_IX"
            
            response = requests.get(f"{self.base_url}{endpoint}")
            if response.status_code == 200:
                data = response.json()
                # Process the JSON response into a DataFrame
                # This is a simplified version - actual implementation would need to parse the specific IMF JSON structure
                df = pd.DataFrame(data)
                return df
            else:
                logging.error(f"Failed to fetch GDP data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logging.error(f"Error fetching GDP data: {str(e)}")
            return pd.DataFrame()
    
    def fetch_employment_data(self, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """
        Fetch employment rate data from IMF
        """
        try:
            # IMF API endpoint for employment statistics
            endpoint = f"/CompactData/IFS/Q..LUR"
            
            response = requests.get(f"{self.base_url}{endpoint}")
            if response.status_code == 200:
                data = response.json()
                # Process the JSON response into a DataFrame
                # This is a simplified version - actual implementation would need to parse the specific IMF JSON structure
                df = pd.DataFrame(data)
                return df
            else:
                logging.error(f"Failed to fetch employment data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logging.error(f"Error fetching employment data: {str(e)}")
            return pd.DataFrame()
    
    def process_economic_data(self, gdp_df: pd.DataFrame, employment_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and combine GDP and employment data
        """
        try:
            # Merge GDP and employment data
            economic_df = pd.merge(
                gdp_df,
                employment_df,
                on=['country', 'year'],
                how='outer'
            )
            
            # Clean and process the data
            economic_df = economic_df.dropna()
            
            return economic_df
            
        except Exception as e:
            logging.error(f"Error processing economic data: {str(e)}")
            return pd.DataFrame()
    
    def merge_economic_data(self, gdp_data: pd.DataFrame, employment_data: pd.DataFrame) -> pd.DataFrame:
        """Merge GDP and employment data into a single DataFrame.
        
        Args:
            gdp_data (pd.DataFrame): DataFrame containing GDP data
            employment_data (pd.DataFrame): DataFrame containing employment data
            
        Returns:
            pd.DataFrame: Merged DataFrame containing both GDP and employment data
        """
        try:
            # Merge on country and year
            merged_data = pd.merge(
                gdp_data,
                employment_data,
                on=['country', 'year'],
                how='inner'
            )
            
            logging.info(f"Successfully merged economic data: {len(merged_data)} rows")
            return merged_data
            
        except Exception as e:
            logging.error(f"Error merging economic data: {str(e)}")
            raise
    
    def get_economic_indicators(self, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """
        Main method to get all economic indicators
        """
        # Fetch both GDP and employment data
        gdp_df = self.fetch_gdp_data(countries, start_year, end_year)
        employment_df = self.fetch_employment_data(countries, start_year, end_year)
        
        # Process and combine the data
        economic_df = self.process_economic_data(gdp_df, employment_df)
        
        return economic_df
