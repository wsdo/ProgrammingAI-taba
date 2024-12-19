"""
IMF Data Processing Module
Responsible for fetching and processing economic data from IMF API
"""

import os
import logging
import pandas as pd
import requests
from typing import Dict, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IMFDataProcessor:
    """IMF data processing class"""
    
    def __init__(self):
        """Initialize IMF data processor"""
        self.base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc"
        self.cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
    def fetch_gdp_data(self, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """Fetch GDP data
        
        Args:
            countries: List of country codes
            start_year: Start year
            end_year: End year
            
        Returns:
            DataFrame containing GDP data
        """
        try:
            # Using International Financial Statistics (IFS) database for GDP data
            # NGDP_R_SA_XDC: GDP Volume, Reference Year Prices, Seasonally Adjusted
            endpoint = "/CompactData/IFS/Q.DE+FR+IT+ES+NL+BE.NGDP_R_SA_XDC"
            params = {
                "startPeriod": f"{start_year}",
                "endPeriod": f"{end_year}"
            }
            
            response = requests.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()  # Raise error for bad status codes
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched GDP data: {len(data.get('CompactData', {}).get('DataSet', {}).get('Series', []))} series")
                df = self._process_gdp_response(data)
                return df
            else:
                logger.error(f"Failed to fetch GDP data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error occurred while fetching GDP data: {str(e)}")
            return pd.DataFrame()
            
    def fetch_employment_data(self, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """Fetch employment rate data
        
        Args:
            countries: List of country codes
            start_year: Start year
            end_year: End year
            
        Returns:
            DataFrame containing employment rate data
        """
        try:
            # Using International Financial Statistics (IFS) database for employment data
            # LUR_PT: Unemployment Rate, Percent
            endpoint = "/CompactData/IFS/Q.DE+FR+IT+ES+NL+BE.LUR_PT"
            params = {
                "startPeriod": f"{start_year}",
                "endPeriod": f"{end_year}"
            }
            
            response = requests.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()  # Raise error for bad status codes
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched employment data: {len(data.get('CompactData', {}).get('DataSet', {}).get('Series', []))} series")
                df = self._process_employment_response(data)
                return df
            else:
                logger.error(f"Failed to fetch employment rate data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error occurred while fetching employment rate data: {str(e)}")
            return pd.DataFrame()
            
    def fetch_inflation_data(self, countries: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """Fetch inflation rate data
        
        Args:
            countries: List of country codes
            start_year: Start year
            end_year: End year
            
        Returns:
            DataFrame containing inflation rate data
        """
        try:
            # Using International Financial Statistics (IFS) database for inflation data
            # PCPI_PC_CP_A_PT: Consumer Prices, All items, Percentage Change, Previous Period
            endpoint = "/CompactData/IFS/Q.DE+FR+IT+ES+NL+BE.PCPI_PC_CP_A_PT"
            params = {
                "startPeriod": f"{start_year}",
                "endPeriod": f"{end_year}"
            }
            
            response = requests.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()  # Raise error for bad status codes
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched inflation data: {len(data.get('CompactData', {}).get('DataSet', {}).get('Series', []))} series")
                df = self._process_inflation_response(data)
                return df
            else:
                logger.error(f"Failed to fetch inflation rate data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error occurred while fetching inflation rate data: {str(e)}")
            return pd.DataFrame()
            
    def _process_gdp_response(self, data: Dict) -> pd.DataFrame:
        """Process GDP data response
        
        Args:
            data: IMF API returned JSON data
            
        Returns:
            Processed DataFrame containing GDP data
        """
        try:
            # Extract the data series from the response
            series = data.get('CompactData', {}).get('DataSet', {}).get('Series', [])
            if not series:
                return pd.DataFrame()
                
            # Process each series into records
            records = []
            for s in series:
                country = s.get('@REF_AREA', '')
                obs = s.get('Obs', [])
                if not isinstance(obs, list):
                    obs = [obs]
                    
                for ob in obs:
                    if ob is None:
                        continue
                    try:
                        record = {
                            'country': country,
                            'date': ob.get('@TIME_PERIOD', ''),
                            'value': float(ob.get('@OBS_VALUE', 0))
                        }
                        records.append(record)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not process observation for {country}: {str(e)}")
                        continue
                    
            # Create DataFrame
            df = pd.DataFrame(records)
            if df.empty:
                return df
                
            # Convert date to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Sort by country and date
            df = df.sort_values(['country', 'date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing GDP data: {str(e)}")
            return pd.DataFrame()
            
    def _process_employment_response(self, data: Dict) -> pd.DataFrame:
        """Process employment rate data response
        
        Args:
            data: IMF API returned JSON data
            
        Returns:
            Processed DataFrame containing employment rate data
        """
        try:
            # Extract the data series from the response
            series = data.get('CompactData', {}).get('DataSet', {}).get('Series', [])
            if not series:
                return pd.DataFrame()
                
            # Process each series into records
            records = []
            for s in series:
                country = s.get('@REF_AREA', '')
                obs = s.get('Obs', [])
                if not isinstance(obs, list):
                    obs = [obs]
                    
                for ob in obs:
                    if ob is None:
                        continue
                    try:
                        record = {
                            'country': country,
                            'date': ob.get('@TIME_PERIOD', ''),
                            'value': float(ob.get('@OBS_VALUE', 0))
                        }
                        records.append(record)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not process observation for {country}: {str(e)}")
                        continue
                    
            # Create DataFrame
            df = pd.DataFrame(records)
            if df.empty:
                return df
                
            # Convert date to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Sort by country and date
            df = df.sort_values(['country', 'date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing employment rate data: {str(e)}")
            return pd.DataFrame()
            
    def _process_inflation_response(self, data: Dict) -> pd.DataFrame:
        """Process inflation rate data response
        
        Args:
            data: IMF API returned JSON data
            
        Returns:
            Processed DataFrame containing inflation rate data
        """
        try:
            # Extract the data series from the response
            series = data.get('CompactData', {}).get('DataSet', {}).get('Series', [])
            if not series:
                return pd.DataFrame()
                
            # Process each series into records
            records = []
            for s in series:
                country = s.get('@REF_AREA', '')
                obs = s.get('Obs', [])
                if not isinstance(obs, list):
                    obs = [obs]
                    
                for ob in obs:
                    if ob is None:
                        continue
                    try:
                        record = {
                            'country': country,
                            'date': ob.get('@TIME_PERIOD', ''),
                            'value': float(ob.get('@OBS_VALUE', 0))
                        }
                        records.append(record)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not process observation for {country}: {str(e)}")
                        continue
                    
            # Create DataFrame
            df = pd.DataFrame(records)
            if df.empty:
                return df
                
            # Convert date to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Sort by country and date
            df = df.sort_values(['country', 'date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing inflation rate data: {str(e)}")
            return pd.DataFrame()
