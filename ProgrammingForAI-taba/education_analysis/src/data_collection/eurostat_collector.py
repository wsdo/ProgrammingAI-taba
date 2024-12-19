"""
Module for collecting education and economic data from Eurostat and other sources.
"""

import os
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import eurostat
import wbgapi as wb
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EurostatCollector:
    """Class for collecting education and economic data from Eurostat and other sources"""
    
    def __init__(self):
        """Initialize the collector"""
        load_dotenv()
        
        # Eurostat dataset codes
        self.EDUCATION_INVESTMENT_CODE = 'educ_uoe_fine09'
        self.STUDENT_TEACHER_RATIO_CODE = 'educ_uoe_perp04'
        
        # World Bank indicator codes
        self.WB_INDICATORS = {
            'gdp_growth': 'NY.GDP.MKTP.KD.ZG',
            'employment_rate': 'SL.EMP.TOTL.SP.ZS',
            'gdp_per_capita': 'NY.GDP.PCAP.CD',
            'industry_value': 'NV.IND.TOTL.ZS'
        }
        
        # EU country codes
        self.EU_COUNTRIES = [
            'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST',
            'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA',
            'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK',
            'SVN', 'ESP', 'SWE'
        ]
        
        # EU education policy document URL
        self.EU_POLICY_URL = 'https://education.ec.europa.eu/education-levels'
        
        self.cache_dir = Path(__file__).parent.parent.parent / 'data' / 'cache'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_expiry = timedelta(days=1)  # Cache expires after 1 day

    def _get_cached_data(self, cache_file: str) -> pd.DataFrame | None:
        """Get data from cache if it exists and is not expired."""
        cache_path = self.cache_dir / cache_file
        if cache_path.exists():
            # Check if cache is expired
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
            if datetime.now() - mtime < self.cache_expiry:
                try:
                    return pd.read_csv(cache_path)
                except Exception as e:
                    logger.error(f"Error reading cache file {cache_file}: {str(e)}")
        return None

    def _save_to_cache(self, data: pd.DataFrame, cache_file: str) -> None:
        """Save data to cache."""
        try:
            cache_path = self.cache_dir / cache_file
            data.to_csv(cache_path, index=False)
            logger.info(f"Saved data to cache: {cache_file}")
        except Exception as e:
            logger.error(f"Error saving to cache {cache_file}: {str(e)}")

    def get_education_investment_data(self) -> pd.DataFrame:
        """
        Get education investment data from Eurostat
        
        Returns:
            DataFrame containing education investment data
        """
        logger.info("Getting education investment data...")
        
        # Try to get from cache first
        cached_data = self._get_cached_data('education_investment.csv')
        if cached_data is not None:
            logger.info("Using cached education investment data")
            return cached_data
        
        try:
            # Get raw data
            data = eurostat.get_data_df(self.EDUCATION_INVESTMENT_CODE)
            
            # Process data
            data = data.reset_index()
            data = data.rename(columns={
                'geo': 'country_code',
                'time': 'year',
                'values': 'education_investment'
            })
            
            # Add metadata
            data['collected_at'] = datetime.now()
            data['source'] = 'Eurostat'
            
            # Save to cache
            self._save_to_cache(data, 'education_investment.csv')
            
            logger.info(f"Successfully got education investment data: {len(data)} records")
            return data
            
        except Exception as e:
            logger.error(f"Error getting education investment data: {str(e)}")
            raise
    
    def get_economic_indicators(self) -> pd.DataFrame:
        """
        Get economic indicators data from World Bank
        
        Returns:
            DataFrame containing economic indicators data
        """
        logger.info("Getting economic indicators data...")
        
        # Try to get from cache first
        cached_data = self._get_cached_data('economic_indicators.csv')
        if cached_data is not None:
            logger.info("Using cached economic indicators data")
            return cached_data
            
        try:
            # Get data for all indicators
            dfs = []
            for indicator_name, indicator_code in self.WB_INDICATORS.items():
                logger.info(f"Fetching data for indicator: {indicator_name} ({indicator_code})")
                
                try:
                    # Get data from World Bank API
                    df = wb.data.DataFrame(
                        indicator_code,
                        self.EU_COUNTRIES,
                        time=range(2010, 2024)
                    )
                    
                    if df is None or df.empty:
                        logger.warning(f"No data received for {indicator_name}")
                        continue
                    
                    # Reset index to convert multi-index to columns
                    df = df.reset_index()
                    
                    # Melt the DataFrame to convert from wide to long format
                    df = pd.melt(
                        df,
                        id_vars=['economy'],
                        var_name='year',
                        value_name=indicator_name
                    )
                    
                    # Clean up year column (remove 'YR' prefix)
                    df['year'] = df['year'].str.replace('YR', '').astype(int)
                    
                    # Rename columns
                    df = df.rename(columns={'economy': 'country_code'})
                    
                    # Keep only necessary columns
                    df = df[['country_code', 'year', indicator_name]]
                    
                    dfs.append(df)
                    logger.info(f"Successfully processed {indicator_name} data with {len(df)} records")
                    
                except Exception as e:
                    logger.error(f"Error processing {indicator_name}: {str(e)}")
                    continue
            
            if not dfs:
                logger.warning("No economic indicators data was collected")
                return pd.DataFrame()
            
            # Merge all indicator dataframes
            result = dfs[0]
            for i, df in enumerate(dfs[1:], 1):
                logger.info(f"Merging dataframe {i} with shape {df.shape}")
                result = pd.merge(result, df, on=['country_code', 'year'], how='outer')
            
            # Add metadata
            result['collected_at'] = datetime.now()
            result['source'] = 'World Bank'
            
            # Save to cache
            self._save_to_cache(result, 'economic_indicators.csv')
            
            logger.info(f"Successfully got economic indicators data: {len(result)} records")
            return result
            
        except Exception as e:
            logger.error(f"Error getting economic indicators data: {str(e)}")
            print(f"Error getting economic data: {str(e)}")
            return pd.DataFrame()
    
    def get_education_policies(self) -> List[Dict]:
        """
        Get education policy documents from OECD and EU sources
        
        Returns:
            List of dictionaries containing policy information
        """
        logger.info("Getting education policies...")
        
        # Try to get from cache first
        cache_file = self.cache_dir / 'education_policies.json'
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    policies = json.load(f)
                logger.info(f"Using cached education policies: {len(policies)} documents")
                return policies
            except Exception as e:
                logger.error(f"Error reading cached policies: {str(e)}")
                if cache_file.exists():
                    cache_file.unlink()  # Delete invalid cache file
        
        try:
            policies = []
            
            # Get education data to determine available years
            education_data = self.get_education_investment_data()
            available_years = sorted(education_data['year'].unique())
            start_year = min(available_years)
            end_year = max(available_years)
            
            # Policy data sources
            policy_sources = {
                'DE': {
                    'name': 'Germany',
                    'policies': [
                        {
                            'year': 2015,
                            'title': 'Digital Education Strategy',
                            'description': 'Implementation of digital learning tools and infrastructure in schools',
                            'source': 'Federal Ministry of Education and Research',
                            'url': 'https://www.bmbf.de/bmbf/en/education/digital-education/digital-education.html'
                        },
                        {
                            'year': 2018,
                            'title': 'Excellence Strategy',
                            'description': 'Strengthening research excellence in universities',
                            'source': 'German Research Foundation',
                            'url': 'https://www.dfg.de/en/research_funding/programmes/excellence_strategy/'
                        }
                    ]
                },
                'FR': {
                    'name': 'France',
                    'policies': [
                        {
                            'year': 2016,
                            'title': 'Education Reform Act',
                            'description': 'Comprehensive reform of primary and secondary education',
                            'source': 'Ministry of National Education',
                            'url': 'https://www.education.gouv.fr/'
                        },
                        {
                            'year': 2019,
                            'title': 'Higher Education Reform',
                            'description': 'Modernization of university system and access',
                            'source': 'Ministry of Higher Education',
                            'url': 'https://www.enseignementsup-recherche.gouv.fr/'
                        }
                    ]
                },
                'IT': {
                    'name': 'Italy',
                    'policies': [
                        {
                            'year': 2017,
                            'title': 'La Buona Scuola',
                            'description': 'School system reform focusing on teacher training and digital skills',
                            'source': 'Ministry of Education',
                            'url': 'https://www.miur.gov.it/'
                        }
                    ]
                },
                'ES': {
                    'name': 'Spain',
                    'policies': [
                        {
                            'year': 2018,
                            'title': 'LOMLOE Education Law',
                            'description': 'Reform focusing on equity and quality in education',
                            'source': 'Ministry of Education',
                            'url': 'http://www.educacionyfp.gob.es/'
                        }
                    ]
                },
                'PL': {
                    'name': 'Poland',
                    'policies': [
                        {
                            'year': 2016,
                            'title': 'Education System Reform',
                            'description': 'Structural changes in school organization and curriculum',
                            'source': 'Ministry of Education',
                            'url': 'https://www.gov.pl/web/education'
                        }
                    ]
                }
            }
            
            # Process each country's policies
            for country_code, data in policy_sources.items():
                for policy in data['policies']:
                    if start_year <= policy['year'] <= end_year:
                        policy_doc = {
                            'country': country_code,
                            'year': policy['year'],
                            'title': policy['title'],
                            'description': policy['description'],
                            'source': policy['source'],
                            'url': policy['url']
                        }
                        policies.append(policy_doc)
                        logger.info(f"Added policy for {country_code} from {policy['year']}: {policy['title']}")
            
            # Save to cache
            try:
                with open(cache_file, 'w') as f:
                    json.dump(policies, f)
                logger.info(f"Saved {len(policies)} policies to cache")
            except Exception as e:
                logger.error(f"Error saving policies to cache: {str(e)}")
            
            return policies
            
        except Exception as e:
            logger.error(f"Error getting education policies: {str(e)}")
            return []

    def collect_education_data(self) -> pd.DataFrame:
        """
        Collect education data from Eurostat.
        
        Returns:
            DataFrame containing education data including investment and student-teacher ratios
        """
        logger.info("Collecting education data...")
        
        try:
            # Get education investment data
            investment_data = self.get_education_investment_data()
            
            # Get student-teacher ratio data
            ratio_data = self.get_student_teacher_ratio()
            
            # Merge the datasets
            education_data = pd.merge(
                investment_data,
                ratio_data,
                on=['country_code', 'year'],
                how='outer'
            )
            
            # Add collection timestamp
            education_data['collected_at'] = datetime.now()
            education_data['source'] = 'eurostat'
            
            logger.info("Successfully collected education data")
            return education_data
            
        except Exception as e:
            logger.error(f"Error collecting education data: {str(e)}")
            raise

    def save_data_locally(self, 
                         data: pd.DataFrame, 
                         filename: str,
                         directory: str = 'data') -> None:
        """
        Save data to local file
        
        Args:
            data: Data to save
            filename: File name
            directory: Save directory
        """
        try:
            # Ensure directory exists
            os.makedirs(directory, exist_ok=True)
            
            # Construct full file path
            filepath = os.path.join(directory, filename)
            
            # Save data based on file extension
            if filename.endswith('.csv'):
                data.to_csv(filepath, index=False)
            elif filename.endswith('.parquet'):
                data.to_parquet(filepath, index=False)
            elif filename.endswith('.json'):
                data.to_json(filepath, orient='records')
            else:
                raise ValueError(f"Unsupported file format: {filename}")
            
            logger.info(f"Data saved to: {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving data to local file: {str(e)}")
            raise
