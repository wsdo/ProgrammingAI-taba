"""
Database Management Module
"""

from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        """Initialize database connections"""
        # PostgreSQL connection
        self.pg_engine = create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        
    def load_from_postgres(self, table_name: str, conditions: Optional[Dict] = None) -> pd.DataFrame:
        """Load data from PostgreSQL
        
        Args:
            table_name: Table name to query
            conditions: Query conditions (country_code, date range)
            
        Returns:
            DataFrame with query results
        """
        try:
            # Build query
            query = f"SELECT * FROM {table_name}"
            
            if conditions:
                where_clauses = []
                
                if 'country_code' in conditions:
                    countries = "', '".join(conditions['country_code'])
                    where_clauses.append(f"country_code IN ('{countries}')")
                
                if 'date' in conditions:
                    start_date, end_date = conditions['date']
                    where_clauses.append(
                        f"date BETWEEN '{start_date}' AND '{end_date}'"
                    )
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
            
            # Execute query
            df = pd.read_sql(query, self.pg_engine)
            logger.info(f"Data loaded from PostgreSQL table: {table_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from PostgreSQL: {str(e)}")
            raise
            
    def save_to_postgres(self, data: pd.DataFrame, data_type: str):
        """Save data to PostgreSQL
        
        Args:
            data: DataFrame with columns [country, date, value]
            data_type: Type of data (gdp, employment, inflation)
        """
        try:
            # Prepare data
            df = data.copy()
            df['country_code'] = df['country']
            df = df.drop('country', axis=1)
            
            # Save to PostgreSQL
            table_name = f"economic_data.{data_type}"
            df.to_sql(
                name=data_type,
                schema='economic_data',
                con=self.pg_engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            logger.info(f"Data saved to PostgreSQL table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {str(e)}")
            raise
            
    def save_to_mongodb(self, gdp_df: pd.DataFrame, emp_df: pd.DataFrame, inf_df: pd.DataFrame):
        """Save data to MongoDB for future reference
        
        Args:
            gdp_df: GDP data
            emp_df: Employment data
            inf_df: Inflation data
        """
        try:
            logger.info("MongoDB operations temporarily disabled")
            return
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise

    def save_analysis_results(self, results: Dict, table_name: str = 'economic_data.analysis_results'):
        """Save analysis results to PostgreSQL
        
        Args:
            results: Dictionary containing analysis results
            table_name: Target table name
        """
        try:
            # Convert results to DataFrame
            df = pd.DataFrame()
            for country, data in results.items():
                country_df = pd.DataFrame({
                    'country_code': country,
                    'analysis_date': datetime.now(),
                    'gdp_growth': [data['gdp_growth']],
                    'employment_change': [data['employment_change']],
                    'inflation_change': [data['inflation_change']],
                    'gdp_emp_corr': [data['correlations']['gdp_employment']],
                    'gdp_inf_corr': [data['correlations']['gdp_inflation']],
                    'emp_inf_corr': [data['correlations']['employment_inflation']]
                })
                df = pd.concat([df, country_df], ignore_index=True)
            
            # Save to PostgreSQL
            df.to_sql(
                table_name.split('.')[-1],
                self.pg_engine,
                schema=table_name.split('.')[0],
                if_exists='append',
                index=False
            )
            logger.info(f"Analysis results saved to {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
            raise
