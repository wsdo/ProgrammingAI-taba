"""
Module for managing database connections and operations.
"""

import os
import pandas as pd
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Class to handle database connections and operations."""
    
    def __init__(self):
        """Initialize database connections."""
        # PostgreSQL connection parameters
        self.pg_params = {
            'user': os.getenv('POSTGRES_USER', 'nci'),
            'password': os.getenv('POSTGRES_PASSWORD', 'yHyULpyUXZ4y32gdEi80'),
            'host': os.getenv('POSTGRES_HOST', '47.91.31.227'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'education_db')
        }
        
        # MongoDB connection parameters
        self.mongo_params = {
            'host': os.getenv('MONGODB_HOST', '47.91.31.227'),
            'port': int(os.getenv('MONGODB_PORT', '27017')),
            'username': os.getenv('MONGODB_USER', 'nci'),
            'password': os.getenv('MONGODB_PASSWORD', 'xJcTB7fnyA17GNuQk3Aa'),
            'database': os.getenv('MONGODB_DB', 'education_db')
        }
        
        self.pg_conn = None
        self.mongo_client = None
        self.mongo_db = None
    
    def connect_postgres(self) -> bool:
        """
        Establish connection to PostgreSQL database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.pg_conn = psycopg2.connect(**self.pg_params)
            logger.info("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            return False
    
    def connect_mongodb(self) -> bool:
        """
        Establish connection to MongoDB database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            mongo_uri = (f"mongodb://{self.mongo_params['username']}:{self.mongo_params['password']}"
                        f"@{self.mongo_params['host']}:{self.mongo_params['port']}")
            self.mongo_client = MongoClient(mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_params['database']]
            logger.info("Successfully connected to MongoDB")
            return True
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            return False
    
    def setup_postgres_tables(self) -> bool:
        """
        Set up necessary PostgreSQL tables.
        
        Returns:
            bool: True if setup successful, False otherwise
        """
        try:
            if not self.pg_conn:
                if not self.connect_postgres():
                    return False
            
            with self.pg_conn.cursor() as cur:
                # Create tables for different education indicators
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS education_data (
                        id SERIAL PRIMARY KEY,
                        indicator_code VARCHAR(50),
                        country_code VARCHAR(10),
                        year INTEGER,
                        value FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create index for faster queries
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_education_data 
                    ON education_data(indicator_code, country_code, year)
                """)
                
                self.pg_conn.commit()
                logger.info("Successfully set up PostgreSQL tables")
                return True
                
        except Exception as e:
            logger.error(f"Error setting up PostgreSQL tables: {str(e)}")
            return False
    
    def store_in_postgres(self, df: pd.DataFrame, indicator_code: str) -> bool:
        """
        Store data in PostgreSQL.
        
        Args:
            df (pd.DataFrame): Data to store
            indicator_code (str): Indicator code for the data
            
        Returns:
            bool: True if storage successful, False otherwise
        """
        try:
            if not self.pg_conn:
                if not self.connect_postgres():
                    return False
            
            # Prepare data for insertion
            data = [(indicator_code, row['geo'], row['time'], row['values']) 
                   for _, row in df.iterrows()]
            
            with self.pg_conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO education_data (indicator_code, country_code, year, value)
                    VALUES %s
                    ON CONFLICT (indicator_code, country_code, year) 
                    DO UPDATE SET value = EXCLUDED.value
                """, data)
                
            self.pg_conn.commit()
            logger.info(f"Successfully stored {len(data)} records in PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Error storing data in PostgreSQL: {str(e)}")
            return False
    
    def store_in_mongodb(self, data: Dict[str, Any], collection: str) -> bool:
        """
        Store data in MongoDB.
        
        Args:
            data (Dict[str, Any]): Data to store
            collection (str): Collection name
            
        Returns:
            bool: True if storage successful, False otherwise
        """
        try:
            if not self.mongo_db:
                if not self.connect_mongodb():
                    return False
            
            collection = self.mongo_db[collection]
            
            # Insert or update data
            result = collection.update_one(
                {'_id': data.get('_id', data.get('indicator_code'))},
                {'$set': data},
                upsert=True
            )
            
            logger.info(f"Successfully stored/updated document in MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Error storing data in MongoDB: {str(e)}")
            return False
    
    def fetch_from_postgres(self, indicator_code: str, 
                          countries: Optional[list] = None) -> Optional[pd.DataFrame]:
        """
        Fetch data from PostgreSQL.
        
        Args:
            indicator_code (str): Indicator code to fetch
            countries (list, optional): List of country codes to filter
            
        Returns:
            pd.DataFrame: Retrieved data or None if fetch fails
        """
        try:
            if not self.pg_conn:
                if not self.connect_postgres():
                    return None
            
            query = """
                SELECT country_code, year, value 
                FROM education_data 
                WHERE indicator_code = %s
            """
            params = [indicator_code]
            
            if countries:
                query += " AND country_code = ANY(%s)"
                params.append(countries)
            
            return pd.read_sql(query, self.pg_conn, params=params)
            
        except Exception as e:
            logger.error(f"Error fetching data from PostgreSQL: {str(e)}")
            return None
    
    def fetch_from_mongodb(self, collection: str, 
                         query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch data from MongoDB.
        
        Args:
            collection (str): Collection to fetch from
            query (Dict[str, Any]): Query parameters
            
        Returns:
            Dict[str, Any]: Retrieved data or None if fetch fails
        """
        try:
            if not self.mongo_db:
                if not self.connect_mongodb():
                    return None
            
            collection = self.mongo_db[collection]
            return collection.find_one(query)
            
        except Exception as e:
            logger.error(f"Error fetching data from MongoDB: {str(e)}")
            return None
    
    def close_connections(self):
        """Close all database connections."""
        try:
            if self.pg_conn:
                self.pg_conn.close()
            if self.mongo_client:
                self.mongo_client.close()
            logger.info("Closed all database connections")
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")

def main():
    """Main function to demonstrate usage."""
    db_manager = DatabaseManager()
    
    # Test PostgreSQL connection and setup
    if db_manager.connect_postgres():
        print("PostgreSQL connection successful")
        if db_manager.setup_postgres_tables():
            print("PostgreSQL tables setup successful")
    
    # Test MongoDB connection
    if db_manager.connect_mongodb():
        print("MongoDB connection successful")
    
    # Close connections
    db_manager.close_connections()

if __name__ == "__main__":
    main()
