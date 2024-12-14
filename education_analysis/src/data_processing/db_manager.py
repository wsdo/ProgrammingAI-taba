"""
Module for managing database connections and operations.
Handles both PostgreSQL (structured data) and MongoDB (unstructured data).
"""

import os
import logging
import pandas as pd
import psycopg2
from pymongo import MongoClient
from datetime import datetime
from typing import List, Dict, Optional, Union
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager for handling PostgreSQL and MongoDB connections."""
    
    def __init__(self):
        """Initialize database connections."""
        self.pg_conn = None
        self.pg_engine = None
        self.mongo_client = None
        self.mongo_db = None
        
        # Get database credentials from environment variables
        self.pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.pg_port = os.getenv('POSTGRES_PORT', '5432')
        self.pg_db = os.getenv('POSTGRES_DB', 'education_db')
        self.pg_user = os.getenv('POSTGRES_USER', 'postgres')
        self.pg_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        
        self.mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.mongo_db_name = os.getenv('MONGODB_DB', 'education_db')
    
    def connect_postgres(self) -> None:
        """Connect to PostgreSQL database."""
        try:
            # Create SQLAlchemy engine
            db_url = f'postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}'
            self.pg_engine = create_engine(db_url)
            
            # Also maintain psycopg2 connection for operations that need it
            self.pg_conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_password
            )
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise
    
    def connect_mongo(self) -> None:
        """Connect to MongoDB database."""
        try:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_db_name]
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            raise
    
    def save_to_postgres(self, table_name: str, data: pd.DataFrame, batch_size: int = 1000) -> None:
        """
        Save DataFrame to PostgreSQL table using batch processing.
        
        Args:
            table_name: Name of the target table
            data: DataFrame to save
            batch_size: Number of rows to insert in each batch
        """
        try:
            if not self.pg_engine:
                self.connect_postgres()
            
            # Calculate number of batches
            total_rows = len(data)
            num_batches = (total_rows + batch_size - 1) // batch_size
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch = data.iloc[start_idx:end_idx]
                
                # Save batch to database
                batch.to_sql(
                    name=table_name,
                    con=self.pg_engine,
                    if_exists='append',
                    index=False
                )
                logger.info(f"Saved batch {i+1} ({len(batch)} rows)")
            
            logger.info(f"Successfully saved {total_rows} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {str(e)}")
            raise
    
    def save_to_mongo(self, collection: str, documents: Union[Dict, List[Dict]]) -> None:
        """
        Save documents to MongoDB collection.
        
        Args:
            collection: Name of the target collection
            documents: Document or list of documents to save
        """
        try:
            if not self.mongo_db:
                self.connect_mongo()
            
            coll = self.mongo_db[collection]
            
            # Convert single document to list
            if isinstance(documents, dict):
                documents = [documents]
            
            # Add timestamp to documents
            for doc in documents:
                if isinstance(doc, dict) and 'created_at' not in doc:
                    doc['created_at'] = datetime.now()
            
            # Insert documents
            result = coll.insert_many(documents)
            logger.info(f"Successfully saved {len(documents)} documents to {collection}")
            
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {str(e)}")
            raise
    
    def query_postgres(self, query: str) -> pd.DataFrame:
        """
        Execute a query on PostgreSQL and return results as DataFrame.
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame containing query results
        """
        try:
            if not self.pg_engine:
                self.connect_postgres()
            
            return pd.read_sql_query(query, self.pg_engine)
            
        except Exception as e:
            logger.error(f"Error querying PostgreSQL: {str(e)}")
            raise
    
    def query_mongo(self, collection: str, query: Dict = None) -> List[Dict]:
        """
        Query documents from MongoDB collection.
        
        Args:
            collection: Name of the collection to query
            query: MongoDB query dictionary
        
        Returns:
            List of documents matching the query
        """
        try:
            if not self.mongo_db:
                self.connect_mongo()
            
            if query is None:
                query = {}
            
            coll = self.mongo_db[collection]
            return list(coll.find(query))
            
        except Exception as e:
            logger.error(f"Error querying MongoDB: {str(e)}")
            raise
    
    def get_education_data(self) -> pd.DataFrame:
        """
        Get education investment data from PostgreSQL.
        
        Returns:
            DataFrame containing education data
        """
        try:
            if not self.pg_engine:
                self.connect_postgres()
            
            query = """
                SELECT * FROM education_data 
                WHERE value IS NOT NULL 
                ORDER BY year, geo_time_period
            """
            
            data = pd.read_sql_query(query, self.pg_engine)
            logger.info(f"Retrieved {len(data)} rows of education data")
            return data
            
        except Exception as e:
            logger.error(f"Error retrieving education data: {str(e)}")
            raise
    
    def get_economic_data(self) -> pd.DataFrame:
        """
        Get economic data from PostgreSQL.
        
        Returns:
            DataFrame containing economic data
        """
        try:
            if not self.pg_engine:
                self.connect_postgres()
            
            query = """
                SELECT * FROM economic_data 
                WHERE gdp_growth IS NOT NULL 
                   OR employment_rate IS NOT NULL 
                   OR gdp_per_capita IS NOT NULL 
                   OR industry_value IS NOT NULL 
                ORDER BY year, country_code
            """
            
            data = pd.read_sql_query(query, self.pg_engine)
            logger.info(f"Retrieved {len(data)} rows of economic data")
            return data
            
        except Exception as e:
            logger.error(f"Error retrieving economic data: {str(e)}")
            raise
    
    def close_connections(self) -> None:
        """Close all database connections."""
        try:
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("PostgreSQL connection closed")
            
            if self.mongo_client:
                self.mongo_client.close()
                logger.info("MongoDB connection closed")
                
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")
            raise
