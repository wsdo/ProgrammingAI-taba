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
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager for handling PostgreSQL and MongoDB connections."""
    
    def __init__(self):
        """Initialize database manager"""
        self.pg_conn = None
        self.pg_engine = None
        self.mongo_client = None
        self.mongo_db = None
        
        # For testing with SQLite
        self.sqlite_conn = None
        self.sqlite_engine = None
        
        if os.getenv('TESTING') == 'true' and os.getenv('DB_TYPE') == 'sqlite':
            self.use_sqlite = True
            self.sqlite_path = os.getenv('SQLITE_DB_PATH')
        else:
            self.use_sqlite = False
        
        # Get database credentials from environment variables with no defaults
        self.pg_host = os.getenv('POSTGRES_HOST')
        self.pg_port = int(os.getenv('POSTGRES_PORT', '5432'))  # Only port has a default
        self.pg_db = os.getenv('POSTGRES_DB')
        self.pg_user = os.getenv('POSTGRES_USER')
        self.pg_password = os.getenv('POSTGRES_PASSWORD')
        
        self.mongo_uri = os.getenv('MONGODB_URI')
        self.mongo_db_name = os.getenv('MONGODB_DB')
    
    def connect_postgres(self):
        """Connect to PostgreSQL database"""
        if self.use_sqlite:
            logger.info("Using SQLite for testing")
            return

        try:
            # Validate required environment variables
            if not all([self.pg_host, self.pg_db, self.pg_user, self.pg_password]):
                missing_vars = [var for var, val in {
                    'POSTGRES_HOST': self.pg_host,
                    'POSTGRES_DB': self.pg_db,
                    'POSTGRES_USER': self.pg_user,
                    'POSTGRES_PASSWORD': self.pg_password
                }.items() if not val]
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

            if not self.pg_conn:
                self.pg_engine = create_engine(
                    f"postgresql://{self.pg_user}:{self.pg_password}@"
                    f"{self.pg_host}:{self.pg_port}/{self.pg_db}"
                )
                self.pg_conn = self.pg_engine.connect()

                logger.info("Successfully connected to PostgreSQL")

        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            if self.pg_engine:
                self.pg_engine.dispose()
            self.pg_engine = None
            self.pg_conn = None
            raise
    
    def connect_mongo(self) -> None:
        """Connect to MongoDB database"""
        try:
            # Get individual MongoDB credentials
            mongo_host = os.getenv('MONGODB_HOST')
            mongo_port = int(os.getenv('MONGODB_PORT', '27017'))
            mongo_user = os.getenv('MONGODB_USER')
            mongo_password = os.getenv('MONGODB_PASSWORD')
            mongo_db = os.getenv('MONGODB_DB')
            
            # Log connection attempt details (without sensitive info)
            logger.info(f"Attempting to connect to MongoDB at {mongo_host}:{mongo_port}")
            
            # Create a simple connection string
            connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}"
            
            # Connect to MongoDB
            self.mongo_client = MongoClient(connection_string)
            
            # Select the database
            self.mongo_db = self.mongo_client[mongo_db]
            
            # Test connection with the specific database
            self.mongo_db.command('ping')
            
            logger.info("Successfully connected to MongoDB")
            
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            if self.mongo_client:
                self.mongo_client.close()
            self.mongo_client = None
            self.mongo_db = None
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
            
            # Reset the table before inserting new data
            with self.pg_engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))
                conn.commit()
            
            # Prepare data for insertion
            if table_name == 'education_data':
                # Melt the wide format data into long format
                id_vars = ['freq', 'unit', 'isced11', 'geo\\TIME_PERIOD']
                value_vars = [str(year) for year in range(2012, 2022)]
                melted_data = data.melt(
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name='year',
                    value_name='value'
                )
                
                # Rename columns to match database schema
                melted_data = melted_data.rename(columns={
                    'geo\\TIME_PERIOD': 'geo_time_period'
                })
                
                # Add metadata columns
                melted_data['collected_at'] = pd.Timestamp.now()
                melted_data['source'] = 'Eurostat'
                
                # Convert year to integer
                melted_data['year'] = melted_data['year'].astype(int)
                
                # Remove rows with null values
                melted_data = melted_data.dropna(subset=['value'])
                
                data_to_save = melted_data
                
            elif table_name == 'economic_data':
                # Prepare economic data
                data_to_save = data.copy()
                data_to_save['collected_at'] = pd.Timestamp.now()
                data_to_save['source'] = 'World Bank'
            
            # Save data in batches
            total_rows = len(data_to_save)
            num_batches = (total_rows + batch_size - 1) // batch_size
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch = data_to_save.iloc[start_idx:end_idx]
                
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
            if self.mongo_db is None:
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
        """Execute SQL query and return results as DataFrame"""
        try:
            if not self.pg_conn:
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
            if self.mongo_db is None:
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
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            if not self.pg_conn:
                self.connect_postgres()
            
            # Get the appropriate connection
            engine = self.pg_engine
            
            # Create tables using SQLAlchemy
            with engine.connect() as conn:
                # Drop existing tables if they exist
                conn.execute(text("DROP TABLE IF EXISTS education_data"))
                conn.execute(text("DROP TABLE IF EXISTS economic_data"))
                
                # Create education_data table with all necessary columns
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS education_data (
                        id SERIAL PRIMARY KEY,
                        freq VARCHAR(10),
                        unit VARCHAR(20),
                        isced11 VARCHAR(10),
                        geo_time_period VARCHAR(10),
                        year INTEGER,
                        value FLOAT,
                        collected_at TIMESTAMP,
                        source VARCHAR(50)
                    )
                """))
                
                # Create economic_data table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS economic_data (
                        id SERIAL PRIMARY KEY,
                        country_code VARCHAR(10),
                        year INTEGER,
                        gdp_growth FLOAT,
                        employment_rate FLOAT,
                        gdp_per_capita FLOAT,
                        industry_value FLOAT,
                        collected_at TIMESTAMP,
                        source VARCHAR(50)
                    )
                """))
                
                conn.commit()
                
            logger.info("Tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
            
    def insert_economic_data(self, data: pd.DataFrame):
        """Insert economic data into database"""
        try:
            # Save data using the save_to_postgres method
            self.save_to_postgres('economic_data', data)
            logger.info(f"Successfully inserted {len(data)} rows of economic data")
        except Exception as e:
            logger.error(f"Error inserting economic data: {str(e)}")
            raise
    
    def insert_education_data(self, data: pd.DataFrame):
        """Insert education data into database.
        
        Args:
            data (pd.DataFrame): DataFrame containing education data
        """
        try:
            # Save data using the save_to_postgres method
            self.save_to_postgres('education_data', data)
            logger.info(f"Successfully inserted {len(data)} rows of education data")
        except Exception as e:
            logger.error(f"Error inserting education data: {str(e)}")
            raise
    
    def get_economic_data(self) -> pd.DataFrame:
        """Retrieve economic data from database"""
        try:
            if not self.pg_conn:
                self.connect_postgres()
            
            query = "SELECT * FROM economic_data"
            return pd.read_sql_query(query, self.pg_engine)
        except Exception as e:
            logger.error(f"Error retrieving economic data: {str(e)}")
            return pd.DataFrame()
    
    def close_connections(self) -> None:
        """Close all database connections."""
        try:
            if self.pg_conn:
                self.pg_conn.close()
                self.pg_conn = None
                
            if self.mongo_client:
                self.mongo_client.close()
                self.mongo_client = None
                
            logger.info("All database connections closed")
            
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")
            raise
