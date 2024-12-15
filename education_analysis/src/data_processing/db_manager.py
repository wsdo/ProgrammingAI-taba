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
        
        # Get database credentials from environment variables
        self.pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.pg_port = os.getenv('POSTGRES_PORT', '5432')
        self.pg_db = os.getenv('POSTGRES_DB', 'education_db')
        self.pg_user = os.getenv('POSTGRES_USER', 'postgres')
        self.pg_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        
        self.mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.mongo_db_name = os.getenv('MONGODB_DB', 'education_db')
    
    def connect_postgres(self):
        """Connect to PostgreSQL database"""
        try:
            if self.use_sqlite:
                self.sqlite_engine = create_engine(f'sqlite:///{self.sqlite_path}')
                self.sqlite_conn = self.sqlite_engine.connect()
                return
                
            if not self.pg_conn:
                self.pg_engine = create_engine(
                    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
                    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
                )
                self.pg_conn = self.pg_engine.connect()
                
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
            if not self.pg_engine and not self.sqlite_engine:
                self.connect_postgres()
            
            # Calculate number of batches
            total_rows = len(data)
            num_batches = (total_rows + batch_size - 1) // batch_size
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch = data.iloc[start_idx:end_idx]
                
                # Save batch to database
                if self.use_sqlite:
                    batch.to_sql(
                        name=table_name,
                        con=self.sqlite_engine,
                        if_exists='append',
                        index=False
                    )
                else:
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
        """Execute SQL query and return results as DataFrame"""
        try:
            if not self.pg_conn and not self.sqlite_conn:
                self.connect_postgres()
                
            if self.use_sqlite:
                return pd.read_sql_query(query, self.sqlite_engine)
            else:
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
            if not self.pg_engine and not self.sqlite_engine:
                self.connect_postgres()
            
            query = """
                SELECT * FROM education_data 
                WHERE value IS NOT NULL 
                ORDER BY year, geo_time_period
            """
            
            if self.use_sqlite:
                data = pd.read_sql_query(query, self.sqlite_engine)
            else:
                data = pd.read_sql_query(query, self.pg_engine)
            logger.info(f"Retrieved {len(data)} rows of education data")
            return data
            
        except Exception as e:
            logger.error(f"Error retrieving education data: {str(e)}")
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            if not self.pg_conn and not self.sqlite_conn:
                self.connect_postgres()
            
            # Get the appropriate connection
            engine = self.sqlite_engine if self.use_sqlite else self.pg_engine
            
            # Create tables using SQLAlchemy
            with engine.connect() as conn:
                if self.use_sqlite:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS education_data (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            geo_time_period TEXT,
                            year INTEGER,
                            value REAL,
                            source TEXT
                        )
                    """))
                    
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS economic_data (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            country TEXT,
                            year INTEGER,
                            gdp_growth REAL,
                            employment_rate REAL,
                            source TEXT
                        )
                    """))
                else:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS education_data (
                            id SERIAL PRIMARY KEY,
                            geo_time_period VARCHAR(10),
                            year INTEGER,
                            value FLOAT,
                            source VARCHAR(50)
                        )
                    """))
                    
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS economic_data (
                            id SERIAL PRIMARY KEY,
                            country VARCHAR(10),
                            year INTEGER,
                            gdp_growth FLOAT,
                            employment_rate FLOAT,
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
            if not self.pg_conn and not self.sqlite_conn:
                self.connect_postgres()
            
            if self.use_sqlite:
                conn = self.sqlite_conn
            else:
                conn = self.pg_conn
            
            for _, row in data.iterrows():
                conn.cursor().execute("""
                    INSERT INTO economic_data (country, year, gdp_growth, employment_rate, source)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row['country'],
                    row['year'],
                    row['gdp_growth'],
                    row['employment_rate'],
                    'IMF'
                ))
            if self.use_sqlite:
                conn.commit()
            else:
                conn.commit()
            logger.info("Economic data inserted successfully")
        except Exception as e:
            logger.error(f"Error inserting economic data: {str(e)}")
            if self.use_sqlite:
                conn.rollback()
            else:
                conn.rollback()

    def get_economic_data(self) -> pd.DataFrame:
        """Retrieve economic data from database"""
        try:
            if not self.pg_conn and not self.sqlite_conn:
                self.connect_postgres()
            
            if self.use_sqlite:
                query = "SELECT * FROM economic_data"
                return pd.read_sql_query(query, self.sqlite_engine)
            else:
                query = "SELECT * FROM economic_data"
                return pd.read_sql_query(query, self.pg_conn)
        except Exception as e:
            logger.error(f"Error retrieving economic data: {str(e)}")
            return pd.DataFrame()
    
    def insert_education_data(self, data: pd.DataFrame):
        """Insert education data into database.
        
        Args:
            data (pd.DataFrame): DataFrame containing education data
        """
        try:
            if not self.pg_conn and not self.sqlite_conn:
                self.connect_postgres()
                
            if self.use_sqlite:
                conn = self.sqlite_conn
            else:
                conn = self.pg_conn
                
            for _, row in data.iterrows():
                conn.cursor().execute("""
                    INSERT INTO education_data (geo_time_period, year, value, source)
                    VALUES (?, ?, ?, ?)
                """, (
                    row['geo_time_period'],
                    row['year'],
                    row['value'],
                    row['source']
                ))
            
            if self.use_sqlite:
                conn.commit()
            else:
                conn.commit()
            logger.info("Education data inserted successfully")
            
        except Exception as e:
            logger.error(f"Error inserting education data: {str(e)}")
            if self.use_sqlite:
                conn.rollback()
            else:
                conn.rollback()
    
    def close_connections(self) -> None:
        """Close all database connections."""
        try:
            if self.pg_conn:
                self.pg_conn.close()
                self.pg_conn = None
                
            if self.sqlite_conn:
                self.sqlite_conn.close()
                self.sqlite_conn = None
                
            if self.mongo_client:
                self.mongo_client.close()
                self.mongo_client = None
                
            logger.info("All database connections closed")
            
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")
            raise
