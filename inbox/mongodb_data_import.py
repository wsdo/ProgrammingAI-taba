"""
MongoDB Data Import Script for Education Data Analysis
This script handles the import of education data into MongoDB collections.
"""

import pandas as pd
import numpy as np
import eurostat
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import os
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MongoDB configuration
MONGODB_HOST = os.getenv('MONGODB_HOST')
MONGODB_PORT = int(os.getenv('MONGODB_PORT'))
MONGODB_USER = os.getenv('MONGODB_USER')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')
MONGODB_DB = os.getenv('MONGODB_DB')

def get_mongodb_connection():
    """Create a connection to MongoDB database"""
    try:
        # Create client with authentication options
        client = MongoClient(
            host=MONGODB_HOST,
            port=MONGODB_PORT,
            username=MONGODB_USER,
            password=MONGODB_PASSWORD,
            authSource='admin',
            serverSelectionTimeoutMS=5000
        )
        db = client[MONGODB_DB]
        # Test the connection
        client.server_info()
        logger.info("Successfully connected to MongoDB")
        return db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return None

def process_dataset(df, metric, info):
    """Process a single dataset"""
    logger.info(f"Processing {metric} dataset...")
    
    try:
        # Reset index and get time column
        df = df.reset_index()
        time_col = [col for col in df.columns if 'TIME' in col][0]
        logger.info(f"Found time column: {time_col}")
        
        # Extract country
        df['country'] = df[time_col].str.split('\\').str[0]
        
        # Get year columns
        year_columns = [col for col in df.columns if str(col).isdigit()]
        logger.info(f"Found {len(year_columns)} year columns")
        
        processed_data = []
        for year in tqdm(year_columns, desc=f"Processing years for {metric}"):
            try:
                year_data = df[['country', year] + [col for col in df.columns if col not in year_columns and col != time_col]]
                year_data = year_data.rename(columns={year: 'value'})
                year_data['year'] = int(year)
                processed_data.append(year_data)
            except Exception as e:
                logger.error(f"Error processing year {year}: {str(e)}")
                continue
        
        # Combine all years
        df_processed = pd.concat(processed_data, ignore_index=True)
        
        # Convert value column to float
        df_processed['value'] = pd.to_numeric(df_processed['value'], errors='coerce')
        df_processed = df_processed.dropna(subset=['value'])
        
        logger.info(f"Successfully processed {len(df_processed)} records for {metric}")
        return df_processed
    
    except Exception as e:
        logger.error(f"Error processing dataset {metric}: {str(e)}")
        return None

def store_in_mongodb(collection, df_processed, metric, info):
    """Store processed data in MongoDB using batch operations"""
    logger.info(f"Storing {metric} data in MongoDB...")
    
    successful_inserts = 0
    errors = 0
    batch_size = 1000  # Increased batch size
    
    # Create indexes
    collection.create_index([("country", 1), ("year", 1)], unique=True)
    
    # Prepare batches
    total_records = len(df_processed)
    batches = [df_processed[i:i + batch_size] for i in range(0, total_records, batch_size)]
    
    # Process each batch
    for batch in tqdm(batches, desc=f"Storing {metric} data"):
        operations = []
        
        for _, record in batch.iterrows():
            try:
                mongo_doc = {
                    'country': str(record['country']),
                    'year': int(record['year']),
                    'value': float(record['value']),
                    'metadata': {
                        'freq': str(record.get('freq', '')),
                        'unit': str(record.get('unit', '')),
                        'isced11': str(record.get('isced11', '')),
                        'description': info['description'],
                        'unit_type': info['unit'],
                        'source': info['source']
                    },
                    'updated_at': datetime.now()
                }
                
                operations.append(UpdateOne(
                    {'country': mongo_doc['country'], 'year': mongo_doc['year']},
                    {'$set': mongo_doc},
                    upsert=True
                ))
                
            except Exception as e:
                logger.error(f"Error preparing record: {str(e)}")
                errors += 1
                continue
        
        # Execute batch operation
        if operations:
            try:
                result = collection.bulk_write(operations, ordered=False)
                successful_inserts += result.upserted_count + result.modified_count
                
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {str(bwe.details)}")
                errors += len(bwe.details['writeErrors'])
                successful_inserts += bwe.details['nInserted']
            
            except Exception as e:
                logger.error(f"Error executing batch: {str(e)}")
                errors += len(operations)
                continue
        
        # Small delay between batches to prevent overwhelming server
        time.sleep(0.01)  # Reduced delay to 10ms
    
    logger.info(f"Completed storing {metric} data: {successful_inserts} successful, {errors} errors")
    return successful_inserts, errors

def import_education_data():
    """Import education data into MongoDB"""
    datasets = {
        'education_investment': {
            'code': 'educ_uoe_fine09',
            'description': 'Annual expenditure on educational institutions per student',
            'unit': 'EUR per student',
            'source': 'Eurostat'
        },
        'student_teacher_ratio': {
            'code': 'educ_uoe_perp04',
            'description': 'Ratio of students to teachers',
            'unit': 'Ratio',
            'source': 'Eurostat'
        },
        'completion_rate': {
            'code': 'edat_lfse_03',
            'description': 'Early leavers from education and training',
            'unit': 'Percentage',
            'source': 'Eurostat'
        },
        'literacy_rate': {
            'code': 'edat_lfse_01',
            'description': 'Population by educational attainment level',
            'unit': 'Percentage',
            'source': 'Eurostat'
        }
    }
    
    # Connect to MongoDB
    mongo_db = get_mongodb_connection()
    if mongo_db is None:
        return
    
    total_records = 0
    total_errors = 0
    
    for metric, info in datasets.items():
        try:
            logger.info(f"\nProcessing {metric} dataset...")
            
            # Get data from Eurostat
            logger.info(f"Fetching data from Eurostat for {metric}...")
            df = eurostat.get_data_df(info['code'])
            
            # Process the dataset
            df_processed = process_dataset(df, metric, info)
            if df_processed is None:
                continue
                
            # Store in MongoDB
            collection = mongo_db[metric]
            successful, errors = store_in_mongodb(collection, df_processed, metric, info)
            
            total_records += successful
            total_errors += errors
            
            logger.info(f"Completed processing {metric}")
            
        except Exception as e:
            logger.error(f"Error processing {metric}: {str(e)}")
            continue
    
    logger.info(f"\nImport completed: {total_records} total records imported, {total_errors} total errors")

if __name__ == "__main__":
    import_education_data()
