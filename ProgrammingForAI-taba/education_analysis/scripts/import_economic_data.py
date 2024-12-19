import sys
from pathlib import Path
import logging
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_processing.db_manager import DatabaseManager
from scripts.download_imf_data import download_imf_data

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def import_economic_data():
    """Download IMF data and import it into the database"""
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Create tables if they don't exist
        db_manager.create_tables()
        
        # Download IMF data
        logging.info("Downloading IMF data...")
        economic_data = download_imf_data()
        
        if economic_data is not None and not economic_data.empty:
            # Import data into database
            logging.info("Importing data into database...")
            db_manager.insert_economic_data(economic_data)
            logging.info("Data import completed successfully")
        else:
            logging.error("No data to import")
            
    except Exception as e:
        logging.error(f"Error importing data: {str(e)}")
    finally:
        db_manager.close_connections()

if __name__ == "__main__":
    import_economic_data()
