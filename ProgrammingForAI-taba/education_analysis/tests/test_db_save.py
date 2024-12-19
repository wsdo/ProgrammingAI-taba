import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_collection.eurostat_collector import EurostatCollector
from src.data_processing.db_manager import DatabaseManager

def main():
    # Initialize objects
    collector = EurostatCollector()
    db_manager = DatabaseManager()
    
    try:
        # Connect to databases
        db_manager.connect_postgres()
        db_manager.connect_mongo()
        
        # Get data
        print("Collecting education data...")
        education_data = collector.get_education_investment_data()
        print(f"Education data shape: {education_data.shape}")
        
        print("\nCollecting economic data...")
        economic_data = collector.get_economic_indicators()
        print(f"Economic data shape: {economic_data.shape}")
        
        print("\nCollecting policy documents...")
        policy_docs = collector.get_education_policies()
        print(f"Number of policy documents: {len(policy_docs)}")
        
        # Reset tables
        print("\nDropping existing tables...")
        db_manager.drop_tables()
        
        print("\nCreating new tables...")
        db_manager.setup_postgres_tables()
        
        # Save data
        print("\nSaving education data...")
        db_manager.save_to_postgres(education_data, 'education_data')
        
        print("\nSaving economic data...")
        db_manager.save_to_postgres(economic_data, 'economic_data')
        
        print("\nSaving policy documents...")
        db_manager.save_to_mongo('education_policies', policy_docs)
        
        print("\nAll data saved successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        if db_manager.pg_conn:
            db_manager.pg_conn.close()
        if db_manager.mongo_client:
            db_manager.mongo_client.close()

if __name__ == "__main__":
    main()
