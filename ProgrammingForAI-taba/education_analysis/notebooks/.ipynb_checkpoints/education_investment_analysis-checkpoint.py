# Import required libraries
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import matplotlib

# Load environment variables from .env file
load_dotenv(Path('..').resolve() / '.env')

# Add project root to Python path
project_root = Path('..').resolve()
sys.path.append(str(project_root))

# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner

# Set plotting style
plt.style.use('seaborn-v0_8')  # Use the v0.8 compatible style
sns.set_theme()  # Use seaborn's default theme
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

# Initialize database connections and utilities
db_manager = DatabaseManager()
cleaner = DataCleaner()

# Get education investment data from PostgreSQL
education_data = db_manager.query_postgres("""
    SELECT *
    FROM education_data
    ORDER BY year, geo_time_period
""")

# Get economic indicators from PostgreSQL
economic_data = db_manager.query_postgres("""
    SELECT *
    FROM economic_data
    ORDER BY year, country_code
""")

# Try to get policy data from MongoDB
try:
    if db_manager.mongo_db:
        policy_data = db_manager.mongo_db['education_policies'].find()
        policy_docs = list(policy_data)
        print(f"Retrieved {len(policy_docs)} education policy documents")
    else:
        print("Warning: MongoDB connection not available, skipping policy data")
        policy_docs = []
except Exception as e:
    print(f"Error retrieving MongoDB data: {str(e)}")
    policy_docs = []

print(f"Retrieved {len(education_data)} education investment records")
print(f"Retrieved {len(economic_data)} economic indicator records")

# Display sample of education data
print("\nSample of education investment data:")
print(education_data.head())
