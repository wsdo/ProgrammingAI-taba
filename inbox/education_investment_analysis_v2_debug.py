# Import required libraries
import sys
import os
import json
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
from src.data_collection.eurostat_collector import EurostatCollector

# Set plotting style
plt.style.use('seaborn-v0_8')  # Use the v0.8 compatible style
sns.set_theme()  # Use seaborn's default theme
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

print("Step 1: Data Collection")
print("-" * 50)

# Initialize collectors and managers
collector = EurostatCollector()
db_manager = DatabaseManager()
cleaner = DataCleaner()

# Collect fresh data
print("\nCollecting education investment data...")
education_data_raw = collector.get_education_investment_data()
print(f"Collected {len(education_data_raw)} education investment records")

print("\nCollecting economic indicators...")
economic_data_raw = collector.get_economic_indicators()
print(f"Collected {len(economic_data_raw)} economic indicator records")

print("\nCollecting education policies...")
policy_docs = collector.get_education_policies()
print(f"Collected {len(policy_docs)} policy documents")

print("\nStep 2: Data Storage")
print("-" * 50)

# Store data in databases
print("\nConnecting to databases...")
try:
    db_manager.connect_postgres()
    print("Successfully connected to PostgreSQL")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {str(e)}")
    print("Cannot proceed without PostgreSQL connection")
    sys.exit(1)

try:
    db_manager.connect_mongo()
    print("Successfully connected to MongoDB")
except Exception as e:
    print(f"Warning: MongoDB connection failed: {str(e)}")
    print("Continuing without MongoDB...")

print("\nSetting up database tables...")
try:
    db_manager.create_tables()
    print("Successfully set up PostgreSQL tables")
except Exception as e:
    print(f"Error setting up tables: {str(e)}")
    sys.exit(1)

print("\nSaving data to PostgreSQL...")
try:
    # Insert data using the specific insert methods
    db_manager.insert_education_data(education_data_raw)
    db_manager.insert_economic_data(economic_data_raw)
    print("Successfully saved data to PostgreSQL")
except Exception as e:
    print(f"Error saving to PostgreSQL: {str(e)}")
    sys.exit(1)

print("\nSaving policy documents to MongoDB...")
if db_manager.mongo_db is not None:
    try:
        db_manager.save_to_mongo('education_policies', policy_docs)
        print("Successfully saved policy documents to MongoDB")
    except Exception as e:
        print(f"Warning: Failed to save to MongoDB: {str(e)}")
        print("Continuing without policy data...")
else:
    print("Skipping MongoDB storage as connection is not available")

print("\nStep 3: Data Retrieval and Analysis")
print("-" * 50)

# Get education investment data from PostgreSQL
print("\nRetrieving data from databases...")
try:
    education_data = db_manager.get_education_data()
    print(f"Retrieved {len(education_data)} education investment records")

    economic_data = db_manager.get_economic_data()
    print(f"Retrieved {len(economic_data)} economic indicator records")
except Exception as e:
    print(f"Error retrieving data from PostgreSQL: {str(e)}")
    sys.exit(1)

# Try to get policy data from MongoDB
policy_docs = []
if db_manager.mongo_db is not None:
    try:
        policy_data = db_manager.query_mongo('education_policies')
        policy_docs = list(policy_data)
        print(f"Retrieved {len(policy_docs)} education policy documents")
    except Exception as e:
        print(f"Warning: Could not retrieve MongoDB data: {str(e)}")
        print("Continuing without policy data...")

print("\nStep 4: Data Analysis")
print("-" * 50)

# Clean and prepare data
education_data_cleaned = cleaner.clean_education_data(education_data)
print(f"\nCleaned education data shape: {education_data_cleaned.shape}")

print("\nAnalyzing major EU countries...")
major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']
major_country_data = education_data_cleaned[
    education_data_cleaned['geo_time_period'].isin(major_countries)
]

# Debug: Print data availability for each country
print("\nData availability for each country:")
for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    print(f"{country}: {len(country_data)} records")

country_names = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'PL': 'Poland'
}

# Visualization 1: Investment Trends
plt.figure(figsize=(15, 8))
colors = {'DE': 'blue', 'FR': 'red', 'IT': 'green', 'ES': 'orange', 'PL': 'purple'}

# Debug: Track which countries are plotted
plotted_countries = []

for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    if not country_data.empty:
        country_data = country_data.sort_values('year')
        line = plt.plot(country_data['year'], 
                       country_data['value'], 
                       label=country_names[country],
                       color=colors[country],
                       marker='o')
        plotted_countries.append(country)
        print(f"Plotted data for {country_names[country]}")

plt.title('Education Investment Trends in Major EU Countries')
plt.xlabel('Year')
plt.ylabel('Investment Value')

# Only add legend if we have plotted countries
if plotted_countries:
    plt.legend()
else:
    print("Warning: No data was plotted!")

plt.grid(True, linestyle='--', alpha=0.7)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.tight_layout()

# Debug: Print the axis limits
print(f"\nPlot ranges - X: {plt.gca().get_xlim()}, Y: {plt.gca().get_ylim()}")

plt.show()

# Calculate and display CAGR
print("\nCompound Annual Growth Rate (CAGR) by Country:")
print("-" * 40)

for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    if len(country_data) >= 2:
        # Sort by year and get first and last values
        country_data = country_data.sort_values('year')
        first_year = country_data.iloc[0]
        last_year = country_data.iloc[-1]
        
        # Calculate CAGR
        years = last_year['year'] - first_year['year']
        if years > 0:
            cagr = (((last_year['value'] / first_year['value']) ** (1/years)) - 1) * 100
            print(f"{country_names[country]}: {cagr:.2f}%")
        else:
            print(f"Warning: Not enough years of data for {country_names[country]}")
    else:
        print(f"Warning: Insufficient data for {country_names[country]}")

print("\nStep 5: Cleanup")
print("-" * 50)

# Close database connections
print("\nClosing database connections...")
db_manager.close_connections()

print("\nAnalysis completed successfully!")
