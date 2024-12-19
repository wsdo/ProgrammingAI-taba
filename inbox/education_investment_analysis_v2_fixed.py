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

# Set plotting style with better defaults for visualization
plt.style.use('seaborn-v0_8')
sns.set_theme()
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

# Data Storage and Processing
print("\nStep 2: Data Processing")
print("-" * 50)

# Clean and prepare data
education_data_cleaned = cleaner.clean_education_data(education_data_raw)
print(f"\nCleaned education data shape: {education_data_cleaned.shape}")

# Analysis of Major EU Countries
print("\nAnalyzing major EU countries...")
major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']
major_country_data = education_data_cleaned[
    education_data_cleaned['geo_time_period'].isin(major_countries)
]

# Debug: Print data availability
print("\nData availability for each country:")
for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    print(f"{country}: {len(country_data)} records")

# Country name mapping
country_names = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'PL': 'Poland'
}

# Create figure with larger size for better visibility
plt.figure(figsize=(15, 8))

# Define colors for each country
colors = {
    'DE': '#1f77b4',  # Blue
    'FR': '#ff7f0e',  # Orange
    'IT': '#2ca02c',  # Green
    'ES': '#d62728',  # Red
    'PL': '#9467bd'   # Purple
}

# Plot data for each country with explicit labels and styling
for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    if not country_data.empty:
        country_data = country_data.sort_values('year')
        plt.plot(country_data['year'], 
                country_data['value'],
                label=country_names[country],
                color=colors[country],
                marker='o',
                markersize=6,
                linewidth=2,
                linestyle='-')
        print(f"Plotted data for {country_names[country]}")

# Customize plot appearance
plt.title('Education Investment Trends in Major EU Countries', 
         fontsize=14, 
         pad=20)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Investment Value (Million EUR)', fontsize=12)

# Add legend with better positioning
plt.legend(loc='upper left', 
          bbox_to_anchor=(1, 1),
          frameon=True,
          fancybox=True,
          shadow=True)

# Customize grid
plt.grid(True, linestyle='--', alpha=0.7)

# Remove top and right spines
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Show plot
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

print("\nAnalysis completed successfully!")
