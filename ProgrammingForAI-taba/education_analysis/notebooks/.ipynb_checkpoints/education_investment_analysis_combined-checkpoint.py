# %% [markdown]
# # Comprehensive Education Investment Analysis
# 
# This notebook provides a comprehensive analysis of education investment data across EU countries, including:
# 1. Data Collection and Processing
# 2. Basic Statistics and Overview
# 3. Time Series Analysis
# 4. Country Comparisons
# 5. Investment Trend Analysis
# 6. Economic Impact Assessment
# 7. Policy Analysis

# %%
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
plt.rcParams['axes.unicode_minus'] = False

# %% [markdown]
# ## 1. Data Collection and Processing

# %%
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

# Get education policies from MongoDB
policy_data = db_manager.mongo_client['education']['education_policies'].find()
policy_docs = list(policy_data)

print(f"Retrieved {len(education_data)} education investment records")
print(f"Retrieved {len(economic_data)} economic indicator records")
print(f"Retrieved {len(policy_docs)} education policy documents")

# Display sample of education data
print("\nSample of education investment data:")
display(education_data.head())

# %% [markdown]
# ## 2. Data Cleaning and Preparation

# %%
# Clean education investment data
education_data_cleaned = cleaner.clean_education_data(education_data)

print("Data cleaning results:")
print("Raw data shape:", education_data.shape)
print("Cleaned data shape:", education_data_cleaned.shape)

# Display available countries
print("\nAvailable country codes:")
print(sorted(education_data_cleaned['geo_time_period'].unique()))

# %% [markdown]
# ## 3. Investment Trend Analysis

# %%
# Select major EU countries for analysis
major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']
major_country_data = education_data_cleaned[
    education_data_cleaned['geo_time_period'].isin(major_countries)
]

# Country name mapping
country_names = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'PL': 'Poland'
}

# Create investment trends visualization
if not major_country_data.empty:
    plt.figure(figsize=(15, 8))
    colors = {'DE': 'blue', 'FR': 'red', 'IT': 'green', 'ES': 'orange', 'PL': 'purple'}
    
    for country in major_countries:
        country_data = major_country_data[major_country_data['geo_time_period'] == country]
        if not country_data.empty:
            country_data = country_data.sort_values('year')
            plt.plot(country_data['year'], 
                    country_data['value'], 
                    label=country_names[country],
                    color=colors[country],
                    marker='o')
    
    plt.title('Education Investment Trends in Major EU Countries')
    plt.xlabel('Year')
    plt.ylabel('Investment Value (PPS)')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.tight_layout()
    plt.show()

# %% [markdown]
# ## 4. Growth Rate Analysis

# %%
# Calculate Compound Annual Growth Rate (CAGR)
print("Compound Annual Growth Rate (CAGR) by Country:")
print("-" * 40)

for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    if len(country_data) >= 2:
        country_data = country_data.sort_values('year')
        start_value = country_data.iloc[0]['value']
        end_value = country_data.iloc[-1]['value']
        years = country_data.iloc[-1]['year'] - country_data.iloc[0]['year']
        
        if years > 0 and start_value > 0:
            cagr = (end_value/start_value)**(1/years) - 1
            start_year = country_data.iloc[0]['year']
            end_year = country_data.iloc[-1]['year']
            print(f"{country_names[country]} ({country}): {cagr*100:.2f}% ({start_year}-{end_year})")
    else:
        print(f"{country_names[country]} ({country}): Insufficient data points")

# %% [markdown]
# ## 5. Statistical Analysis

# %%
# Generate statistical summary by country
print("Statistical Summary by Country:")
print("-" * 40)
summary_stats = major_country_data.groupby('geo_time_period').agg({
    'value': ['count', 'mean', 'std', 'min', 'max']
}).round(2)
print(summary_stats)

# Calculate yearly averages
yearly_avg = education_data_cleaned.groupby('year')['value'].mean().reset_index()

# Plot yearly average investment trend
plt.figure(figsize=(12, 6))
plt.plot(yearly_avg['year'], yearly_avg['value'], marker='o')
plt.title('Average Education Investment Trend Across All Countries')
plt.xlabel('Year')
plt.ylabel('Average Investment Value (PPS)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 6. Economic Impact Analysis

# %%
# Merge education and economic data
merged_data = pd.merge(
    education_data_cleaned,
    economic_data,
    left_on=['geo_time_period', 'year'],
    right_on=['country_code', 'year'],
    how='inner'
)

# Calculate correlation between education investment and economic indicators
correlations = merged_data.groupby('geo_time_period').apply(
    lambda x: x['value'].corr(x['gdp_growth'])
).round(3)

print("Correlation between Education Investment and GDP Growth by Country:")
print("-" * 60)
for country in correlations.index:
    if country in country_names:
        print(f"{country_names[country]} ({country}): {correlations[country]}")

# Visualize relationship
plt.figure(figsize=(10, 6))
sns.scatterplot(data=merged_data, x='value', y='gdp_growth', hue='geo_time_period')
plt.title('Education Investment vs GDP Growth')
plt.xlabel('Education Investment (PPS)')
plt.ylabel('GDP Growth Rate (%)')
plt.legend(title='Country')
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 7. Policy Analysis

# %%
# Analyze policy documents from MongoDB
if policy_docs:
    print("Policy Analysis Summary:")
    print("-" * 40)
    for doc in policy_docs[:5]:  # Display first 5 policies
        print(f"Country: {doc.get('country', 'N/A')}")
        print(f"Year: {doc.get('year', 'N/A')}")
        print(f"Policy Type: {doc.get('policy_type', 'N/A')}")
        print(f"Description: {doc.get('description', 'N/A')[:200]}...")
        print("-" * 40)

# Clean up database connections
db_manager.close_connections()
print("\nDatabase connections closed")
