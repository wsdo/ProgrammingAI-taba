# %% [markdown]
# # Comprehensive Education Investment Analysis
# 
# This notebook provides a complete pipeline for:
# 1. Data Collection from Multiple Sources
# 2. Data Processing and Storage
# 3. Comprehensive Analysis and Visualization
# 4. Trend Analysis and Statistical Insights

# %% [code]
# Import required libraries
import sys
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import matplotlib

# Add project root to Python path
project_root = Path('..').resolve()
sys.path.append(str(project_root))

# Import project modules
from src.data_collection.eurostat_collector import EurostatCollector
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner

# Set plotting style
plt.style.use('seaborn')
sns.set_palette('husl')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# %% [markdown]
# ## 1. Data Collection and Storage

# %% [code]
# Initialize objects
collector = EurostatCollector()
db_manager = DatabaseManager()
cleaner = DataCleaner()

# Collect education investment data
education_data = collector.get_education_investment_data()
print(f"Education data shape: {education_data.shape}")
display(education_data.head())

# Collect economic indicators
try:
    economic_data = collector.get_economic_indicators()
    print(f"Economic data shape: {economic_data.shape}")
    display(economic_data.head())
except Exception as e:
    print(f"Error getting economic data: {str(e)}")

# Collect education policies
policy_docs = collector.get_education_policies()
print(f"Collected {len(policy_docs)} policy documents\n")
print("Example document:")
print(policy_docs[0] if policy_docs else "No documents found")

# %% [markdown]
# ## 2. Data Storage and Processing

# %% [code]
# Connect to databases
db_manager.connect_postgres()
db_manager.connect_mongo()

# Reset and setup database structure
db_manager.drop_tables()
db_manager.setup_postgres_tables()

# Save data to databases
db_manager.save_to_postgres(education_data, 'education_data')
db_manager.save_to_postgres(economic_data, 'economic_data')
db_manager.save_to_mongo('education_policies', policy_docs)

print("Data storage completed!")

# Clean the education data
education_data_cleaned = cleaner.clean_education_data(education_data)
print("\nData cleaning results:")
print("Raw data shape:", education_data.shape)
print("Cleaned data shape:", education_data_cleaned.shape)

# %% [markdown]
# ## 3. Investment Trend Analysis

# %% [code]
# Display available country codes
print("\nAvailable country codes:")
print(sorted(education_data_cleaned['geo_time_period'].unique()))

# Select major countries for analysis
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

# Create visualization
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
    plt.ylabel('Investment Value')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.tight_layout()
    plt.show()

# %% [markdown]
# ## 4. Growth Rate Analysis

# %% [code]
# Calculate Compound Annual Growth Rate (CAGR)
print("\nCompound Annual Growth Rate (CAGR) by Country:")
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

# %% [code]
# Statistical summary by country
print("\nStatistical Summary by Country:")
print("-" * 40)
summary_stats = major_country_data.groupby('geo_time_period').agg({
    'value': ['count', 'mean', 'std', 'min', 'max']
}).round(2)
print(summary_stats)

# %% [markdown]
# ## 6. Economic Impact Analysis

# %% [code]
# Analyze relationship between education investment and GDP growth
with db_manager.pg_conn.cursor() as cur:
    cur.execute("""
        WITH edu_data AS (
            SELECT geo_time_period as country, year, value as investment
            FROM education_data
            WHERE isced11 = 'ED0'
        )
        SELECT 
            e.country,
            e.year,
            e.investment,
            c.gdp_growth
        FROM edu_data e
        JOIN economic_data c ON e.country = c.country_code 
            AND e.year = c.year
        WHERE e.investment IS NOT NULL 
            AND c.gdp_growth IS NOT NULL
    """)
    correlation_data = pd.DataFrame(
        cur.fetchall(), 
        columns=['country', 'year', 'investment', 'gdp_growth']
    )

# Create scatter plot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=correlation_data, x='investment', y='gdp_growth')
plt.title('Education Investment vs GDP Growth')
plt.xlabel('Education Investment (EUR)')
plt.ylabel('GDP Growth Rate (%)')
plt.tight_layout()
plt.show()

# Calculate and display correlation coefficient
correlation = correlation_data['investment'].corr(correlation_data['gdp_growth'])
print(f"\nCorrelation coefficient between education investment and GDP growth: {correlation:.3f}")

# %% [markdown]
# ## 7. Data Verification

# %% [code]
# Verify data in databases
education_count = db_manager.query_postgres("""
    SELECT COUNT(*) as count 
    FROM education_data
""")
print(f"Number of records in education_data: {education_count['count'].iloc[0]}")

economic_count = db_manager.query_postgres("""
    SELECT COUNT(*) as count 
    FROM economic_data
""")
print(f"Number of records in economic_data: {economic_count['count'].iloc[0]}")

policy_count = db_manager.mongo_client['education']['education_policies'].count_documents({})
print(f"Number of documents in education_policies: {policy_count}")

# Clean up connections
db_manager.close_connections()
print("\nDatabase connections closed")
