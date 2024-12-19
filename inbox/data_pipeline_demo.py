# %% [markdown]
# # Education Data Collection and Analysis Pipeline Demo
# 
# This notebook demonstrates how to:
# 1. Collect education-related data from different sources
# 2. Save data to databases
# 3. Perform basic data analysis

# %%
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path('..').resolve()
sys.path.append(str(project_root))

from src.data_collection.eurostat_collector import EurostatCollector
from src.data_processing.db_manager import DatabaseManager

# Initialize objects
collector = EurostatCollector()
db_manager = DatabaseManager()

# %% [markdown]
# ## 1. Data Collection

# %%
# Get education investment data
education_data = collector.get_education_investment_data()
print(f"Education data shape: {education_data.shape}")
display(education_data.head())

# %%
# Get economic indicators data
try:
    economic_data = collector.get_economic_indicators()
    print(f"Economic data shape: {economic_data.shape}")
    display(economic_data.head())
except Exception as e:
    print(f"Error getting economic data: {str(e)}")

# %%
# Get education policy documents
policy_docs = collector.get_education_policies()
print(f"Collected {len(policy_docs)} policy documents\n")
print("Example document:")
print(policy_docs[0] if policy_docs else "No documents found")

# %% [markdown]
# ## 2. Data Storage
# 
# - Structured data (education investment and economic indicators) -> PostgreSQL
# - Unstructured data (policy documents) -> MongoDB

# %%
# Connect to databases
db_manager.connect_postgres()
db_manager.connect_mongo()

# Reset table structure
db_manager.drop_tables()
db_manager.setup_postgres_tables()

# Save structured data to PostgreSQL
db_manager.save_to_postgres(education_data, 'education_data')
db_manager.save_to_postgres(economic_data, 'economic_data')

# Save unstructured data to MongoDB
db_manager.save_to_mongo('education_policies', policy_docs)

print("Data storage completed!")

# %% [markdown]
# ## 3. Data Analysis Examples
# 
# Let's query some data from the database and perform simple analysis

# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to databases for analysis
db_manager.connect_postgres()
db_manager.connect_mongo()

# Query data from PostgreSQL
with db_manager.pg_conn.cursor() as cur:
    # Query education investment data for 2020
    cur.execute("""
        SELECT geo_time_period as country, value as investment
        FROM education_data
        WHERE year = 2020 AND isced11 = 'ED0'
        ORDER BY value DESC
        LIMIT 10
    """)
    education_2020 = pd.DataFrame(cur.fetchall(), columns=['country', 'investment'])

# Plot top 10 countries by education investment in 2020
plt.figure(figsize=(12, 6))
sns.barplot(data=education_2020, x='country', y='investment')
plt.title('Top 10 Countries by Education Investment (2020)')
plt.xlabel('Country')
plt.ylabel('Investment (EUR)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %%
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

# Plot scatter plot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=correlation_data, x='investment', y='gdp_growth')
plt.title('Education Investment vs GDP Growth')
plt.xlabel('Education Investment (EUR)')
plt.ylabel('GDP Growth Rate (%)')
plt.tight_layout()
plt.show()

# Calculate correlation coefficient
correlation = correlation_data['investment'].corr(correlation_data['gdp_growth'])
print(f"\nCorrelation coefficient between education investment and GDP growth: {correlation:.3f}")

# %%
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
if db_manager.pg_conn:
    db_manager.pg_conn.close()
if db_manager.mongo_client:
    db_manager.mongo_client.close()

print("Database connections closed")
