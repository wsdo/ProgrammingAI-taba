# # Education Investment Analysis
#
# This notebook analyzes education investment data across EU countries, including economic indicators and policy impacts.

# !pip install pandas numpy matplotlib seaborn plotly psycopg2-binary pymongo python-dotenv eurostat statsmodels

# !pip install pandas numpy matplotlib seaborn plotly psycopg2-binary pymongo python-dotenv eurostat statsmodels

# !pip install pandas numpy matplotlib seaborn plotly psycopg2-binary pymongo python-dotenv eurostat statsmodels

# %%
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

# %%
# Load environment variables from .env file
load_dotenv(Path('..').resolve() / '.env')

# %%
# Add project root to Python path
project_root = Path('..').resolve()
sys.path.append(str(project_root))

# %%
# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner
from src.data_collection.eurostat_collector import EurostatCollector

# %%
# Set plotting style
plt.style.use('seaborn-v0_8')  # Use the v0.8 compatible style
sns.set_theme()  # Use seaborn's default theme
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

# ## Step 1: Data Collection and Storage

# %%
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

# ## Step 2: Database Storage

# %%
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

# %%
print("\nSetting up database tables...")
try:
    db_manager.create_tables()
    print("Successfully set up PostgreSQL tables")
except Exception as e:
    print(f"Error setting up tables: {str(e)}")
    sys.exit(1)

# %%
print("\nSaving data to PostgreSQL...")
try:
    # Insert data using the specific insert methods
    db_manager.insert_education_data(education_data_raw)
    db_manager.insert_economic_data(economic_data_raw)
    print("Successfully saved data to PostgreSQL")
except Exception as e:
    print(f"Error saving to PostgreSQL: {str(e)}")
    sys.exit(1)

# %%
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

# ## Step 3: Data Retrieval and Analysis

# %%
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

# ## Step 4: Data Analysis

# %%
print("\nStep 4: Data Analysis")
print("-" * 50)

# Clean and prepare data
education_data_cleaned = cleaner.clean_education_data(education_data)
print(f"\nCleaned education data shape: {education_data_cleaned.shape}")

# ### Analysis 1: Major EU Countries Investment Trends

# %%
print("\nAnalyzing major EU countries...")
major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']
major_country_data = education_data_cleaned[
    education_data_cleaned['geo_time_period'].isin(major_countries)
]

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

# +
plotted_countries = []

for country in major_countries:
    country_data = major_country_data[major_country_data['geo_time_period'] == country]
    if not country_data.empty:
        country_data = country_data.sort_values('year')
        plt.plot(country_data['year'], 
                country_data['value'], 
                label=country_names[country],
                color=colors[country],
                marker='o')
        plotted_countries.append(country)
        print(f"Plotted data for {country_names[country]}")

# +
plt.plot(country_data['year'], 
         country_data['value'],
         label=country_names[country],  
         color=colors[country],
         marker='o',
         markersize=6,
         linewidth=2,
         linestyle='-')

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
# -

# %%
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

# ### Analysis 2: Economic Correlation Analysis

# %%
print("\nAnalyzing correlation with economic indicators...")
print("-" * 40)

# +
# unify the feature names for combining
country_code_map = {
    'AUT': 'AT', 'BEL': 'BE', 'BGR': 'BG', 'CYP': 'CY', 'CZE': 'CZ',
    'DEU': 'DE', 'DNK': 'DK', 'ESP': 'ES', 'EST': 'EE', 'FIN': 'FI',
    'FRA': 'FR', 'GRC': 'EL', 'HRV': 'HR', 'HUN': 'HU', 'IRL': 'IE',
    'ITA': 'IT', 'LTU': 'LT', 'LUX': 'LU', 'LVA': 'LV', 'MLT': 'MT',
    'NLD': 'NL', 'POL': 'PL', 'PRT': 'PT', 'ROU': 'RO', 'SVK': 'SK',
    'SVN': 'SI', 'SWE': 'SE'
}

economic_data['country_code'] = economic_data['country_code'].map(country_code_map)

# Merge education and economic data
merged_data = pd.merge(
    education_data_cleaned,
    economic_data,
    left_on=['geo_time_period', 'year'],
    right_on=['country_code', 'year'],
    how='inner'
)
# -

if not merged_data.empty:
    # Calculate correlations
    correlation_vars = ['value', 'gdp_growth', 'employment_rate', 'gdp_per_capita']
    correlations = merged_data[correlation_vars].corr()
    
    # Plot correlation heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlations, annot=True, cmap='coolwarm', center=0)
    plt.title('Correlation between Education Investment and Economic Indicators')
    plt.tight_layout()
    plt.show()
    
    # Print key findings
    edu_gdp_corr = correlations.loc['value', 'gdp_per_capita']
    edu_emp_corr = correlations.loc['value', 'employment_rate']
    print(f"\nKey Correlations:")
    print(f"Education Investment vs GDP per capita: {edu_gdp_corr:.2f}")
    print(f"Education Investment vs Employment Rate: {edu_emp_corr:.2f}")

# ### Analysis 3: Policy Impact Analysis

# +
# Analysis 3: Policy Impact Analysis
print("\nAnalyzing Policy Impact...")
print("-" * 40)

print("\nFetching education policy data...")
# Get policy data
policy_docs = collector.get_education_policies()
print(f"Found {len(policy_docs)} policy documents")

# Display sample of policy documents
if policy_docs:
    print("\nSample of policy documents:")
    for doc in policy_docs[:3]:
        try:
            print(f"\nPolicy document details:")
            for key, value in doc.items():
                print(f"{key}: {value}")
            print("-" * 30)
        except Exception as e:
            print(f"Error processing document: {e}")
            print(f"Document content: {doc}")

print("\nEducation data info:")
print(education_data_cleaned.info())

if policy_docs:
    # Extract policy years and analyze investment changes
    policy_years = []
    policy_countries = []
    
    for doc in policy_docs:
        try:
            if isinstance(doc, dict) and 'year' in doc and 'country' in doc:
                policy_years.append(doc['year'])
                policy_countries.append(doc['country'])
        except Exception as e:
            print(f"Error processing document: {e}")
            print(f"Document content: {doc}")
    
    print(f"\nFound {len(policy_years)} policies with valid year and country")
    
    if policy_years:
        # Create a DataFrame for analysis results
        results = []
        
        # Analyze investment changes around policy implementation
        for doc in policy_docs:
            try:
                if not isinstance(doc, dict):
                    print(f"Skipping invalid document: {doc}")
                    continue
                    
                country = doc.get('country')
                year = doc.get('year')
                
                if not country or not year:
                    print(f"Missing country or year in document: {doc}")
                    continue
                
                print(f"\nAnalyzing {country} policy from {year}")
                
                # Filter data for the specific country
                country_data = education_data_cleaned[
                    education_data_cleaned['geo_time_period'] == country
                ]
                
                if not country_data.empty:
                    # Get investment before and after policy
                    before_data = country_data[country_data['year'] < year]
                    after_data = country_data[country_data['year'] >= year]
                    
                    before_policy = before_data['value'].mean()
                    after_policy = after_data['value'].mean()
                    
                    if pd.notna(before_policy) and pd.notna(after_policy):
                        change_pct = ((after_policy - before_policy) / before_policy) * 100
                        
                        print(f"\nPolicy Analysis for {country}:")
                        print("-" * 30)
                        print(f"Policy: {doc.get('title', 'Unknown')}")
                        print(f"Description: {doc.get('description', 'No description')}")
                        print(f"Source: {doc.get('source', 'Unknown')}")
                        print(f"Implementation Year: {year}")
                        print(f"Average Investment Before Policy: {before_policy:.2f}%")
                        print(f"Average Investment After Policy: {after_policy:.2f}%")
                        print(f"Investment Change: {change_pct:.2f}%")
                        
                        # Store results for summary
                        results.append({
                            'country': country,
                            'year': year,
                            'title': doc.get('title', 'Unknown'),
                            'before_policy': before_policy,
                            'after_policy': after_policy,
                            'change_pct': change_pct
                        })
                        
                        # Additional analysis
                        if change_pct > 0:
                            print("Impact: Positive - Investment increased after policy implementation")
                        elif change_pct < 0:
                            print("Impact: Negative - Investment decreased after policy implementation")
                        else:
                            print("Impact: Neutral - No significant change in investment")
                        
                        # Show data points used
                        print(f"\nData points before policy: {len(before_data)}")
                        print(f"Data points after policy: {len(after_data)}")
                    else:
                        print(f"\nWarning: Insufficient data for {country}")
                else:
                    print(f"\nWarning: No education data found for country {country}")
                    print("Available countries:", education_data_cleaned['geo_time_period'].unique())
            except Exception as e:
                print(f"Error processing document: {e}")
                print(f"Document content: {doc}")
                continue
        
        # Print summary
        if results:
            print("\nSummary of Policy Impacts:")
            print("-" * 40)
            for result in results:
                print(f"\n{result['country']} - {result['title']} ({result['year']}):")
                print(f"Investment change: {result['change_pct']:.2f}%")
else:
    print("No policy documents available for analysis")
# -

# ### Analysis 4: Investment Efficiency Analysis

# %%
print("\nAnalyzing Investment Efficiency...")
print("-" * 40)

if not merged_data.empty:
    # Calculate investment efficiency (GDP per capita / Education Investment)
    merged_data['investment_efficiency'] = merged_data['gdp_per_capita'] / merged_data['value']
    
    # Get top 5 most efficient countries
    latest_year = merged_data['year'].max()
    latest_efficiency = merged_data[merged_data['year'] == latest_year]
    top_efficient = latest_efficiency.nlargest(5, 'investment_efficiency')
    
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top_efficient, x='geo_time_period', y='investment_efficiency')
    plt.title(f'Top 5 Countries by Investment Efficiency ({latest_year})')
    plt.xlabel('Country')
    plt.ylabel('Efficiency Ratio (GDP per capita / Investment)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
    print("\nTop 5 Countries by Investment Efficiency:")
    for _, row in top_efficient.iterrows():
        print(f"{row['geo_time_period']}: {row['investment_efficiency']:.2f}")

# ## Step 5: Cleanup

# %%
print("\nStep 5: Cleanup")
print("-" * 50)

# Close database connections
print("\nClosing database connections...")
db_manager.close_connections()

print("\nAnalysis completed successfully!")
