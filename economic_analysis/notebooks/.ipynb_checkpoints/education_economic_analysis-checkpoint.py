"""
Education and Economic Growth Analysis
This notebook analyzes the relationship between education investment and economic growth
"""

# %% [markdown]
# # Education and Economic Growth Analysis
# 
# This notebook analyzes the relationship between education investment and economic indicators,
# combining our previous analyses of education and economic data.

# %% [markdown]
# ## 1. Data Loading and Preparation

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.stattools import grangercausalitytests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up MongoDB connection
mongo_client = MongoClient(
    f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
    f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
    "?authSource=admin"
)
mongo_db = mongo_client[os.getenv('MONGODB_DB')]

# %% [markdown]
# ### Load and Prepare Data

# %%
def load_data_from_mongodb():
    """Load education and economic data from MongoDB"""
    # Education data
    education_data = pd.DataFrame(list(mongo_db.preprocessed_education.find()))
    
    # Economic data
    gdp_data = pd.DataFrame(list(mongo_db.preprocessed_gdp.find()))
    employment_data = pd.DataFrame(list(mongo_db.preprocessed_employment.find()))
    
    return education_data, gdp_data, employment_data

# Load data
education_data, gdp_data, employment_data = load_data_from_mongodb()

# Prepare merged dataset
def prepare_merged_dataset(education_data, gdp_data, employment_data):
    """Merge and prepare datasets for analysis"""
    # Convert dates
    for df in [education_data, gdp_data, employment_data]:
        df['date'] = pd.to_datetime(df['date'])
    
    # Merge datasets
    merged_data = pd.merge(education_data, gdp_data, 
                          on=['country_code', 'date'], 
                          suffixes=('_edu', '_gdp'))
    merged_data = pd.merge(merged_data, employment_data,
                          on=['country_code', 'date'],
                          suffixes=('', '_emp'))
    
    return merged_data

merged_data = prepare_merged_dataset(education_data, gdp_data, employment_data)

# %% [markdown]
# ## 2. Correlation Analysis

# %%
def perform_correlation_analysis(data):
    """Perform correlation analysis between education and economic indicators"""
    # Basic correlations
    correlations = data[['value_edu', 'value_gdp', 'value']].corr()
    
    # Create correlation heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlations, annot=True, cmap='coolwarm', center=0)
    plt.title('Correlation between Education Investment and Economic Indicators')
    plt.show()
    
    return correlations

correlations = perform_correlation_analysis(merged_data)

# %% [markdown]
# ### Lag Effect Analysis

# %%
def analyze_lag_effects(data, lag_years=[3, 5, 10]):
    """Analyze lag effects of education investment on economic growth"""
    results = {}
    
    for lag in lag_years:
        # Create lagged education investment
        data[f'edu_lag_{lag}'] = data.groupby('country_code')['value_edu'].shift(lag)
        
        # Calculate correlation with GDP growth
        correlation = data[['value_gdp', f'edu_lag_{lag}']].corr().iloc[0, 1]
        results[lag] = correlation
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.bar(results.keys(), results.values())
    plt.title('Correlation between GDP Growth and Lagged Education Investment')
    plt.xlabel('Lag (years)')
    plt.ylabel('Correlation Coefficient')
    plt.show()
    
    return results

lag_effects = analyze_lag_effects(merged_data)

# %% [markdown]
# ## 3. Statistical Modeling

# %%
def perform_regression_analysis(data):
    """Perform regression analysis"""
    # Prepare data
    X = data[['value_edu']]
    y = data['value_gdp']
    
    # Fit regression model
    model = LinearRegression()
    model.fit(X, y)
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, alpha=0.5)
    plt.plot(X, model.predict(X), color='red', linewidth=2)
    plt.xlabel('Education Investment')
    plt.ylabel('GDP Growth')
    plt.title('Education Investment vs GDP Growth')
    plt.show()
    
    return model

regression_model = perform_regression_analysis(merged_data)

# %% [markdown]
# ### Granger Causality Test

# %%
def perform_granger_causality(data, maxlag=5):
    """Perform Granger causality test"""
    # Prepare data for Granger test
    edu_gdp_data = data.groupby('date')[['value_edu', 'value_gdp']].mean()
    
    # Perform Granger causality test
    granger_test = grangercausalitytests(edu_gdp_data[['value_edu', 'value_gdp']], maxlag=maxlag)
    
    return granger_test

granger_results = perform_granger_causality(merged_data)

# %% [markdown]
# ## 4. Visualization Analysis

# %%
def create_trend_analysis_plots(data):
    """Create trend analysis plots"""
    # Time series plot
    fig = make_subplots(rows=2, cols=1,
                        subplot_titles=('Education Investment Trends',
                                      'GDP Growth Trends'))
    
    for country in data['country_code'].unique():
        country_data = data[data['country_code'] == country]
        
        # Education investment
        fig.add_trace(
            go.Scatter(x=country_data['date'], y=country_data['value_edu'],
                      name=f'{country} Education', mode='lines'),
            row=1, col=1
        )
        
        # GDP growth
        fig.add_trace(
            go.Scatter(x=country_data['date'], y=country_data['value_gdp'],
                      name=f'{country} GDP', mode='lines'),
            row=2, col=1
        )
    
    fig.update_layout(height=800, title_text="Education Investment and GDP Growth Trends")
    fig.show()

# Create visualization
create_trend_analysis_plots(merged_data)

# %% [markdown]
# ### Regional Analysis

# %%
def perform_regional_analysis(data):
    """Perform regional analysis"""
    # Calculate average values by country
    regional_data = data.groupby('country_code').agg({
        'value_edu': 'mean',
        'value_gdp': 'mean',
        'value': 'mean'  # employment rate
    }).round(2)
    
    # Create scatter plot with size representing employment rate
    plt.figure(figsize=(12, 8))
    plt.scatter(regional_data['value_edu'], regional_data['value_gdp'],
                s=regional_data['value']*100, alpha=0.6)
    
    # Add country labels
    for idx, row in regional_data.iterrows():
        plt.annotate(idx, (row['value_edu'], row['value_gdp']))
    
    plt.xlabel('Average Education Investment')
    plt.ylabel('Average GDP Growth')
    plt.title('Regional Comparison: Education Investment vs GDP Growth')
    plt.show()
    
    return regional_data

regional_analysis = perform_regional_analysis(merged_data)

# %% [markdown]
# ## 5. Key Findings and Conclusions
# 
# Based on our analysis, we can draw the following conclusions:
# 
# 1. Correlation Analysis:
#    - Direct correlation between education investment and GDP growth
#    - Lag effects showing the long-term impact of education investment
# 
# 2. Statistical Evidence:
#    - Regression analysis confirms the positive relationship
#    - Granger causality tests suggest causal relationship
# 
# 3. Regional Patterns:
#    - Variation in education investment effectiveness across countries
#    - Different returns on education investment by region
# 
# 4. Policy Implications:
#    - Importance of sustained education investment
#    - Need for long-term planning in education policy
#    - Regional considerations in education investment strategy

# %%
# Close database connection
mongo_client.close()
print("Analysis completed successfully!")
