"""
Economic Indicators Analysis with MongoDB Integration
This notebook demonstrates the analysis of economic indicators using both PostgreSQL and MongoDB
"""

# %% [markdown]
# # Economic Indicators Analysis with MongoDB Integration
# 
# This notebook demonstrates the analysis of economic indicators for major EU economies, using both PostgreSQL and MongoDB.
# We'll analyze GDP, employment, and inflation data to understand their relationships and trends.
# 
# ## Table of Contents
# 1. Setup and Data Loading
# 2. Data Preprocessing and MongoDB Storage
# 3. Statistical Analysis and MongoDB Storage
# 4. Data Retrieval from MongoDB
# 5. Visualization and Analysis
# 6. Results and Conclusions

# %% [markdown]
# ## 1. Setup and Data Loading
# First, let's import the necessary libraries and set up our database connections.

# %%
# Import required libraries
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    # Set up PostgreSQL connection
    pg_engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

    # Set up MongoDB connection
    mongo_client = MongoClient(
        f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
        f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
        "?authSource=admin"
    )
    mongo_db = mongo_client[os.getenv('MONGODB_DB')]

    # Set plotting style
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = [12, 8]

    # %% [markdown]
    # ## 2. Data Loading and Preprocessing
    # Let's load our economic data from PostgreSQL and preprocess it.

    # %%
    # Load data from PostgreSQL
    print("Loading data from PostgreSQL...")
    gdp_data = pd.read_sql('SELECT * FROM economic_data.gdp', pg_engine)
    emp_data = pd.read_sql('SELECT * FROM economic_data.employment', pg_engine)
    inf_data = pd.read_sql('SELECT * FROM economic_data.inflation', pg_engine)

    # Display basic information
    print("\nData Info:")
    for name, df in [("GDP", gdp_data), ("Employment", emp_data), ("Inflation", inf_data)]:
        print(f"\n{name} Data:")
        print(df.info())

    # %% [markdown]
    # Now let's preprocess the data and store it in MongoDB.

    # %%
    def preprocess_data(df):
        """Clean and preprocess the data"""
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['country_code', 'date'])
        return df.reset_index(drop=True)

    def save_to_mongodb(collection, data, identifier=None):
        """Save data to MongoDB collection"""
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')
        
        if identifier:
            collection.update_one(identifier, {'$set': data}, upsert=True)
        else:
            for item in data:
                item['timestamp'] = datetime.utcnow()
            collection.insert_many(data)

    # Preprocess data
    print("Preprocessing data...")
    gdp_data = preprocess_data(gdp_data)
    emp_data = preprocess_data(emp_data)
    inf_data = preprocess_data(inf_data)

    # Save preprocessed data to MongoDB
    print("\nSaving preprocessed data to MongoDB...")
    save_to_mongodb(mongo_db.preprocessed_gdp, gdp_data)
    save_to_mongodb(mongo_db.preprocessed_employment, emp_data)
    save_to_mongodb(mongo_db.preprocessed_inflation, inf_data)

    # %% [markdown]
    # ## 3. Statistical Analysis and MongoDB Storage
    # Let's calculate various statistics and store them in MongoDB.

    # %%
    # Calculate and store summary statistics
    def calculate_summary_statistics(df, indicator_name):
        """Calculate summary statistics and store in MongoDB"""
        summary = df.groupby('country_code')['value'].agg([
            'count', 'mean', 'std', 'min', 'max'
        ]).round(2)
        
        summary_dict = {
            'indicator': indicator_name,
            'statistics': summary.to_dict(orient='index'),
            'calculation_date': datetime.utcnow()
        }
        save_to_mongodb(mongo_db.summary_statistics, summary_dict,
                    identifier={'indicator': indicator_name})
        
        print(f"\n{indicator_name} Summary Statistics:")
        print(summary)
        return summary

    # Calculate statistics for each indicator
    gdp_stats = calculate_summary_statistics(gdp_data, "GDP")
    emp_stats = calculate_summary_statistics(emp_data, "Employment")
    inf_stats = calculate_summary_statistics(inf_data, "Inflation")

    # %% [markdown]
    # Now let's calculate correlations between indicators for each country.

    # %%
    def calculate_correlations(gdp_data, emp_data, inf_data, country_code):
        """Calculate correlations and store in MongoDB"""
        # Filter data for the country
        country_gdp = gdp_data[gdp_data['country_code'] == country_code].set_index('date')['value']
        country_emp = emp_data[emp_data['country_code'] == country_code].set_index('date')['value']
        country_inf = inf_data[inf_data['country_code'] == country_code].set_index('date')['value']
        
        # Calculate correlations
        combined_data = pd.DataFrame({
            'GDP': country_gdp,
            'Employment': country_emp,
            'Inflation': country_inf
        })
        correlations = combined_data.corr()
        
        # Save to MongoDB
        correlation_dict = {
            'country_code': country_code,
            'correlations': correlations.to_dict(),
            'calculation_date': datetime.utcnow()
        }
        save_to_mongodb(mongo_db.correlations, correlation_dict,
                    identifier={'country_code': country_code})
        
        print(f"\nCorrelations for {country_code}:")
        print(correlations.round(3))
        return correlations

    # Calculate correlations for each country
    countries = gdp_data['country_code'].unique()
    correlation_results = {
        country: calculate_correlations(gdp_data, emp_data, inf_data, country)
        for country in countries
    }

    # %% [markdown]
    # ## 4. Data Retrieval from MongoDB
    # Let's demonstrate how to retrieve and use the data stored in MongoDB.

    # %%
    def get_mongodb_data():
        """Retrieve all analysis results from MongoDB"""
        # Get preprocessed data
        gdp_df = pd.DataFrame(list(mongo_db.preprocessed_gdp.find()))
        emp_df = pd.DataFrame(list(mongo_db.preprocessed_employment.find()))
        inf_df = pd.DataFrame(list(mongo_db.preprocessed_inflation.find()))
        
        # Get summary statistics
        stats = list(mongo_db.summary_statistics.find())
        
        # Get correlations
        correlations = list(mongo_db.correlations.find())
        
        return {
            'preprocessed': {'gdp': gdp_df, 'employment': emp_df, 'inflation': inf_df},
            'statistics': stats,
            'correlations': correlations
        }

    # Retrieve data from MongoDB
    print("Retrieving data from MongoDB...")
    mongodb_data = get_mongodb_data()

    # Display some retrieved data
    print("\nSample of retrieved data:")
    print("\nPreprocessed GDP data:")
    print(mongodb_data['preprocessed']['gdp'].head())
    print("\nStatistics sample:")
    print(pd.DataFrame(mongodb_data['statistics'][0]['statistics']).T)

    # %% [markdown]
    # ## 5. Visualization and Analysis
    # Let's create some visualizations using the data from MongoDB.

    # %%
    def plot_correlation_heatmaps(correlations):
        """Create correlation heatmaps for each country"""
        n_countries = len(correlations)
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle('Correlation Heatmaps by Country')
        
        for i, corr in enumerate(correlations):
            row = i // 3
            col = i % 3
            country = corr['country_code']
            corr_matrix = pd.DataFrame(corr['correlations'])
            
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0,
                    ax=axes[row, col])
            axes[row, col].set_title(f'{country}')
        
        plt.tight_layout()
        return fig

    # Create correlation heatmaps
    correlation_fig = plot_correlation_heatmaps(mongodb_data['correlations'])
    plt.show()

    # %% [markdown]
    # Let's also create time series plots using the preprocessed data.

    # %%
    def plot_time_series(gdp_df, emp_df, inf_df):
        """Create time series plots for all indicators"""
        # Convert ObjectId to string for JSON serialization
        for df in [gdp_df, emp_df, inf_df]:
            if '_id' in df.columns:
                df['_id'] = df['_id'].astype(str)
        
        fig = make_subplots(rows=3, cols=1,
                          subplot_titles=('GDP Trends', 'Employment Trends', 'Inflation Trends'))
        
        for country in gdp_df['country_code'].unique():
            # GDP
            country_gdp = gdp_df[gdp_df['country_code'] == country]
            fig.add_trace(
                go.Scatter(x=country_gdp['date'], y=country_gdp['value'],
                        name=f'{country} GDP', mode='lines'),
                row=1, col=1
            )
            
            # Employment
            country_emp = emp_df[emp_df['country_code'] == country]
            fig.add_trace(
                go.Scatter(x=country_emp['date'], y=country_emp['value'],
                        name=f'{country} Employment', mode='lines'),
                row=2, col=1
            )
            
            # Inflation
            country_inf = inf_df[inf_df['country_code'] == country]
            fig.add_trace(
                go.Scatter(x=country_inf['date'], y=country_inf['value'],
                        name=f'{country} Inflation', mode='lines'),
                row=3, col=1
            )
        
        fig.update_layout(height=900, title_text="Economic Indicators Trends")
        return fig

    # Create time series plots
    ts_fig = plot_time_series(
        mongodb_data['preprocessed']['gdp'],
        mongodb_data['preprocessed']['employment'],
        mongodb_data['preprocessed']['inflation']
    )
    ts_fig.show()

    # %% [markdown]
    # ## 6. Results and Conclusions
    # 
    # Our analysis using MongoDB has revealed several interesting patterns:
    # 
    # 1. Data Storage and Retrieval:
    #    - Successfully stored preprocessed data in MongoDB collections
    #    - Implemented efficient retrieval mechanisms for analysis
    # 
    # 2. Statistical Insights:
    #    - Calculated and stored comprehensive summary statistics
    #    - Generated correlation matrices for each country
    # 
    # 3. Visualization Capabilities:
    #    - Created interactive time series plots
    #    - Generated correlation heatmaps
    # 
    # The MongoDB integration provides:
    # - Efficient storage of preprocessed data
    # - Quick access to analysis results
    # - Flexible data structure for various types of analysis
    # 
    # This combined PostgreSQL-MongoDB approach allows us to:
    # - Keep raw data in PostgreSQL for ACID compliance
    # - Store analysis results and preprocessed data in MongoDB for flexibility
    # - Easily retrieve and visualize results

    # %%
    # Close database connections
    mongo_client.close()
    pg_engine.dispose()
    print("Analysis completed successfully!")

except Exception as e:
    print(f"Error: {str(e)}")
    if 'mongo_client' in locals():
        mongo_client.close()
    if 'pg_engine' in locals():
        pg_engine.dispose()
