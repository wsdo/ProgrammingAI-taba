"""
Economic Indicators Analysis Script
This script analyzes economic indicators for major EU economies using both PostgreSQL and MongoDB
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from datetime import datetime
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json

def setup_database_connections():
    """Set up connections to PostgreSQL and MongoDB databases"""
    load_dotenv()
    
    # PostgreSQL connection
    pg_engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    
    # MongoDB connection
    mongo_client = MongoClient(
        f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
        f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
        "?authSource=admin"
    )
    mongo_db = mongo_client[os.getenv('MONGODB_DB')]
    
    return pg_engine, mongo_db

def load_economic_data(engine):
    """Load economic data from PostgreSQL tables"""
    
    # Load GDP data
    gdp_data = pd.read_sql(
        'SELECT * FROM economic_data.gdp',
        engine
    )
    
    # Load employment data
    emp_data = pd.read_sql(
        'SELECT * FROM economic_data.employment',
        engine
    )
    
    # Load inflation data
    inf_data = pd.read_sql(
        'SELECT * FROM economic_data.inflation',
        engine
    )
    
    return gdp_data, emp_data, inf_data

def preprocess_data(df):
    """Clean and preprocess the data"""
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by country and date
    df = df.sort_values(['country_code', 'date'])
    
    # Reset index
    df = df.reset_index(drop=True)
    
    return df

def save_to_mongodb(mongo_db, collection_name, data, identifier=None):
    """Save data to MongoDB collection"""
    collection = mongo_db[collection_name]
    
    # Convert DataFrame to dict if necessary
    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient='records')
    
    # If identifier is provided, use upsert
    if identifier:
        collection.update_one(
            identifier,
            {'$set': data},
            upsert=True
        )
    else:
        # Add timestamp to the data
        if isinstance(data, list):
            for item in data:
                item['timestamp'] = datetime.utcnow()
            collection.insert_many(data)
        else:
            data['timestamp'] = datetime.utcnow()
            collection.insert_one(data)

def calculate_summary_statistics(df, indicator_name, mongo_db):
    """Calculate summary statistics for each country and save to MongoDB"""
    
    summary = df.groupby('country_code')['value'].agg([
        'count', 'mean', 'std', 'min', 'max'
    ]).round(2)
    
    # Save to MongoDB
    summary_dict = {
        'indicator': indicator_name,
        'statistics': summary.to_dict(orient='index'),
        'calculation_date': datetime.utcnow()
    }
    save_to_mongodb(mongo_db, 'summary_statistics', summary_dict,
                   identifier={'indicator': indicator_name})
    
    print(f"\n{indicator_name} Summary Statistics:")
    print(summary)
    
    return summary

def calculate_correlations(gdp_data, emp_data, inf_data, country_code, mongo_db):
    """Calculate correlations between indicators for a specific country and save to MongoDB"""
    
    # Filter data for the country
    country_gdp = gdp_data[gdp_data['country_code'] == country_code].set_index('date')['value']
    country_emp = emp_data[emp_data['country_code'] == country_code].set_index('date')['value']
    country_inf = inf_data[inf_data['country_code'] == country_code].set_index('date')['value']
    
    # Combine into a single DataFrame
    combined_data = pd.DataFrame({
        'GDP': country_gdp,
        'Employment': country_emp,
        'Inflation': country_inf
    })
    
    # Calculate correlations
    correlations = combined_data.corr()
    
    # Save to MongoDB
    correlation_dict = {
        'country_code': country_code,
        'correlations': correlations.to_dict(),
        'calculation_date': datetime.utcnow()
    }
    save_to_mongodb(mongo_db, 'correlations', correlation_dict,
                   identifier={'country_code': country_code})
    
    print(f"\nCorrelations for {country_code}:")
    print(correlations.round(3))
    
    return correlations

def calculate_key_metrics(gdp_data, emp_data, inf_data, countries, mongo_db):
    """Calculate and display key metrics for each country and save to MongoDB"""
    
    results = {}
    
    for country in countries:
        # Calculate metrics
        gdp_growth = gdp_data[gdp_data['country_code'] == country]['value'].pct_change().mean() * 100
        emp_change = emp_data[emp_data['country_code'] == country]['value'].diff().mean()
        inf_avg = inf_data[inf_data['country_code'] == country]['value'].mean()
        
        results[country] = {
            'Average GDP Growth (%)': round(gdp_growth, 2),
            'Average Employment Change': round(emp_change, 2),
            'Average Inflation Rate (%)': round(inf_avg, 2)
        }
    
    # Save to MongoDB
    metrics_dict = {
        'metrics': results,
        'calculation_date': datetime.utcnow()
    }
    save_to_mongodb(mongo_db, 'key_metrics', metrics_dict)
    
    # Convert to DataFrame for better display
    results_df = pd.DataFrame(results).T
    print("\nKey Economic Metrics by Country:")
    print(results_df)
    
    return results_df

def plot_indicator_trends(gdp_df, emp_df, inf_df):
    """Create interactive plots for economic indicators"""
    
    # Create figure with secondary y-axis
    fig = make_subplots(rows=3, cols=1,
                        subplot_titles=('GDP Trends', 'Employment Trends', 'Inflation Trends'),
                        vertical_spacing=0.1)

    # Add traces for each country
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

    # Update layout
    fig.update_layout(
        height=900,
        title_text="Economic Indicators Over Time",
        showlegend=True
    )

    # Save the plot
    fig.write_html("economic_indicators.html")
    print("\nVisualization saved as 'economic_indicators.html'")

def main():
    """Main analysis function"""
    # Setup
    pg_engine, mongo_db = setup_database_connections()
    
    # Load data from PostgreSQL
    print("Loading data from PostgreSQL...")
    gdp_data, emp_data, inf_data = load_economic_data(pg_engine)
    
    # Preprocess data
    print("\nPreprocessing data...")
    gdp_data = preprocess_data(gdp_data)
    emp_data = preprocess_data(emp_data)
    inf_data = preprocess_data(inf_data)
    
    # Save preprocessed data to MongoDB
    print("\nSaving preprocessed data to MongoDB...")
    save_to_mongodb(mongo_db, 'preprocessed_gdp', gdp_data)
    save_to_mongodb(mongo_db, 'preprocessed_employment', emp_data)
    save_to_mongodb(mongo_db, 'preprocessed_inflation', inf_data)
    
    # Calculate summary statistics
    print("\nCalculating summary statistics...")
    gdp_stats = calculate_summary_statistics(gdp_data, "GDP", mongo_db)
    emp_stats = calculate_summary_statistics(emp_data, "Employment", mongo_db)
    inf_stats = calculate_summary_statistics(inf_data, "Inflation", mongo_db)
    
    # Create visualizations
    print("\nCreating visualizations...")
    plot_indicator_trends(gdp_data, emp_data, inf_data)
    
    # Calculate correlations
    print("\nCalculating correlations...")
    countries = gdp_data['country_code'].unique()
    correlation_results = {
        country: calculate_correlations(gdp_data, emp_data, inf_data, country, mongo_db) 
        for country in countries
    }
    
    # Calculate key metrics
    print("\nCalculating key metrics...")
    key_metrics = calculate_key_metrics(gdp_data, emp_data, inf_data, countries, mongo_db)
    
    print("\nAnalysis completed successfully!")

if __name__ == "__main__":
    main()
