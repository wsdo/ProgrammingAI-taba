"""
MongoDB Analysis Script
This script demonstrates how to retrieve and use data from MongoDB collections
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
from dotenv import load_dotenv

def connect_mongodb():
    """Connect to MongoDB database"""
    load_dotenv()
    
    # MongoDB connection
    mongo_client = MongoClient(
        f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
        f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
        "?authSource=admin"
    )
    return mongo_client[os.getenv('MONGODB_DB')]

def get_preprocessed_data(db):
    """Retrieve preprocessed data from MongoDB"""
    # Convert MongoDB documents to pandas DataFrames
    gdp_df = pd.DataFrame(list(db.preprocessed_gdp.find()))
    emp_df = pd.DataFrame(list(db.preprocessed_employment.find()))
    inf_df = pd.DataFrame(list(db.preprocessed_inflation.find()))
    
    print("\nPreprocessed Data Sample:")
    print("\nGDP Data:")
    print(gdp_df.head())
    
    return gdp_df, emp_df, inf_df

def get_summary_statistics(db):
    """Retrieve and display summary statistics"""
    stats = list(db.summary_statistics.find())
    
    print("\nSummary Statistics:")
    for stat in stats:
        print(f"\n{stat['indicator']} Statistics:")
        df = pd.DataFrame(stat['statistics']).T
        print(df)
    
    return stats

def get_correlations(db):
    """Retrieve and display correlations"""
    correlations = list(db.correlations.find())
    
    print("\nCorrelations by Country:")
    correlation_dict = {}
    for corr in correlations:
        country = corr['country_code']
        corr_df = pd.DataFrame(corr['correlations'])
        correlation_dict[country] = corr_df
        print(f"\n{country}:")
        print(corr_df)
    
    return correlation_dict

def get_key_metrics(db):
    """Retrieve and display key metrics"""
    metrics = db.key_metrics.find_one()
    if metrics:
        df = pd.DataFrame(metrics['metrics']).T
        print("\nKey Economic Metrics:")
        print(df)
        return df
    return None

def visualize_correlations(correlation_dict):
    """Create heatmap visualization for correlations"""
    countries = list(correlation_dict.keys())
    n_countries = len(countries)
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    fig.suptitle('Correlation Heatmaps by Country')
    
    for i, (country, corr_df) in enumerate(correlation_dict.items()):
        row = i // 3
        col = i % 3
        sns.heatmap(corr_df, annot=True, cmap='coolwarm', center=0,
                   ax=axes[row, col])
        axes[row, col].set_title(f'{country}')
    
    plt.tight_layout()
    plt.savefig('correlation_heatmaps.png')
    print("\nCorrelation heatmaps saved as 'correlation_heatmaps.png'")

def analyze_trends(gdp_df, emp_df, inf_df):
    """Analyze trends in preprocessed data"""
    # Convert timestamp to datetime if needed
    for df in [gdp_df, emp_df, inf_df]:
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
    
    # Create time series plots
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
    fig.write_html("mongodb_trends.html")
    print("\nTrends visualization saved as 'mongodb_trends.html'")

def main():
    """Main function to demonstrate MongoDB data retrieval and analysis"""
    try:
        # Connect to MongoDB
        print("Connecting to MongoDB...")
        db = connect_mongodb()
        
        # Retrieve and display data
        print("\nRetrieving data from MongoDB...")
        gdp_df, emp_df, inf_df = get_preprocessed_data(db)
        stats = get_summary_statistics(db)
        correlation_dict = get_correlations(db)
        metrics = get_key_metrics(db)
        
        # Create visualizations
        print("\nCreating visualizations...")
        visualize_correlations(correlation_dict)
        analyze_trends(gdp_df, emp_df, inf_df)
        
        print("\nAnalysis completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
