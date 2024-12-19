"""
Economic Data Analysis for Major EU Countries
This notebook analyzes economic indicators (GDP, Employment, Inflation) for major EU economies
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Set style for better visualizations
sns.set_theme()

# Data paths
DATA_DIR = Path("../data/processed")
GDP_FILE = DATA_DIR / "gdp_2023_processed_20241215.csv"
EMPLOYMENT_FILE = DATA_DIR / "employment_2023_processed_20241215.csv"
INFLATION_FILE = DATA_DIR / "inflation_2023_processed_20241215.csv"

# Load data
def load_data():
    """Load and prepare all economic data"""
    gdp_df = pd.read_csv(GDP_FILE)
    employment_df = pd.read_csv(EMPLOYMENT_FILE)
    inflation_df = pd.read_csv(INFLATION_FILE)
    
    # Convert date columns to datetime
    for df in [gdp_df, employment_df, inflation_df]:
        df['date'] = pd.to_datetime(df['date'])
    
    return gdp_df, employment_df, inflation_df

# Load the data
gdp_df, employment_df, inflation_df = load_data()

# 1. GDP Analysis and Visualization
def plot_gdp_trends():
    """Plot GDP trends for all countries"""
    fig = px.line(gdp_df, x='date', y='value', color='country',
                  title='GDP Trends for Major EU Economies',
                  labels={'value': 'GDP', 'date': 'Year', 'country': 'Country'})
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="GDP",
        legend_title="Country",
        hovermode='x unified'
    )
    fig.write_html("../visualizations/gdp_trends.html")
    return fig

def plot_gdp_growth_rates():
    """Calculate and plot GDP growth rates"""
    # Calculate year-over-year growth rates
    gdp_pivot = gdp_df.pivot(index='date', columns='country', values='value')
    growth_rates = gdp_pivot.pct_change(periods=4) * 100  # 4 quarters for annual growth
    
    fig = px.line(growth_rates.reset_index().melt(id_vars=['date']),
                  x='date', y='value', color='country',
                  title='GDP Growth Rates for Major EU Economies',
                  labels={'value': 'Growth Rate (%)', 'date': 'Year', 'country': 'Country'})
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Growth Rate (%)",
        legend_title="Country",
        hovermode='x unified'
    )
    fig.write_html("../visualizations/gdp_growth_rates.html")
    return fig

# 2. Employment Analysis and Visualization
def plot_employment_trends():
    """Plot employment trends for all countries"""
    fig = px.line(employment_df, x='date', y='value', color='country',
                  title='Employment Trends in Major EU Economies',
                  labels={'value': 'Employment Rate (%)', 'date': 'Year', 'country': 'Country'})
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Employment Rate (%)",
        legend_title="Country",
        hovermode='x unified'
    )
    fig.write_html("../visualizations/employment_trends.html")
    return fig

# 3. Inflation Analysis and Visualization
def plot_inflation_trends():
    """Plot inflation trends for all countries"""
    fig = px.line(inflation_df, x='date', y='value', color='country',
                  title='Inflation Rates in Major EU Economies',
                  labels={'value': 'Inflation Rate (%)', 'date': 'Year', 'country': 'Country'})
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Inflation Rate (%)",
        legend_title="Country",
        hovermode='x unified'
    )
    fig.write_html("../visualizations/inflation_trends.html")
    return fig

# 4. Correlation Analysis
def plot_correlation_matrix():
    """Create correlation matrix between different economic indicators"""
    # Prepare data for correlation analysis
    merged_data = pd.DataFrame()
    
    for country in gdp_df['country'].unique():
        country_data = pd.DataFrame()
        
        # GDP
        gdp_country = gdp_df[gdp_df['country'] == country].set_index('date')['value']
        country_data['GDP'] = gdp_country
        
        # Employment
        emp_country = employment_df[employment_df['country'] == country].set_index('date')['value']
        country_data['Employment'] = emp_country
        
        # Inflation
        inf_country = inflation_df[inflation_df['country'] == country].set_index('date')['value']
        country_data['Inflation'] = inf_country
        
        country_data['Country'] = country
        merged_data = pd.concat([merged_data, country_data])
    
    # Calculate correlation matrix
    corr_matrix = merged_data.groupby('Country')[['GDP', 'Employment', 'Inflation']].corr()
    
    # Create correlation heatmap using plotly
    countries = merged_data['Country'].unique()
    fig = make_subplots(rows=2, cols=3,
                        subplot_titles=countries,
                        vertical_spacing=0.15)
    
    for i, country in enumerate(countries):
        row = (i // 3) + 1
        col = (i % 3) + 1
        
        country_corr = merged_data[merged_data['Country'] == country][['GDP', 'Employment', 'Inflation']].corr()
        
        heatmap = go.Heatmap(z=country_corr.values,
                            x=country_corr.columns,
                            y=country_corr.columns,
                            colorscale='RdBu',
                            zmin=-1, zmax=1)
        
        fig.add_trace(heatmap, row=row, col=col)
    
    fig.update_layout(height=800, width=1200,
                     title_text="Correlation Matrix of Economic Indicators by Country")
    fig.write_html("../visualizations/correlation_matrix.html")
    return fig

# Create visualizations directory if it doesn't exist
Path("../visualizations").mkdir(exist_ok=True)

# Generate all visualizations
gdp_fig = plot_gdp_trends()
growth_fig = plot_gdp_growth_rates()
employment_fig = plot_employment_trends()
inflation_fig = plot_inflation_trends()
correlation_fig = plot_correlation_matrix()

print("All visualizations have been generated and saved in the '../visualizations' directory.")
