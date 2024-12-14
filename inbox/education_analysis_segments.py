"""
Education Data Analysis Code Segments
This file contains code segments that can be run in a Jupyter notebook
"""

# Segment 1: Import required libraries
"""
首先导入所需的库和设置基本配置
"""
import os
import logging
from datetime import datetime
import numpy as np
import pandas as pd
import eurostat
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
import time
from pymongo import UpdateOne
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
import seaborn as sns
from plotly import graph_objects as go
from plotly.subplots import make_subplots

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Segment 2: Database Connection Functions
"""
数据库连接函数
- get_postgres_connection(): 连接到PostgreSQL数据库
- get_mongodb_connection(): 连接到MongoDB数据库
"""
def get_postgres_connection():
    """Get PostgreSQL connection with retry mechanism"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT'),
                connect_timeout=30
            )
            print("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            print(f"Attempt {retry_count + 1} failed: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2)
    
    print("Failed to connect to PostgreSQL after all retries")
    return None

def get_mongodb_connection():
    """Get MongoDB connection with retry mechanism"""
    try:
        client = MongoClient(
            host=os.getenv('MONGODB_HOST'),
            port=int(os.getenv('MONGODB_PORT')),
            username=os.getenv('MONGODB_USER'),
            password=os.getenv('MONGODB_PASSWORD'),
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            retryWrites=True,
            w='majority'
        )
        db = client[os.getenv('MONGODB_DB')]
        client.server_info()
        print("Successfully connected to MongoDB")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None

# Segment 3: Database Setup
"""
设置PostgreSQL数据库表结构
"""
def setup_postgres_database(conn):
    """Set up PostgreSQL database tables"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_education_data (
                    id SERIAL PRIMARY KEY,
                    country VARCHAR(50),
                    year INTEGER,
                    metric_name VARCHAR(100),
                    metric_value FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(country, year, metric_name)
                );
                
                CREATE INDEX IF NOT EXISTS idx_country ON raw_education_data(country);
                CREATE INDEX IF NOT EXISTS idx_year ON raw_education_data(year);
                CREATE INDEX IF NOT EXISTS idx_metric ON raw_education_data(metric_name);
            """)
            conn.commit()
            print("Successfully set up PostgreSQL database")
    except Exception as e:
        print(f"Error setting up PostgreSQL database: {str(e)}")
        conn.rollback()

# Segment 4: Data Collection
"""
从Eurostat收集教育数据
"""
def collect_eurostat_data(metric_name):
    """Collect education data from Eurostat with error handling"""
    try:
        dataset_codes = {
            'education_investment': 'educ_uoe_fine09',
            'student_teacher_ratio': 'educ_uoe_perp04',
            'completion_rate': 'edat_lfse_03'
        }
        
        if metric_name not in dataset_codes:
            raise ValueError(f"Unknown metric: {metric_name}")
            
        dataset_code = dataset_codes[metric_name]
        df = eurostat.get_data_df(dataset_code)
        
        if df is None or df.empty:
            print(f"No data found for {metric_name}")
            return None
            
        df = df.reset_index()
        
        time_cols = [col for col in df.columns if 'TIME' in col]
        if not time_cols:
            print(f"No time column found for {metric_name}")
            return None
            
        time_col = time_cols[0]
        df['country'] = df[time_col].str.split('\\').str[0]
        
        year_cols = [col for col in df.columns if str(col).isdigit()]
        if not year_cols:
            print(f"No year columns found for {metric_name}")
            return None
            
        processed_data = []
        for year in year_cols:
            year_data = df[['country', year]].copy()
            year_data = year_data.rename(columns={year: 'value'})
            year_data['year'] = int(year)
            processed_data.append(year_data)
            
        result_df = pd.concat(processed_data, ignore_index=True)
        result_df['value'] = pd.to_numeric(result_df['value'], errors='coerce')
        result_df = result_df.dropna(subset=['value'])
        
        return result_df
        
    except Exception as e:
        print(f"Error collecting {metric_name} data: {str(e)}")
        return None

# Segment 5: Data Analysis Functions
"""
数据分析函数
- analyze_education_metrics(): 分析教育指标
- generate_forecasts(): 生成预测
- compare_countries(): 比较不同国家的指标
"""
def analyze_education_metrics(mongo_db, country=None, year_range=None):
    """Analyze education metrics with advanced analytics"""
    try:
        results = {}
        metrics = ['education_investment', 'student_teacher_ratio', 'completion_rate']
        
        for metric in metrics:
            collection = mongo_db[metric]
            query = {}
            if country:
                query['country'] = country
            if year_range:
                query['year'] = {'$gte': year_range[0], '$lte': year_range[1]}
            
            cursor = collection.find(query)
            df = pd.DataFrame(list(cursor))
            
            if not df.empty:
                stats = {
                    'mean': df['value'].mean(),
                    'median': df['value'].median(),
                    'std': df['value'].std(),
                    'min': df['value'].min(),
                    'max': df['value'].max()
                }
                
                if len(df) > 1:
                    df_sorted = df.sort_values('year')
                    trend = np.polyfit(df_sorted['year'], df_sorted['value'], 1)
                    stats['trend'] = {
                        'slope': trend[0],
                        'intercept': trend[1]
                    }
                    
                    df_sorted['yoy_change'] = df_sorted['value'].pct_change()
                    stats['avg_yoy_change'] = df_sorted['yoy_change'].mean()
                
                results[metric] = stats
        
        return results
        
    except Exception as e:
        logging.error(f"Error analyzing education metrics: {str(e)}")
        return None

def generate_forecasts(mongo_db, metric, country, forecast_years=5):
    """Generate forecasts using time series analysis"""
    try:
        collection = mongo_db[metric]
        cursor = collection.find({'country': country})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return None
            
        df_sorted = df.sort_values('year')
        model = ARIMA(df_sorted['value'], order=(1,1,1))
        results = model.fit()
        
        forecast = results.forecast(steps=forecast_years)
        forecast_years = range(df_sorted['year'].max() + 1, 
                             df_sorted['year'].max() + forecast_years + 1)
        
        forecast_data = {
            'years': list(forecast_years),
            'values': forecast.values.tolist(),
            'confidence_intervals': results.get_forecast(steps=forecast_years).conf_int().values.tolist()
        }
        
        return forecast_data
        
    except Exception as e:
        logging.error(f"Error generating forecasts: {str(e)}")
        return None

def compare_countries(mongo_db, countries, metric, year_range=None):
    """Compare education metrics across countries"""
    try:
        collection = mongo_db[metric]
        query = {'country': {'$in': countries}}
        if year_range:
            query['year'] = {'$gte': year_range[0], '$lte': year_range[1]}
        
        cursor = collection.find(query)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return None
        
        results = {}
        for country in countries:
            country_data = df[df['country'] == country]
            if not country_data.empty:
                stats = {
                    'mean': country_data['value'].mean(),
                    'latest_value': country_data.loc[country_data['year'].idxmax(), 'value'],
                    'trend': np.polyfit(country_data['year'], country_data['value'], 1)[0]
                }
                results[country] = stats
        
        return results
        
    except Exception as e:
        logging.error(f"Error comparing countries: {str(e)}")
        return None

# Segment 6: Visualization Functions
"""
数据可视化函数
"""
def plot_metric_trends(df, metric_name):
    """Plot trends for a specific metric"""
    plt.figure(figsize=(12, 6))
    for country in df['country'].unique():
        country_data = df[df['country'] == country]
        plt.plot(country_data['year'], country_data['value'], 
                marker='o', label=country)
    
    plt.title(f'{metric_name} Trends by Country')
    plt.xlabel('Year')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_country_comparison(comparison_results, metric_name):
    """Plot country comparison results"""
    countries = list(comparison_results.keys())
    means = [stats['mean'] for stats in comparison_results.values()]
    latest = [stats['latest_value'] for stats in comparison_results.values()]
    
    fig = go.Figure(data=[
        go.Bar(name='Mean Value', x=countries, y=means),
        go.Bar(name='Latest Value', x=countries, y=latest)
    ])
    
    fig.update_layout(
        title=f'{metric_name} Comparison Across Countries',
        barmode='group',
        xaxis_title='Country',
        yaxis_title='Value'
    )
    fig.show()

def plot_forecast(forecast_data, metric_name, country):
    """Plot forecast results"""
    years = forecast_data['years']
    values = forecast_data['values']
    ci = forecast_data['confidence_intervals']
    
    fig = go.Figure()
    
    # Add forecast line
    fig.add_trace(go.Scatter(
        x=years,
        y=values,
        mode='lines+markers',
        name='Forecast'
    ))
    
    # Add confidence intervals
    fig.add_trace(go.Scatter(
        x=years + years[::-1],
        y=[ci[i][0] for i in range(len(years))] + 
           [ci[i][1] for i in range(len(years)-1, -1, -1)],
        fill='toself',
        fillcolor='rgba(0,100,80,0.2)',
        line=dict(color='rgba(255,255,255,0)'),
        name='95% Confidence Interval'
    ))
    
    fig.update_layout(
        title=f'{metric_name} Forecast for {country}',
        xaxis_title='Year',
        yaxis_title='Value',
        showlegend=True
    )
    fig.show()

def analyze_and_visualize_data():
    """Main function to analyze and visualize education data"""
    try:
        # 1. 设置环境变量
        os.environ.update({
            'POSTGRES_USER': 'nci',
            'POSTGRES_PASSWORD': 'yHyULpyUXZ4y32gdEi80',
            'POSTGRES_HOST': '47.91.31.227',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'education_db',
            'MONGODB_HOST': '47.91.31.227',
            'MONGODB_PORT': '27017',
            'MONGODB_USER': 'nci',
            'MONGODB_PASSWORD': 'xJcTB7fnyA17GNuQk3Aa',
            'MONGODB_DB': 'education_db'
        })

        # 2. 连接数据库
        print("Connecting to databases...")
        pg_conn = get_postgres_connection()
        mongo_db = get_mongodb_connection()

        if pg_conn is None or mongo_db is None:
            print("Failed to connect to databases")
            return

        # 3. 设置数据库表
        print("\nSetting up database tables...")
        setup_postgres_database(pg_conn)

        # 4. 收集和存储数据
        metrics = ['education_investment', 'student_teacher_ratio', 'completion_rate']
        print("\nCollecting and analyzing data...")
        
        for metric in metrics:
            print(f"\nProcessing {metric}...")
            df = collect_eurostat_data(metric)
            
            if df is not None:
                print(f"\nSample data for {metric}:")
                print(df.head())
                print("\nBasic statistics:")
                print(df['value'].describe())
                
                # Plot trend
                print(f"\nPlotting trends for {metric}...")
                plot_metric_trends(df, metric)

        # 5. 分析特定国家数据
        eu_countries = ['DE', 'FR', 'IT', 'ES', 'NL']
        year_range = (2010, 2023)
        
        for country in eu_countries:
            print(f"\nAnalyzing data for {country}...")
            metrics_analysis = analyze_education_metrics(mongo_db, country, year_range)
            
            if metrics_analysis:
                print(f"\nMetrics Analysis for {country}:")
                for metric, stats in metrics_analysis.items():
                    print(f"\n{metric.upper()}:")
                    print(f"Mean: {stats['mean']:.2f}")
                    print(f"Median: {stats['median']:.2f}")
                    if 'trend' in stats:
                        print(f"Trend slope: {stats['trend']['slope']:.4f}")

        # 6. 生成和可视化预测
        print("\nGenerating forecasts...")
        for metric in metrics:
            for country in ['DE', 'FR']:  # 选择主要国家进行预测
                forecast = generate_forecasts(mongo_db, metric, country)
                if forecast:
                    print(f"\nForecast for {metric} in {country}:")
                    for year, value in zip(forecast['years'], forecast['values']):
                        print(f"{year}: {value:.2f}")
                    plot_forecast(forecast, metric, country)

        # 7. 国家间比较和可视化
        print("\nComparing countries...")
        for metric in metrics:
            comparison = compare_countries(mongo_db, eu_countries, metric, year_range)
            if comparison:
                print(f"\n{metric.upper()} Comparison:")
                for country, stats in comparison.items():
                    print(f"\n{country}:")
                    print(f"Mean: {stats['mean']:.2f}")
                    print(f"Latest Value: {stats['latest_value']:.2f}")
                    print(f"Trend: {stats['trend']:.4f}")
                plot_country_comparison(comparison, metric)

    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        import traceback
        print(traceback.format_exc())
    
    finally:
        if pg_conn:
            pg_conn.close()
            print("\nClosed PostgreSQL connection")

if __name__ == "__main__":
    analyze_and_visualize_data()
