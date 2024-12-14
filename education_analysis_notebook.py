"""
Education Data Analysis Project
This script contains the complete code for analyzing education data across different countries.
The analysis includes data collection, storage, and analysis using both SQL and NoSQL databases.
"""

# 1. Required Imports
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
from statsmodels.tsa.statespace.sarimax import SARIMAX
import eurostat
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

# 2. Configure Logging
logging.basicConfig(
    filename='education_data_collection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 3. Database Configuration
load_dotenv()

# PostgreSQL configuration
POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'education_db'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# MongoDB configuration
MONGODB_HOST = os.getenv('MONGODB_HOST')
MONGODB_PORT = int(os.getenv('MONGODB_PORT'))
MONGODB_USER = os.getenv('MONGODB_USER')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')
MONGODB_DB = os.getenv('MONGODB_DB')

# 4. Database Connection Functions
def get_postgres_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        return None

def get_mongodb_connection():
    """Create a connection to MongoDB database"""
    try:
        client = MongoClient(
            host=MONGODB_HOST,
            port=MONGODB_PORT,
            username=MONGODB_USER,
            password=MONGODB_PASSWORD,
            authSource='admin'
        )
        db = client[MONGODB_DB]
        print("Successfully connected to MongoDB")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None

# 5. Database Schema Setup
def setup_postgres_schema(conn):
    """Set up PostgreSQL database schema"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_education_data (
                    id SERIAL PRIMARY KEY,
                    country VARCHAR(100),
                    year INTEGER,
                    metric_name VARCHAR(100),
                    metric_value FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(country, year, metric_name)
                );

                CREATE TABLE IF NOT EXISTS analysis_results (
                    id SERIAL PRIMARY KEY,
                    metric_name VARCHAR(100),
                    year INTEGER,
                    mean_value FLOAT,
                    std_value FLOAT,
                    min_value FLOAT,
                    max_value FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(metric_name, year)
                );

                CREATE TABLE IF NOT EXISTS forecasts (
                    id SERIAL PRIMARY KEY,
                    metric_name VARCHAR(100),
                    forecast_year INTEGER,
                    forecast_value FLOAT,
                    confidence_lower FLOAT,
                    confidence_upper FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(metric_name, forecast_year)
                );
            """)
            conn.commit()
            print("PostgreSQL schema setup completed")
    except Exception as e:
        print(f"Error setting up PostgreSQL schema: {str(e)}")
        conn.rollback()

# 6. Data Collection and Storage
def collect_and_store_education_data(pg_conn, mongo_db):
    """Collect education data from Eurostat and store in databases"""
    datasets = {
        'education_investment': 'educ_uoe_fine09',
        'student_teacher_ratio': 'educ_uoe_perp04',
        'completion_rate': 'edat_lfse_03',
        'literacy_rate': 'edat_lfse_01'
    }
    
    for metric, code in datasets.items():
        try:
            print(f"Collecting {metric} data...")
            df = eurostat.get_data_df(code)
            
            # Process data
            df = df.reset_index()
            
            # Get the geo/time column
            time_col = [col for col in df.columns if 'TIME' in col][0]
            
            # Extract country from the time column
            df['country'] = df[time_col].str.split('\\').str[0]
            
            # Get year columns (they should be numeric)
            year_columns = [col for col in df.columns if str(col).isdigit()]
            
            # Create a new dataframe for the processed data
            processed_data = []
            
            # Process each year column
            for year in year_columns:
                year_data = df[['country', year] + [col for col in df.columns if col not in year_columns and col != time_col]]
                year_data = year_data.rename(columns={year: 'value'})
                year_data['year'] = int(year)
                year_data['metric'] = metric
                processed_data.append(year_data)
            
            # Combine all years
            df_processed = pd.concat(processed_data, ignore_index=True)
            
            # Convert value column to float
            df_processed['value'] = pd.to_numeric(df_processed['value'], errors='coerce')
            
            # Drop rows with missing values
            df_processed = df_processed.dropna(subset=['value'])
            
            # Store in PostgreSQL
            if pg_conn is not None:
                try:
                    with pg_conn.cursor() as cur:
                        # Convert DataFrame to list of tuples for PostgreSQL
                        values = []
                        for _, row in df_processed.iterrows():
                            try:
                                values.append((
                                    str(row['country']),
                                    int(row['year']),
                                    str(metric),
                                    float(row['value'])
                                ))
                            except (ValueError, TypeError) as e:
                                print(f"Skipping row due to conversion error: {e}")
                                continue
                        
                        if values:
                            execute_values(cur, """
                                INSERT INTO raw_education_data (country, year, metric_name, metric_value)
                                VALUES %s
                                ON CONFLICT (country, year, metric_name) 
                                DO UPDATE SET metric_value = EXCLUDED.metric_value;
                            """, values)
                            pg_conn.commit()
                            print(f"Stored {len(values)} records in PostgreSQL for {metric}")
                except Exception as e:
                    print(f"Error storing data in PostgreSQL: {str(e)}")
                    pg_conn.rollback()
            
            # Store in MongoDB
            if mongo_db is not None:
                collection = mongo_db[metric]
                # Convert DataFrame to records
                records = df_processed.to_dict('records')
                
                # Store each record
                successful_inserts = 0
                for record in records:
                    try:
                        mongo_doc = {
                            'country': str(record['country']),
                            'year': int(record['year']),
                            'value': float(record['value']),
                            'metadata': {
                                'freq': str(record.get('freq', '')),
                                'unit': str(record.get('unit', '')),
                                'isced11': str(record.get('isced11', '')),
                            },
                            'updated_at': datetime.now()
                        }
                        collection.update_one(
                            {'country': mongo_doc['country'], 'year': mongo_doc['year']},
                            {'$set': mongo_doc},
                            upsert=True
                        )
                        successful_inserts += 1
                    except (ValueError, TypeError) as e:
                        print(f"Skipping MongoDB record due to conversion error: {e}")
                        continue
                
                print(f"Stored {successful_inserts} records in MongoDB for {metric}")
            
            print(f"Successfully stored {metric} data")
            
        except Exception as e:
            print(f"Error processing {metric} data: {str(e)}")
            if pg_conn is not None:
                pg_conn.rollback()
            import traceback
            print(traceback.format_exc())
            continue

# 7. Data Analysis Functions
def analyze_education_metrics(mongo_db, metric_name):
    """
    Analyze education metrics from MongoDB
    
    Args:
        mongo_db: MongoDB connection
        metric_name: Name of the metric to analyze
    """
    try:
        # Get data from MongoDB
        collection = mongo_db[metric_name]
        cursor = collection.find({}, {'country': 1, 'year': 1, 'value': 1, '_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            print(f"No data found for {metric_name}")
            return None
        
        # Group by year and calculate statistics
        stats = df.groupby('year')['value'].agg([
            ('mean', 'mean'),
            ('std', 'std'),
            ('min', 'min'),
            ('max', 'max')
        ]).reset_index()
        
        # Store analysis results in MongoDB
        analysis_collection = mongo_db['analysis_results']
        
        for _, row in stats.iterrows():
            analysis_doc = {
                'metric': metric_name,
                'year': int(row['year']),
                'statistics': {
                    'mean': float(row['mean']),
                    'std': float(row['std']),
                    'min': float(row['min']),
                    'max': float(row['max'])
                },
                'updated_at': datetime.now()
            }
            
            analysis_collection.update_one(
                {'metric': metric_name, 'year': analysis_doc['year']},
                {'$set': analysis_doc},
                upsert=True
            )
        
        print(f"Analysis completed for {metric_name}")
        return {'metric': metric_name, 'stats': stats}
        
    except Exception as e:
        print(f"Error analyzing {metric_name}: {str(e)}")
        return None

def forecast_metric(mongo_db, metric_name, periods=5):
    """
    Forecast future values for a metric using data from MongoDB
    
    Args:
        mongo_db: MongoDB connection
        metric_name: Name of the metric to forecast
        periods: Number of periods to forecast
    """
    try:
        # Get data from MongoDB
        collection = mongo_db[metric_name]
        cursor = collection.find({}, {'country': 1, 'year': 1, 'value': 1, '_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            print(f"No data found for {metric_name}")
            return None
        
        # Calculate average value per year
        yearly_avg = df.groupby('year')['value'].mean().reset_index()
        yearly_avg = yearly_avg.sort_values('year')
        
        # Prepare time series data
        y = yearly_avg['value']
        
        # Create and fit SARIMA model
        model = SARIMAX(y, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
        results = model.fit()
        
        # Make forecast
        forecast = results.forecast(periods)
        conf_int = results.get_forecast(periods).conf_int()
        
        # Generate future years
        last_year = yearly_avg['year'].max()
        future_years = range(last_year + 1, last_year + periods + 1)
        
        # Store forecast results in MongoDB
        forecast_collection = mongo_db['forecast_results']
        
        for i, year in enumerate(future_years):
            forecast_doc = {
                'metric': metric_name,
                'forecast_year': int(year),
                'forecast_value': float(forecast[i]),
                'confidence_interval': {
                    'lower': float(conf_int.iloc[i, 0]),
                    'upper': float(conf_int.iloc[i, 1])
                },
                'updated_at': datetime.now()
            }
            
            forecast_collection.update_one(
                {'metric': metric_name, 'forecast_year': forecast_doc['forecast_year']},
                {'$set': forecast_doc},
                upsert=True
            )
        
        print(f"Forecast completed for {metric_name}")
        return {
            'metric': metric_name,
            'forecast': forecast,
            'conf_int': conf_int,
            'years': list(future_years)
        }
        
    except Exception as e:
        print(f"Error forecasting {metric_name}: {str(e)}")
        return None

def visualize_metric_analysis(mongo_db, metric_name):
    """
    Create visualizations for metric analysis using data from MongoDB
    
    Args:
        mongo_db: MongoDB connection
        metric_name: Name of the metric to visualize
    """
    try:
        # Get raw data
        collection = mongo_db[metric_name]
        cursor = collection.find({}, {'country': 1, 'year': 1, 'value': 1, '_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            print(f"No data found for {metric_name}")
            return None
        
        # Create time series plot
        fig1 = px.line(df, x='year', y='value', color='country',
                      title=f'{metric_name} Over Time by Country')
        fig1.show()
        
        # Get analysis results
        analysis_collection = mongo_db['analysis_results']
        cursor = analysis_collection.find({'metric': metric_name})
        analysis_df = pd.DataFrame(list(cursor))
        
        if not analysis_df.empty:
            # Create statistics plot
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=analysis_df['year'], y=analysis_df['statistics'].apply(lambda x: x['mean']),
                                    mode='lines+markers', name='Mean'))
            fig2.add_trace(go.Scatter(x=analysis_df['year'], y=analysis_df['statistics'].apply(lambda x: x['mean'] + x['std']),
                                    mode='lines', name='Mean + Std', line=dict(dash='dash')))
            fig2.add_trace(go.Scatter(x=analysis_df['year'], y=analysis_df['statistics'].apply(lambda x: x['mean'] - x['std']),
                                    mode='lines', name='Mean - Std', line=dict(dash='dash')))
            fig2.update_layout(title=f'{metric_name} Statistics Over Time')
            fig2.show()
        
        # Get forecast results
        forecast_collection = mongo_db['forecast_results']
        cursor = forecast_collection.find({'metric': metric_name})
        forecast_df = pd.DataFrame(list(cursor))
        
        if not forecast_df.empty:
            # Create forecast plot
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=forecast_df['forecast_year'], y=forecast_df['forecast_value'],
                                    mode='lines+markers', name='Forecast'))
            fig3.add_trace(go.Scatter(x=forecast_df['forecast_year'],
                                    y=forecast_df['confidence_interval'].apply(lambda x: x['upper']),
                                    mode='lines', name='Upper CI', line=dict(dash='dash')))
            fig3.add_trace(go.Scatter(x=forecast_df['forecast_year'],
                                    y=forecast_df['confidence_interval'].apply(lambda x: x['lower']),
                                    mode='lines', name='Lower CI', line=dict(dash='dash')))
            fig3.update_layout(title=f'{metric_name} Forecast')
            fig3.show()
        
    except Exception as e:
        print(f"Error visualizing {metric_name}: {str(e)}")

def run_education_analysis():
    """Run the complete education data analysis using MongoDB"""
    # Connect to MongoDB
    mongo_db = get_mongodb_connection()
    if mongo_db is None:
        return
    
    metrics = [
        'education_investment',
        'student_teacher_ratio',
        'completion_rate',
        'literacy_rate'
    ]
    
    analysis_results = {}
    forecast_results = {}
    
    for metric in metrics:
        print(f"\nAnalyzing {metric}...")
        
        # Perform analysis
        analysis_result = analyze_education_metrics(mongo_db, metric)
        if analysis_result:
            analysis_results[metric] = analysis_result
        
        # Generate forecast
        forecast_result = forecast_metric(mongo_db, metric)
        if forecast_result:
            forecast_results[metric] = forecast_result
        
        # Create visualizations
        visualize_metric_analysis(mongo_db, metric)
    
    return {'analysis': analysis_results, 'forecasts': forecast_results}

if __name__ == "__main__":
    results = run_education_analysis()
