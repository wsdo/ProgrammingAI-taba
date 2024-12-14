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
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

# MongoDB configuration
MONGODB_URI = 'mongodb+srv://nci:8YWJ0hBAGmZcNo6q74UQ@cluster0.x1ijb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
MONGODB_CONFIG = {
    'uri': MONGODB_URI,
    'db': os.getenv('MONGODB_DB', 'education_db')
}

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
        client = MongoClient(MONGODB_CONFIG['uri'])
        db = client[MONGODB_CONFIG['db']]
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
def analyze_education_metrics(pg_conn, metric_name):
    """Analyze education metrics from PostgreSQL database"""
    if pg_conn is None:
        return None
    
    try:
        with pg_conn.cursor() as cur:
            # Calculate basic statistics
            cur.execute("""
                SELECT 
                    year,
                    AVG(metric_value) as mean_value,
                    STDDEV(metric_value) as std_value,
                    MIN(metric_value) as min_value,
                    MAX(metric_value) as max_value
                FROM raw_education_data
                WHERE metric_name = %s
                GROUP BY year
                ORDER BY year;
            """, (metric_name,))
            
            results = cur.fetchall()
            stats_df = pd.DataFrame(results, 
                                  columns=['year', 'mean', 'std', 'min', 'max'])
            
            # Create visualizations
            trend_fig = px.line(stats_df, x='year', y='mean',
                              title=f'{metric_name} Trend Over Time')
            
            # Store analysis results
            values = [(
                'basic_stats',
                metric_name,
                row['year'],
                None,  # country
                row['mean']
            ) for _, row in stats_df.iterrows()]
            
            execute_values(cur, """
                INSERT INTO analysis_results 
                (analysis_type, metric_name, year, country, result_value)
                VALUES %s
            """, values)
            
            pg_conn.commit()
            
            return {
                'stats': stats_df,
                'trend_plot': trend_fig
            }
            
    except Exception as e:
        print(f"Error analyzing {metric_name}: {str(e)}")
        return None

def forecast_metric(pg_conn, metric_name, periods=5):
    """Forecast future values for a metric"""
    if pg_conn is None:
        return None
    
    try:
        with pg_conn.cursor() as cur:
            # Get historical data
            cur.execute("""
                SELECT year, AVG(metric_value) as avg_value
                FROM raw_education_data
                WHERE metric_name = %s
                GROUP BY year
                ORDER BY year
            """, (metric_name,))
            
            results = cur.fetchall()
            historical_df = pd.DataFrame(results, columns=['year', 'value'])
            
            # Fit SARIMA model
            model = SARIMAX(historical_df['value'], 
                          order=(1, 1, 1), 
                          seasonal_order=(1, 1, 1, 12))
            results = model.fit()
            
            # Generate forecast
            forecast = results.forecast(periods)
            
            # Store forecast results
            last_year = historical_df['year'].max()
            forecast_values = [(
                metric_name,
                last_year + i + 1,
                float(forecast[i]),
                float(results.conf_int().iloc[i, 0]),
                float(results.conf_int().iloc[i, 1])
            ) for i in range(len(forecast))]
            
            execute_values(cur, """
                INSERT INTO forecasts 
                (metric_name, forecast_year, forecast_value, 
                 confidence_lower, confidence_upper)
                VALUES %s
            """, forecast_values)
            
            pg_conn.commit()
            
            # Create visualization
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=historical_df['year'], 
                                   y=historical_df['value'],
                                   name='Historical'))
            fig.add_trace(go.Scatter(x=range(last_year + 1, last_year + periods + 1),
                                   y=forecast,
                                   name='Forecast'))
            
            return {
                'forecast': forecast,
                'plot': fig
            }
            
    except Exception as e:
        print(f"Error forecasting {metric_name}: {str(e)}")
        return None

def store_results_in_mongodb(mongo_db, analysis_results, forecast_results):
    """Store analysis and forecast results in MongoDB"""
    try:
        # Store analysis results
        analysis_collection = mongo_db['analysis_results']
        for metric, results in analysis_results.items():
            if results is not None:
                for year_data in results['stats'].to_dict('records'):
                    analysis_collection.update_one(
                        {'metric': metric, 'year': year_data['year']},
                        {'$set': {
                            'mean_value': year_data['mean'],
                            'std_value': year_data['std'],
                            'min_value': year_data['min'],
                            'max_value': year_data['max'],
                            'updated_at': datetime.now()
                        }},
                        upsert=True
                    )
        
        # Store forecast results
        forecast_collection = mongo_db['forecast_results']
        for metric, results in forecast_results.items():
            if results is not None:
                for i, forecast in enumerate(results['forecast']):
                    forecast_collection.update_one(
                        {'metric': metric, 'forecast_year': results['plot'].data[1].x[i]},
                        {'$set': {
                            'forecast_value': forecast,
                            'confidence_lower': results['plot'].data[1].error_y.array[i][0],
                            'confidence_upper': results['plot'].data[1].error_y.array[i][1],
                            'updated_at': datetime.now()
                        }},
                        upsert=True
                    )
        print("Successfully stored analysis and forecast results in MongoDB")
    except Exception as e:
        print(f"Error storing results in MongoDB: {str(e)}")

# 8. Main Analysis Function
def run_education_analysis():
    """Run the complete education data analysis"""
    pg_conn = None
    mongo_db = None
    try:
        # Connect to databases
        pg_conn = get_postgres_connection()
        mongo_db = get_mongodb_connection()
        
        if pg_conn is None or mongo_db is None:
            raise Exception("Failed to connect to databases")
        
        # Setup schema
        setup_postgres_schema(pg_conn)
        
        # Collect and store data
        collect_and_store_education_data(pg_conn, mongo_db)
        
        # Analyze each metric
        metrics = ['education_investment', 'student_teacher_ratio', 
                  'completion_rate', 'literacy_rate']
        
        analysis_results = {}
        forecast_results = {}
        
        for metric in metrics:
            print(f"\nAnalyzing {metric}...")
            analysis_results[metric] = analyze_education_metrics(pg_conn, metric)
            forecast_results[metric] = forecast_metric(pg_conn, metric)
        
        # Store results in MongoDB
        if analysis_results and forecast_results:
            store_results_in_mongodb(mongo_db, analysis_results, forecast_results)
        
        return {
            'analysis_results': analysis_results,
            'forecast_results': forecast_results
        }
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        return None
    
    finally:
        # Clean up connections
        if pg_conn is not None:
            pg_conn.close()
        if 'mongo_db' in locals() and mongo_db is not None and mongo_db.client is not None:
            mongo_db.client.close()

if __name__ == "__main__":
    results = run_education_analysis()
