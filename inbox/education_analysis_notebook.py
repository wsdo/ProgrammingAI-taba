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
import time
from pymongo import UpdateOne
from statsmodels.tsa.arima.model import ARIMA

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
                connect_timeout=30  # Increase timeout to 30 seconds
            )
            print("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            print(f"Attempt {retry_count + 1} failed: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2)  # Wait 2 seconds before retrying
    
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
            serverSelectionTimeoutMS=30000,  # Increase timeout to 30 seconds
            connectTimeoutMS=30000,
            retryWrites=True,
            w='majority'
        )
        db = client[os.getenv('MONGODB_DB')]
        # Test connection
        client.server_info()
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

def setup_postgres_database(conn):
    """Set up PostgreSQL database tables"""
    try:
        with conn.cursor() as cur:
            # Create raw education data table
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
                
                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_country ON raw_education_data(country);
                CREATE INDEX IF NOT EXISTS idx_year ON raw_education_data(year);
                CREATE INDEX IF NOT EXISTS idx_metric ON raw_education_data(metric_name);
            """)
            
            conn.commit()
            print("Successfully set up PostgreSQL database")
            
    except Exception as e:
        print(f"Error setting up PostgreSQL database: {str(e)}")
        conn.rollback()

# 6. Data Processing and Storage Functions
def process_education_data(df, metric_type):
    """Process education data with error handling and data validation"""
    try:
        # Remove any duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Basic data cleaning
        df = df.dropna(how='all')  # Drop rows where all values are NaN
        
        # Convert numeric columns to float
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add metadata
        df['metric_type'] = metric_type
        df['processed_date'] = pd.Timestamp.now()
        
        # Validate data
        if df.empty:
            raise ValueError(f"No valid data found for {metric_type}")
            
        return df
        
    except Exception as e:
        logging.error(f"Error processing {metric_type} data: {str(e)}")
        return None

def store_in_mongodb(db, collection_name, records):
    """Store data in MongoDB with error handling and batch processing"""
    try:
        collection = db[collection_name]
        
        # Use bulk write operations for better performance
        operations = []
        for record in records:
            # Create unique identifier
            filter_dict = {
                'country': record.get('country'),
                'year': record.get('year'),
                'metric_type': record.get('metric_type')
            }
            
            operations.append(
                UpdateOne(
                    filter_dict,
                    {'$set': record},
                    upsert=True
                )
            )
            
        if operations:
            # Process in batches of 1000
            batch_size = 1000
            for i in range(0, len(operations), batch_size):
                batch = operations[i:i + batch_size]
                collection.bulk_write(batch, ordered=False)
                
        return True
        
    except Exception as e:
        logging.error(f"Error storing data in MongoDB: {str(e)}")
        return False

def store_in_postgres(conn, table_name, records):
    """Store data in PostgreSQL with error handling"""
    try:
        with conn.cursor() as cur:
            # Convert records to list of tuples for PostgreSQL
            values = []
            for record in records:
                try:
                    values.append((
                        str(record['country']),
                        int(record['year']),
                        str(record['metric_type']),
                        float(record['value'])
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Skipping record due to conversion error: {e}")
                    continue
            
            if values:
                execute_values(cur, f"""
                    INSERT INTO {table_name} (country, year, metric_name, metric_value)
                    VALUES %s
                    ON CONFLICT (country, year, metric_name) 
                    DO UPDATE SET metric_value = EXCLUDED.metric_value;
                """, values)
                conn.commit()
                print(f"Stored {len(values)} records in PostgreSQL for {table_name}")
                
    except Exception as e:
        print(f"Error storing data in PostgreSQL: {str(e)}")
        conn.rollback()

def collect_eurostat_data(metric_name):
    """Collect education data from Eurostat with error handling"""
    try:
        # Define dataset codes
        dataset_codes = {
            'education_investment': 'educ_uoe_fine09',
            'student_teacher_ratio': 'educ_uoe_perp04',
            'completion_rate': 'edat_lfse_03'
        }
        
        if metric_name not in dataset_codes:
            raise ValueError(f"Unknown metric: {metric_name}")
            
        # Get dataset code
        dataset_code = dataset_codes[metric_name]
        
        # Collect data from Eurostat
        df = eurostat.get_data_df(dataset_code)
        
        if df is None or df.empty:
            print(f"No data found for {metric_name}")
            return None
            
        # Process the DataFrame
        df = df.reset_index()
        
        # Get time column (should contain country codes)
        time_cols = [col for col in df.columns if 'TIME' in col]
        if not time_cols:
            print(f"No time column found for {metric_name}")
            return None
            
        time_col = time_cols[0]
        
        # Extract country from the time column
        df['country'] = df[time_col].str.split('\\').str[0]
        
        # Get year columns (numeric columns)
        year_cols = [col for col in df.columns if str(col).isdigit()]
        if not year_cols:
            print(f"No year columns found for {metric_name}")
            return None
            
        # Create processed dataframe
        processed_data = []
        
        # Process each year
        for year in year_cols:
            year_data = df[['country', year]].copy()
            year_data = year_data.rename(columns={year: 'value'})
            year_data['year'] = int(year)
            processed_data.append(year_data)
            
        # Combine all years
        result_df = pd.concat(processed_data, ignore_index=True)
        
        # Clean up the data
        result_df['value'] = pd.to_numeric(result_df['value'], errors='coerce')
        result_df = result_df.dropna(subset=['value'])
        
        return result_df
        
    except Exception as e:
        print(f"Error collecting {metric_name} data: {str(e)}")
        return None

def collect_and_store_education_data(pg_conn, mongo_db):
    """Collect and store education data with improved error handling"""
    metrics = {
        'education_investment': 'educ_investment',
        'student_teacher_ratio': 'student_teacher',
        'completion_rate': 'completion_rate'
    }
    
    for metric_name, table_name in metrics.items():
        try:
            print(f"Collecting {metric_name} data...")
            
            # Collect data
            df = collect_eurostat_data(metric_name)
            if df is None:
                print(f"Skipping {metric_name} due to data collection error")
                continue
            
            # Add metadata
            df['metric_type'] = metric_name
            df['processed_date'] = pd.Timestamp.now()
            
            # Convert to records
            records = df.to_dict('records')
            
            if not records:
                print(f"No records to store for {metric_name}")
                continue
            
            # Store in PostgreSQL
            try:
                with pg_conn.cursor() as cur:
                    values = [(
                        str(record['country']),
                        int(record['year']),
                        str(metric_name),
                        float(record['value'])
                    ) for record in records if all(k in record for k in ['country', 'year', 'value'])]
                    
                    if values:
                        execute_values(cur, f"""
                            INSERT INTO raw_education_data (country, year, metric_name, metric_value)
                            VALUES %s
                            ON CONFLICT (country, year, metric_name) 
                            DO UPDATE SET metric_value = EXCLUDED.metric_value;
                        """, values)
                        pg_conn.commit()
                        print(f"Stored {len(values)} records in PostgreSQL for {metric_name}")
            except Exception as e:
                print(f"Error storing data in PostgreSQL: {str(e)}")
                pg_conn.rollback()
            
            # Store in MongoDB
            try:
                collection = mongo_db[metric_name]
                for record in records:
                    if all(k in record for k in ['country', 'year', 'value']):
                        collection.update_one(
                            {
                                'country': str(record['country']),
                                'year': int(record['year'])
                            },
                            {
                                '$set': {
                                    'value': float(record['value']),
                                    'metric_type': metric_name,
                                    'updated_at': datetime.now()
                                }
                            },
                            upsert=True
                        )
                print(f"Successfully stored records in MongoDB for {metric_name}")
            except Exception as e:
                print(f"Error storing data in MongoDB: {str(e)}")
            
        except Exception as e:
            print(f"Error processing {metric_name}: {str(e)}")
            continue

# 7. Data Analysis Functions
def analyze_education_metrics(mongo_db, country=None, year_range=None):
    """Analyze education metrics with advanced analytics"""
    try:
        results = {}
        metrics = ['education_investment', 'student_teacher_ratio', 'completion_rate']
        
        for metric in metrics:
            collection = mongo_db[metric]
            
            # Build query
            query = {}
            if country:
                query['country'] = country
            if year_range:
                query['year'] = {'$gte': year_range[0], '$lte': year_range[1]}
            
            # Get data
            cursor = collection.find(query)
            df = pd.DataFrame(list(cursor))
            
            if not df.empty:
                # Basic statistics
                stats = {
                    'mean': df['value'].mean(),
                    'median': df['value'].median(),
                    'std': df['value'].std(),
                    'min': df['value'].min(),
                    'max': df['value'].max()
                }
                
                # Trend analysis
                if len(df) > 1:
                    df_sorted = df.sort_values('year')
                    trend = np.polyfit(df_sorted['year'], df_sorted['value'], 1)
                    stats['trend'] = {
                        'slope': trend[0],
                        'intercept': trend[1]
                    }
                
                # Year-over-year change
                if len(df) > 1:
                    df_sorted = df.sort_values('year')
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
        # Get historical data
        collection = mongo_db[metric]
        cursor = collection.find({'country': country})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return None
            
        # Prepare time series data
        df_sorted = df.sort_values('year')
        
        # Fit ARIMA model
        model = ARIMA(df_sorted['value'], order=(1,1,1))
        results = model.fit()
        
        # Generate forecasts
        forecast = results.forecast(steps=forecast_years)
        
        # Prepare forecast results
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
        
        # Build query
        query = {'country': {'$in': countries}}
        if year_range:
            query['year'] = {'$gte': year_range[0], '$lte': year_range[1]}
        
        # Get data
        cursor = collection.find(query)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return None
        
        # Calculate statistics for each country
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

def main():
    """Main function to run education data analysis"""
    try:
        # Connect to databases
        pg_conn = get_postgres_connection()
        mongo_db = get_mongodb_connection()
        
        if pg_conn is None or mongo_db is None:
            print("Failed to connect to databases")
            return
        
        try:
            # Test MongoDB connection
            mongo_db.command('ping')
        except Exception as e:
            print(f"MongoDB connection test failed: {str(e)}")
            return
            
        # Set up PostgreSQL database
        setup_postgres_database(pg_conn)
        
        # Collect and store data
        print("\nCollecting and storing education data...")
        collect_and_store_education_data(pg_conn, mongo_db)
        
        # Analyze metrics for EU countries
        print("\nAnalyzing education metrics...")
        eu_countries = ['DE', 'FR', 'IT', 'ES', 'NL']  # Example EU countries
        year_range = (2010, 2023)
        
        for country in eu_countries:
            print(f"\nAnalyzing data for {country}")
            
            # Get metrics analysis
            metrics = analyze_education_metrics(mongo_db, country, year_range)
            if metrics:
                print(f"\nMetrics Analysis for {country}:")
                for metric, stats in metrics.items():
                    print(f"\n{metric.upper()}:")
                    print(f"Mean: {stats['mean']:.2f}")
                    print(f"Median: {stats['median']:.2f}")
                    if 'trend' in stats:
                        print(f"Trend slope: {stats['trend']['slope']:.4f}")
            
            # Generate forecasts
            print(f"\nGenerating forecasts for {country}")
            for metric in ['education_investment', 'student_teacher_ratio', 'completion_rate']:
                forecast = generate_forecasts(mongo_db, metric, country)
                if forecast:
                    print(f"\n{metric.upper()} Forecast:")
                    for year, value in zip(forecast['years'], forecast['values']):
                        print(f"{year}: {value:.2f}")
        
        # Compare countries
        print("\nComparing countries...")
        for metric in ['education_investment', 'student_teacher_ratio', 'completion_rate']:
            comparison = compare_countries(mongo_db, eu_countries, metric, year_range)
            if comparison:
                print(f"\n{metric.upper()} Comparison:")
                for country, stats in comparison.items():
                    print(f"\n{country}:")
                    print(f"Mean: {stats['mean']:.2f}")
                    print(f"Latest Value: {stats['latest_value']:.2f}")
                    print(f"Trend: {stats['trend']:.4f}")
        
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        import traceback
        print(traceback.format_exc())
    
    finally:
        if pg_conn is not None:
            pg_conn.close()

if __name__ == "__main__":
    main()
