"""
Setup PostgreSQL database and tables
"""
from dotenv import load_dotenv
import os
import psycopg2

# Load environment variables
load_dotenv()

def setup_database():
    """Create database and tables"""
    try:
        # First connect to default database
        conn = psycopg2.connect(
            dbname='postgres',
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        conn.autocommit = True
        
        # Create database if it doesn't exist
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname='education_db'")
            if not cur.fetchone():
                cur.execute('CREATE DATABASE education_db')
                print("Created education_db database")
            else:
                print("Database education_db already exists")
        conn.close()
        
        # Connect to education_db
        conn = psycopg2.connect(
            dbname='education_db',
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        
        # Create tables
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
            print("Created tables")
        
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_database()
