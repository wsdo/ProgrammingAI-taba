-- Create test user and database
CREATE USER test_user WITH PASSWORD 'test_password';
CREATE DATABASE test_education_db;
GRANT ALL PRIVILEGES ON DATABASE test_education_db TO test_user;

-- Connect to test database
\c test_education_db;

-- Create schema and grant permissions
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO test_user;

-- Create tables
CREATE TABLE IF NOT EXISTS education_data (
    id SERIAL PRIMARY KEY,
    geo_time_period VARCHAR(10),
    year INTEGER,
    value FLOAT,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS economic_data (
    id SERIAL PRIMARY KEY,
    country VARCHAR(10),
    year INTEGER,
    gdp_growth FLOAT,
    employment_rate FLOAT,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO test_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO test_user;
