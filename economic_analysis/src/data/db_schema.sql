-- Create schema for economic data
CREATE SCHEMA IF NOT EXISTS economic_data;

-- Countries table
CREATE TABLE IF NOT EXISTS economic_data.countries (
    country_code CHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Economic indicators metadata
CREATE TABLE IF NOT EXISTS economic_data.indicators (
    indicator_code VARCHAR(50) PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    unit VARCHAR(50),
    description TEXT,
    source VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- GDP data
CREATE TABLE IF NOT EXISTS economic_data.gdp (
    id SERIAL PRIMARY KEY,
    country_code CHAR(2) REFERENCES economic_data.countries(country_code),
    date DATE NOT NULL,
    value DECIMAL(15,2) NOT NULL,
    quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM date)) STORED,
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date)) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, date)
);

-- Employment data
CREATE TABLE IF NOT EXISTS economic_data.employment (
    id SERIAL PRIMARY KEY,
    country_code CHAR(2) REFERENCES economic_data.countries(country_code),
    date DATE NOT NULL,
    value DECIMAL(5,2) NOT NULL,
    quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM date)) STORED,
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date)) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, date)
);

-- Inflation data
CREATE TABLE IF NOT EXISTS economic_data.inflation (
    id SERIAL PRIMARY KEY,
    country_code CHAR(2) REFERENCES economic_data.countries(country_code),
    date DATE NOT NULL,
    value DECIMAL(5,2) NOT NULL,
    quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM date)) STORED,
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM date)) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, date)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_gdp_date ON economic_data.gdp(date);
CREATE INDEX IF NOT EXISTS idx_gdp_country_date ON economic_data.gdp(country_code, date);
CREATE INDEX IF NOT EXISTS idx_employment_date ON economic_data.employment(date);
CREATE INDEX IF NOT EXISTS idx_employment_country_date ON economic_data.employment(country_code, date);
CREATE INDEX IF NOT EXISTS idx_inflation_date ON economic_data.inflation(date);
CREATE INDEX IF NOT EXISTS idx_inflation_country_date ON economic_data.inflation(country_code, date);

-- Insert indicator metadata
INSERT INTO economic_data.indicators (indicator_code, indicator_name, unit, description, source) 
VALUES 
    ('GDP', 'Gross Domestic Product', 'Million EUR', 'Quarterly GDP at current prices, seasonally adjusted', 'IMF'),
    ('EMP', 'Employment Rate', 'Percentage', 'Percentage of working age population employed', 'IMF'),
    ('INF', 'Inflation Rate', 'Percentage', 'Consumer price inflation rate', 'IMF')
ON CONFLICT (indicator_code) DO NOTHING;

-- Insert country data
INSERT INTO economic_data.countries (country_code, country_name, region)
VALUES 
    ('BE', 'Belgium', 'Western Europe'),
    ('FR', 'France', 'Western Europe'),
    ('DE', 'Germany', 'Western Europe'),
    ('IT', 'Italy', 'Southern Europe'),
    ('NL', 'Netherlands', 'Western Europe'),
    ('ES', 'Spain', 'Southern Europe')
ON CONFLICT (country_code) DO NOTHING;
