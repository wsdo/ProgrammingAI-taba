import sys
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / '.env')

# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner

# Initialize database manager and data cleaner
db_manager = DatabaseManager()
cleaner = DataCleaner()

# Get education data from database
education_data = db_manager.get_education_data()

# Clean the data
education_data_cleaned = cleaner.clean_education_data(education_data)

# Print unique country codes
print("\nAvailable country codes:")
print(sorted(education_data_cleaned['geo_time_period'].unique()))

# Select major countries for trend comparison (using correct country codes)
major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']  # Updated from DEU, FRA, etc.
major_country_data = education_data_cleaned[education_data_cleaned['geo_time_period'].isin(major_countries)]

# Create a mapping for country names in the plot
country_names = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'PL': 'Poland'
}

# Check if data exists
if major_country_data.empty:
    print("No data found for major countries")
else:
    try:
        # Set plotting style
        plt.style.use('seaborn')
        sns.set_palette('husl')
        
        # Create multi-line plot
        plt.figure(figsize=(15, 8))
        
        # Create color mapping for each country
        colors = {'DE': 'blue', 'FR': 'red', 'IT': 'green', 'ES': 'orange', 'PL': 'purple'}
        
        for country in major_countries:
            country_data = major_country_data[major_country_data['geo_time_period'] == country]
            if not country_data.empty:
                # Ensure data is sorted by year
                country_data = country_data.sort_values('year')
                plt.plot(country_data['year'], 
                        country_data['value'], 
                        label=country_names[country],
                        color=colors[country],
                        marker='o')
        
        plt.title('Education Investment Trends in Major EU Countries')
        plt.xlabel('Year')
        plt.ylabel('Investment Value')
        plt.legend()
        
        # Add grid and adjust border
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        
        # Adjust layout to prevent label clipping
        plt.tight_layout()
        
        plt.show()
        
        print("\nCompound Annual Growth Rate (CAGR) by Country:")
        print("-" * 40)
        
        # Calculate CAGR for each country
        for country in major_countries:
            country_data = major_country_data[major_country_data['geo_time_period'] == country]
            if len(country_data) >= 2:  # Ensure at least two data points exist
                # Sort data by year
                country_data = country_data.sort_values('year')
                start_value = country_data.iloc[0]['value']
                end_value = country_data.iloc[-1]['value']
                years = country_data.iloc[-1]['year'] - country_data.iloc[0]['year']  # Use actual year difference
                
                if years > 0 and start_value > 0:  # Avoid division by zero and negative numbers
                    cagr = (end_value/start_value)**(1/years) - 1
                    start_year = country_data.iloc[0]['year']
                    end_year = country_data.iloc[-1]['year']
                    print(f"{country_names[country]} ({country}): {cagr*100:.2f}% ({start_year}-{end_year})")
            else:
                print(f"{country_names[country]} ({country}): Not enough data points")
                
    except Exception as e:
        print(f"Error during analysis: {str(e)}")

# Close database connections
db_manager.close_connections()
