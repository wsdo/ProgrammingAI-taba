# Cell 1: Import required libraries and setup environment
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import matplotlib

# Add project root to Python path
project_root = Path().absolute().parent
sys.path.append(str(project_root))

# Import project modules
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner
from src.data_processing.imf_data_processor import IMFDataProcessor

# Set plotting style
plt.style.use('seaborn')
sns.set_palette('husl')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# Cell 2: Initialize data processors and load data
db_manager = DatabaseManager()
cleaner = DataCleaner()
imf_processor = IMFDataProcessor()

# Get education data
education_data = db_manager.get_education_data()
education_data_cleaned = cleaner.clean_education_data(education_data)

# Get economic data
countries = ['DE', 'FR', 'IT', 'ES', 'PL']  # Major EU countries
start_year = 2010
end_year = 2023
economic_data = imf_processor.get_economic_indicators(countries, start_year, end_year)

# Cell 3: Prepare data for analysis
# Merge education and economic data
merged_data = pd.merge(
    education_data_cleaned,
    economic_data,
    left_on=['geo_time_period', 'year'],
    right_on=['country', 'year'],
    how='inner'
)

# Cell 4: Create correlation heatmap
plt.figure(figsize=(10, 8))
correlation_matrix = merged_data[['value', 'gdp_growth', 'employment_rate']].corr()
sns.heatmap(correlation_matrix, 
            annot=True, 
            cmap='coolwarm', 
            vmin=-1, 
            vmax=1, 
            center=0)
plt.title('Correlation between Education Investment and Economic Indicators')
plt.tight_layout()
plt.show()

# Cell 5: Create trend line plots
plt.figure(figsize=(15, 10))

# Create two subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

# Plot education investment vs GDP growth
for country in countries:
    country_data = merged_data[merged_data['country'] == country]
    ax1.plot(country_data['year'], 
             country_data['gdp_growth'], 
             marker='o', 
             label=f'{country} - GDP Growth')
    ax1.plot(country_data['year'], 
             country_data['value'], 
             marker='s', 
             linestyle='--', 
             label=f'{country} - Education Investment')

ax1.set_title('Education Investment vs GDP Growth')
ax1.set_xlabel('Year')
ax1.set_ylabel('Percentage')
ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax1.grid(True, linestyle='--', alpha=0.7)

# Plot education investment vs employment rate
for country in countries:
    country_data = merged_data[merged_data['country'] == country]
    ax2.plot(country_data['year'], 
             country_data['employment_rate'], 
             marker='o', 
             label=f'{country} - Employment Rate')
    ax2.plot(country_data['year'], 
             country_data['value'], 
             marker='s', 
             linestyle='--', 
             label=f'{country} - Education Investment')

ax2.set_title('Education Investment vs Employment Rate')
ax2.set_xlabel('Year')
ax2.set_ylabel('Percentage')
ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax2.grid(True, linestyle='--', alpha=0.7)

plt.tight_layout()
plt.show()

# Cell 6: Calculate and display statistics
print("\nSummary Statistics by Country:")
print("-" * 50)

for country in countries:
    country_data = merged_data[merged_data['country'] == country]
    print(f"\nCountry: {country}")
    print(f"Average Education Investment: {country_data['value'].mean():.2f}%")
    print(f"Average GDP Growth: {country_data['gdp_growth'].mean():.2f}%")
    print(f"Average Employment Rate: {country_data['employment_rate'].mean():.2f}%")
    print(f"Correlation (Education-GDP): {country_data['value'].corr(country_data['gdp_growth']):.2f}")
    print(f"Correlation (Education-Employment): {country_data['value'].corr(country_data['employment_rate']):.2f}")
