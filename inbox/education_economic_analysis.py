# Cell 1: Import required libraries and setup environment
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import project modules
from src.data_processing.db_manager import DatabaseManager

# Set plotting style
plt.style.use('seaborn')
sns.set_palette('husl')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# Cell 2: Load data from database
db_manager = DatabaseManager()

# Get education and economic data
education_data = db_manager.get_education_data()
economic_data = db_manager.get_economic_data()

# Print data info
print("Education data shape:", education_data.shape)
print("Economic data shape:", economic_data.shape)

# Cell 3: Data preprocessing
# Convert country codes
country_code_map = {
    'DEU': 'DE',
    'FRA': 'FR',
    'ITA': 'IT',
    'ESP': 'ES',
    'POL': 'PL'
}

economic_data['country'] = economic_data['country'].map(country_code_map)

# Merge education and economic data
merged_data = pd.merge(
    education_data,
    economic_data,
    left_on=['geo_time_period', 'year'],
    right_on=['country', 'year'],
    how='inner'
)

print("\nMerged data shape:", merged_data.shape)

# Cell 4: Create correlation heatmap
plt.figure(figsize=(10, 8))
correlation_vars = ['value', 'gdp_growth', 'employment_rate']
correlation_matrix = merged_data[correlation_vars].corr()

sns.heatmap(
    correlation_matrix,
    annot=True,
    cmap='coolwarm',
    vmin=-1,
    vmax=1,
    center=0
)

plt.title('Correlation between Education Investment and Economic Indicators')
plt.tight_layout()
plt.show()

# Cell 5: Create trend analysis plots
countries = ['DE', 'FR', 'IT', 'ES', 'PL']
country_names = {
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'PL': 'Poland'
}

# Create subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

# Plot education investment vs GDP growth
for country in countries:
    country_data = merged_data[merged_data['country'] == country].sort_values('year')
    
    # Plot on first subplot
    ax1.plot(country_data['year'], 
             country_data['gdp_growth'], 
             marker='o',
             label=f'{country_names[country]} - GDP Growth')
    ax1.plot(country_data['year'],
             country_data['value'],
             marker='s',
             linestyle='--',
             label=f'{country_names[country]} - Education Investment')

ax1.set_title('Education Investment vs GDP Growth')
ax1.set_xlabel('Year')
ax1.set_ylabel('Percentage')
ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax1.grid(True, linestyle='--', alpha=0.7)

# Plot education investment vs employment rate
for country in countries:
    country_data = merged_data[merged_data['country'] == country].sort_values('year')
    
    # Plot on second subplot
    ax2.plot(country_data['year'],
             country_data['employment_rate'],
             marker='o',
             label=f'{country_names[country]} - Employment Rate')
    ax2.plot(country_data['year'],
             country_data['value'],
             marker='s',
             linestyle='--',
             label=f'{country_names[country]} - Education Investment')

ax2.set_title('Education Investment vs Employment Rate')
ax2.set_xlabel('Year')
ax2.set_ylabel('Percentage')
ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax2.grid(True, linestyle='--', alpha=0.7)

plt.tight_layout()
plt.show()

# Cell 6: Statistical Analysis
print("\nStatistical Analysis by Country:")
print("-" * 50)

for country in countries:
    country_data = merged_data[merged_data['country'] == country]
    print(f"\nCountry: {country_names[country]}")
    print(f"Average Education Investment: {country_data['value'].mean():.2f}%")
    print(f"Average GDP Growth: {country_data['gdp_growth'].mean():.2f}%")
    print(f"Average Employment Rate: {country_data['employment_rate'].mean():.2f}%")
    
    # Calculate correlations
    edu_gdp_corr = country_data['value'].corr(country_data['gdp_growth'])
    edu_emp_corr = country_data['value'].corr(country_data['employment_rate'])
    
    print(f"Correlation (Education-GDP): {edu_gdp_corr:.2f}")
    print(f"Correlation (Education-Employment): {edu_emp_corr:.2f}")

# Close database connections
db_manager.close_connections()
