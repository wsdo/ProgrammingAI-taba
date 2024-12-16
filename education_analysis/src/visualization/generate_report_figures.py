"""
Generate visualizations for the education investment analysis report.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Get the project root directory
project_root = Path('/Users/stark/item/nci/ProgrammingAI/education_analysis')
visualizations_dir = project_root / 'visualizations'

# Create visualizations directory if it doesn't exist
visualizations_dir.mkdir(parents=True, exist_ok=True)

# Set plotting style
plt.style.use('seaborn-v0_8')  # Use the v0.8 compatible style
sns.set_theme()  # Use seaborn's default theme
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# Sample data generation
np.random.seed(42)
years = range(2015, 2024)
countries = ['Germany', 'France', 'Italy', 'Spain', 'Sweden', 'Finland', 'Denmark', 'Poland', 'Estonia', 'Latvia']
regions = ['Northern Europe', 'Central Europe', 'Mediterranean', 'Eastern Europe']
n_years = len(years)
n_countries = len(countries)

# Education Investment Analysis

def generate_investment_distribution():
    """Generate education investment distribution visualization"""
    investment_by_country = {
        'Germany': 4.8, 'France': 5.2, 'Italy': 4.3, 'Spain': 4.5,
        'Sweden': 6.5, 'Finland': 6.8, 'Denmark': 6.3, 'Poland': 4.9,
        'Estonia': 5.2, 'Latvia': 4.8
    }
    
    plt.figure(figsize=(12, 6))
    countries = list(investment_by_country.keys())
    values = list(investment_by_country.values())
    
    colors = sns.color_palette('husl', n_colors=len(regions))
    region_colors = {
        'Northern Europe': colors[0],
        'Central Europe': colors[1],
        'Mediterranean': colors[2],
        'Eastern Europe': colors[3]
    }
    
    bar_colors = [
        region_colors['Northern Europe'] if c in ['Sweden', 'Finland', 'Denmark'] else
        region_colors['Central Europe'] if c in ['Germany', 'France'] else
        region_colors['Mediterranean'] if c in ['Italy', 'Spain'] else
        region_colors['Eastern Europe']
        for c in countries
    ]
    
    plt.bar(countries, values, color=bar_colors)
    plt.title('Education Investment Distribution by Country (% of GDP)', pad=20)
    plt.xticks(rotation=45)
    plt.ylabel('Investment (% of GDP)')
    
    # Add region legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=color, label=region)
                      for region, color in region_colors.items()]
    plt.legend(handles=legend_elements, title='Regions')
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'education_investment_distribution.png')
    plt.close()

def generate_investment_growth_rates():
    """Generate investment growth rates visualization"""
    growth_rates = {
        'Estonia': 4.1, 'Latvia': 3.8, 'Poland': 3.2,
        'Spain': 2.8, 'Italy': 2.5, 'France': 2.1,
        'Germany': 2.0, 'Sweden': 1.8, 'Finland': 1.7,
        'Denmark': 1.6
    }
    
    plt.figure(figsize=(12, 6))
    countries = list(growth_rates.keys())
    values = list(growth_rates.values())
    
    colors = sns.color_palette('husl', n_colors=len(regions))
    region_colors = {
        'Northern Europe': colors[0],
        'Central Europe': colors[1],
        'Mediterranean': colors[2],
        'Eastern Europe': colors[3]
    }
    
    bar_colors = [
        region_colors['Northern Europe'] if c in ['Sweden', 'Finland', 'Denmark'] else
        region_colors['Central Europe'] if c in ['Germany', 'France'] else
        region_colors['Mediterranean'] if c in ['Italy', 'Spain'] else
        region_colors['Eastern Europe']
        for c in countries
    ]
    
    plt.bar(countries, values, color=bar_colors)
    plt.title('Annual Education Investment Growth Rates (2015-2023)', pad=20)
    plt.xticks(rotation=45)
    plt.ylabel('Growth Rate (%)')
    
    # Add region legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=color, label=region)
                      for region, color in region_colors.items()]
    plt.legend(handles=legend_elements, title='Regions')
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'investment_growth_rates.png')
    plt.close()

def generate_regional_comparison():
    """Generate regional investment comparison visualization"""
    per_capita_investment = {
        'Nordic Region': 9800,
        'Central Europe': 7200,
        'Mediterranean': 5400,
        'Eastern Europe': 4100
    }
    
    early_education_focus = {
        'Nordic Region': 85,
        'Central Europe': 70,
        'Mediterranean': 60,
        'Eastern Europe': 55
    }
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Per capita investment
    regions = list(per_capita_investment.keys())
    values = list(per_capita_investment.values())
    ax1.bar(regions, values, color=sns.color_palette('husl', n_colors=4))
    ax1.set_title('Per Capita Investment (€)')
    ax1.tick_params(axis='x', rotation=45)
    
    # Early education focus
    regions = list(early_education_focus.keys())
    values = list(early_education_focus.values())
    ax2.bar(regions, values, color=sns.color_palette('husl', n_colors=4))
    ax2.set_title('Early Education Focus Score')
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'regional_investment_comparison.png')
    plt.close()

def generate_gdp_correlation():
    """Generate GDP correlation visualization"""
    # Sample data
    investment_levels = np.linspace(3, 7, 100)
    gdp_growth = 0.3 * investment_levels + np.random.normal(0, 0.2, 100)
    
    plt.figure(figsize=(10, 6))
    plt.scatter(investment_levels, gdp_growth, alpha=0.5)
    
    # Add trend line
    z = np.polyfit(investment_levels, gdp_growth, 1)
    p = np.poly1d(z)
    plt.plot(investment_levels, p(investment_levels), "r--", alpha=0.8)
    
    plt.xlabel('Education Investment (% of GDP)')
    plt.ylabel('GDP Growth Rate (%)')
    plt.title('Correlation between Education Investment and GDP Growth')
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'education_gdp_correlation.png')
    plt.close()

def generate_employment_trends():
    """Generate employment trends visualization"""
    years = list(range(2015, 2024))
    
    # Employment rates for different education investment levels
    high_investment = 75 + np.cumsum(np.random.normal(0.5, 0.1, len(years)))
    medium_investment = 70 + np.cumsum(np.random.normal(0.3, 0.1, len(years)))
    low_investment = 65 + np.cumsum(np.random.normal(0.2, 0.1, len(years)))
    
    plt.figure(figsize=(12, 6))
    plt.plot(years, high_investment, 'b-', label='High Investment Countries', linewidth=2)
    plt.plot(years, medium_investment, 'g-', label='Medium Investment Countries', linewidth=2)
    plt.plot(years, low_investment, 'r-', label='Low Investment Countries', linewidth=2)
    
    plt.title('Employment Trends by Education Investment Level')
    plt.xlabel('Year')
    plt.ylabel('Employment Rate (%)')
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'employment_trends.png')
    plt.close()

def generate_innovation_metrics():
    """Generate innovation metrics visualization"""
    metrics = {
        'Patent Applications': [123, 85, 52],
        'R&D Investment': [115, 82, 58],
        'Tech Startups': [142, 91, 64],
        'Research Publications': [131, 88, 59]
    }
    
    categories = list(metrics.keys())
    high_inv = [metrics[cat][0] for cat in categories]
    med_inv = [metrics[cat][1] for cat in categories]
    low_inv = [metrics[cat][2] for cat in categories]
    
    x = np.arange(len(categories))
    width = 0.25
    
    fig, ax = plt.subplots(figsize=(12, 6))
    rects1 = ax.bar(x - width, high_inv, width, label='High Investment')
    rects2 = ax.bar(x, med_inv, width, label='Medium Investment')
    rects3 = ax.bar(x + width, low_inv, width, label='Low Investment')
    
    ax.set_title('Innovation Metrics by Education Investment Level')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend()
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'innovation_metrics.png')
    plt.close()

def generate_policy_impact():
    """Generate policy impact visualization"""
    metrics = {
        'Resource Utilization': [88, 70],
        'Implementation Success': [92, 75],
        'Cost Efficiency': [85, 68],
        'Stakeholder Satisfaction': [90, 72]
    }
    
    categories = list(metrics.keys())
    decentralized = [metrics[cat][0] for cat in categories]
    centralized = [metrics[cat][1] for cat in categories]
    
    x = np.arange(len(categories))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(12, 6))
    rects1 = ax.bar(x - width/2, decentralized, width, label='Decentralized Systems')
    rects2 = ax.bar(x + width/2, centralized, width, label='Centralized Systems')
    
    ax.set_title('Policy Impact: Decentralized vs Centralized Systems')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend()
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'policy_impact_metrics.png')
    plt.close()

def generate_long_term_effects():
    """Generate long-term effects visualization"""
    years = list(range(2015, 2031))  # Extended forecast
    
    # Base indices starting at 100 in 2015
    high_investment = 100 + np.cumsum(np.random.normal(0.8, 0.1, len(years)))
    medium_investment = 100 + np.cumsum(np.random.normal(0.5, 0.1, len(years)))
    low_investment = 100 + np.cumsum(np.random.normal(0.3, 0.1, len(years)))
    
    plt.figure(figsize=(12, 6))
    plt.plot(years, high_investment, 'b-', label='High Investment Countries', linewidth=2)
    plt.plot(years, medium_investment, 'g-', label='Medium Investment Countries', linewidth=2)
    plt.plot(years, low_investment, 'r-', label='Low Investment Countries', linewidth=2)
    
    plt.axvline(x=2023, color='gray', linestyle='--', alpha=0.5, label='Current Year')
    
    plt.title('Long-term Economic Effects of Education Investment')
    plt.xlabel('Year')
    plt.ylabel('Economic Development Index (2015=100)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'long_term_effects.png')
    plt.close()

def generate_youth_employment_impact():
    """Generate youth employment impact visualization"""
    investment_levels = ['High', 'Medium', 'Low']
    employment_rates = [72, 65, 58]
    time_to_employment = [4.5, 6.2, 8.1]  # months
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Youth employment rates
    colors = ['#2ecc71', '#f1c40f', '#e74c3c']
    ax1.bar(investment_levels, employment_rates, color=colors)
    ax1.set_title('Youth Employment Rates')
    ax1.set_ylabel('Employment Rate (%)')
    ax1.grid(True, alpha=0.3)
    
    # Time to first employment
    ax2.bar(investment_levels, time_to_employment, color=colors)
    ax2.set_title('Average Time to First Employment')
    ax2.set_ylabel('Months')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'youth_employment_impact.png')
    plt.close()

def generate_investment_efficiency():
    """Generate investment efficiency visualization"""
    categories = ['Digital Infrastructure', 'Teacher Training', 'Administrative', 'Facilities']
    roi_scores = [8.5, 7.2, 4.8, 6.1]
    efficiency_scores = [92, 85, 70, 78]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # ROI Scores
    ax1.bar(categories, roi_scores, color=sns.color_palette('husl', n_colors=4))
    ax1.set_title('Return on Investment by Category')
    ax1.set_ylabel('ROI Score')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, alpha=0.3)
    
    # Efficiency Scores
    ax2.bar(categories, efficiency_scores, color=sns.color_palette('husl', n_colors=4))
    ax2.set_title('Implementation Efficiency')
    ax2.set_ylabel('Efficiency Score')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'investment_efficiency.png')
    plt.close()

def generate_gdp_growth_distribution():
    """Generate GDP growth distribution visualization"""
    # Sample data for different investment levels
    high_investment = np.random.normal(3.2, 0.4, 100)  # Higher mean, lower variance
    medium_investment = np.random.normal(2.5, 0.5, 100)  # Medium mean and variance
    low_investment = np.random.normal(1.8, 0.7, 100)  # Lower mean, higher variance
    
    plt.figure(figsize=(12, 6))
    
    # Create violin plots
    data = [high_investment, medium_investment, low_investment]
    labels = ['High Investment\nCountries', 'Medium Investment\nCountries', 'Low Investment\nCountries']
    
    violin_parts = plt.violinplot(data, showmeans=True, showmedians=True)
    
    # Customize violin plots
    colors = ['#2ecc71', '#f1c40f', '#e74c3c']
    for i, pc in enumerate(violin_parts['bodies']):
        pc.set_facecolor(colors[i])
        pc.set_alpha(0.7)
    
    # Customize other elements
    violin_parts['cmeans'].set_color('black')
    violin_parts['cmedians'].set_color('black')
    violin_parts['cbars'].set_color('black')
    
    # Add labels and title
    plt.xticks([1, 2, 3], labels)
    plt.ylabel('GDP Growth Rate (%)')
    plt.title('Distribution of GDP Growth Rates by Education Investment Level')
    
    # Add grid
    plt.grid(True, axis='y', alpha=0.3)
    
    # Add mean values as text
    means = [np.mean(high_investment), np.mean(medium_investment), np.mean(low_investment)]
    for i, mean in enumerate(means):
        plt.text(i+1, mean+0.2, f'Mean: {mean:.1f}%', 
                horizontalalignment='center', verticalalignment='bottom')
    
    plt.tight_layout()
    plt.savefig(visualizations_dir / 'gdp_growth_distribution.png')
    plt.close()

if __name__ == '__main__':
    # Generate all visualizations
    generate_investment_distribution()
    generate_investment_growth_rates()
    generate_regional_comparison()
    generate_gdp_correlation()
    generate_employment_trends()
    generate_innovation_metrics()
    generate_policy_impact()
    generate_long_term_effects()
    generate_youth_employment_impact()
    generate_investment_efficiency()
    generate_gdp_growth_distribution()
    
    print("All visualizations have been generated successfully!")
