"""
Main analysis script for economic indicators
"""

from analysis.economic_analyzer import EconomicAnalyzer
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_results(results: dict) -> str:
    """Format analysis results into readable text"""
    output = []
    for key, value in results.items():
        if isinstance(value, dict):
            output.append(f"\n{key.replace('_', ' ').title()}:")
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):
                    output.append(f"  {sub_key}: {sub_value['value']:.2f} (Date: {sub_value['date'].strftime('%Y-%m-%d')})")
                else:
                    output.append(f"  {sub_key}: {sub_value:.2f}")
        else:
            output.append(f"{key}: {value}")
    return "\n".join(output)

def main():
    """Run main analysis"""
    try:
        # Initialize analyzer
        analyzer = EconomicAnalyzer()
        
        # Load data
        logger.info("Loading economic data...")
        gdp_df, emp_df, inf_df = analyzer.load_data()
        
        # Create reports directory
        reports_dir = Path("../reports")
        reports_dir.mkdir(exist_ok=True)
        
        # 1. GDP Analysis
        logger.info("Analyzing GDP trends...")
        gdp_results = analyzer.analyze_gdp_trends(gdp_df)
        with open(reports_dir / "gdp_analysis.txt", "w") as f:
            f.write("GDP Analysis Results\n")
            f.write("===================\n\n")
            f.write(format_results(gdp_results))
        
        # Create GDP visualization
        analyzer.plot_gdp_trends(gdp_df)
        
        # 2. Employment Analysis
        logger.info("Analyzing employment trends...")
        emp_results = analyzer.analyze_employment(emp_df)
        with open(reports_dir / "employment_analysis.txt", "w") as f:
            f.write("Employment Analysis Results\n")
            f.write("=========================\n\n")
            f.write(format_results(emp_results))
        
        # Create employment visualization
        analyzer.plot_employment_trends(emp_df)
        
        # 3. Inflation Analysis
        logger.info("Analyzing inflation trends...")
        inf_results = analyzer.analyze_inflation(inf_df)
        with open(reports_dir / "inflation_analysis.txt", "w") as f:
            f.write("Inflation Analysis Results\n")
            f.write("========================\n\n")
            f.write(format_results(inf_results))
        
        # Create inflation visualization
        analyzer.plot_inflation_trends(inf_df)
        
        # 4. Correlation Analysis
        logger.info("Creating correlation matrix...")
        analyzer.create_correlation_matrix(gdp_df, emp_df, inf_df)
        
        # Calculate overall correlations
        correlations = pd.DataFrame()
        for country in gdp_df['country'].unique():
            country_data = pd.DataFrame()
            
            # Get data for each indicator
            gdp_data = gdp_df[gdp_df['country'] == country].set_index('date')['value']
            emp_data = emp_df[emp_df['country'] == country].set_index('date')['value']
            inf_data = inf_df[inf_df['country'] == country].set_index('date')['value']
            
            # Combine data
            country_data['GDP'] = gdp_data
            country_data['Employment'] = emp_data
            country_data['Inflation'] = inf_data
            
            # Calculate correlations
            corr = country_data.corr()
            
            # Save correlations
            with open(reports_dir / f"correlations_{country}.txt", "w") as f:
                f.write(f"Correlation Analysis for {country}\n")
                f.write("=============================\n\n")
                f.write(str(corr))
        
        logger.info("Analysis complete! Results saved in reports directory.")
        logger.info("Visualizations saved in visualizations directory.")
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()
