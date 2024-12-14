"""
Main script to run the education data analysis.
"""

import os
import sys

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from scripts.data_processing.db_manager import DatabaseManager
from scripts.data_collection.eurostat_collector import EurostatCollector
from scripts.data_processing.data_processor import EducationDataProcessor
from scripts.analysis.education_analyzer import EducationAnalyzer
from scripts.visualization.data_visualizer import EducationVisualizer

def main():
    """Main function to run the analysis."""
    print("Starting education data analysis...")
    
    # Initialize components
    db_manager = DatabaseManager()
    collector = EurostatCollector()
    processor = EducationDataProcessor()
    analyzer = EducationAnalyzer()
    visualizer = EducationVisualizer()
    
    # Test database connections
    print("\nTesting database connections...")
    pg_success = db_manager.connect_postgres()
    mongo_success = db_manager.connect_mongodb()
    
    print(f"PostgreSQL connection: {'Success' if pg_success else 'Failed'}")
    print(f"MongoDB connection: {'Success' if mongo_success else 'Failed'}")
    
    if not (pg_success and mongo_success):
        print("Failed to connect to databases. Exiting...")
        return
    
    # Setup database tables
    print("\nSetting up database tables...")
    db_manager.setup_postgres_tables()
    
    # Collect data
    print("\nCollecting education data...")
    collected_data = collector.collect_all_indicators(start_year=2010)
    
    if not collected_data:
        print("No data collected. Exiting...")
        return
    
    # Process data
    print("\nProcessing collected data...")
    processed_data = processor.process_indicators(collected_data)
    
    # Analyze data
    print("\nAnalyzing data...")
    countries = ['DE', 'FR', 'ES', 'IT']
    
    for code, df in processed_data.items():
        print(f"\nAnalysis for {collector.base_indicators[code]}:")
        
        # Generate and store statistics
        stats = processor.calculate_statistics(df, code)
        print("\nStatistics:")
        for stat, value in stats.items():
            if isinstance(value, (int, float)):
                print(f"{stat}: {value:.2f}")
        
        # Analyze trends
        for country in countries:
            trends = analyzer.analyze_trends(df, country)
            print(f"\nTrends for {country}:")
            for metric, value in trends.items():
                print(f"{metric}: {value:.2f}")
    
    # Create visualizations
    print("\nGenerating visualizations...")
    os.makedirs("plots", exist_ok=True)
    
    for code, df in processed_data.items():
        indicator_name = collector.base_indicators[code]
        
        # Plot trends
        visualizer.plot_trend(df, countries, 
                            f"Education {indicator_name} Trends",
                            f"plots/{code}_trends.png")
        
        # Plot forecasts
        for country in countries:
            forecast, conf_int = analyzer.generate_forecast(df, country)
            if forecast:
                country_data = df[df['geo'] == country]['values'].tolist()
                visualizer.plot_forecast(
                    country_data, forecast, conf_int,
                    f"{indicator_name} Forecast for {country}",
                    f"plots/{code}_{country}_forecast.html"
                )
        
        # Plot comparisons
        comparison = analyzer.compare_countries(df, countries)
        visualizer.plot_comparison(
            comparison, 'latest_value',
            f"{indicator_name} by Country",
            f"plots/{code}_comparison.png"
        )
    
    print("\nAnalysis complete! Check the 'plots' directory for visualizations.")

if __name__ == "__main__":
    main()
