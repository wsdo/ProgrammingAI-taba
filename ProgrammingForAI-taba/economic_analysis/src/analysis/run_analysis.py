"""
Main analysis script for economic data
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.economic_analyzer import EconomicAnalyzer
from data.db_manager import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize components
        db_manager = DatabaseManager()
        analyzer = EconomicAnalyzer(db_manager)
        
        # Configuration
        countries = ['BE', 'FR', 'DE', 'IT', 'NL', 'ES']
        start_date = '2019-01-01'
        end_date = '2023-12-31'
        
        logger.info("Starting economic analysis...")
        
        # Analyze economic indicators
        results = analyzer.analyze_economic_indicators(
            countries=countries,
            start_date=start_date,
            end_date=end_date
        )
        
        # Create visualizations
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "visualizations",
            "economic_analysis.html"
        )
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        analyzer.create_visualization(results, output_path)
        
        logger.info("Analysis completed successfully")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}")
        raise

if __name__ == "__main__":
    results = main()
