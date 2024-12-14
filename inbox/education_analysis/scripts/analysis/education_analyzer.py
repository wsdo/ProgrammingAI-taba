"""
Module for analyzing education data and generating insights.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from statsmodels.tsa.arima.model import ARIMA
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EducationAnalyzer:
    """Class to handle education data analysis."""
    
    def __init__(self):
        """Initialize the analyzer with default parameters."""
        self.forecast_periods = 5
    
    def analyze_trends(self, df: pd.DataFrame, country: str) -> Dict[str, float]:
        """
        Analyze trends for a specific country.
        
        Args:
            df (pd.DataFrame): Input dataset
            country (str): Country code to analyze
            
        Returns:
            Dict[str, float]: Dictionary of trend metrics
        """
        try:
            country_data = df[df['geo'] == country].sort_values('time')
            
            if country_data.empty:
                logger.warning(f"No data found for country: {country}")
                return {}
            
            # Calculate year-over-year changes
            yoy_changes = country_data['values'].pct_change()
            
            # Calculate trend metrics
            metrics = {
                'average_change': yoy_changes.mean(),
                'total_change': (country_data['values'].iloc[-1] / 
                               country_data['values'].iloc[0] - 1),
                'volatility': yoy_changes.std()
            }
            
            logger.info(f"Successfully analyzed trends for {country}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error analyzing trends for {country}: {str(e)}")
            return {}
    
    def generate_forecast(self, df: pd.DataFrame, country: str) -> Tuple[List[float], List[float]]:
        """
        Generate forecasts using ARIMA model.
        
        Args:
            df (pd.DataFrame): Input dataset
            country (str): Country code to forecast
            
        Returns:
            Tuple[List[float], List[float]]: Forecasted values and confidence intervals
        """
        try:
            country_data = df[df['geo'] == country].sort_values('time')
            
            if len(country_data) < 5:
                logger.warning(f"Insufficient data for forecasting {country}")
                return [], []
            
            # Fit ARIMA model
            model = ARIMA(country_data['values'], order=(1,1,1))
            results = model.fit()
            
            # Generate forecast
            forecast = results.forecast(steps=self.forecast_periods)
            conf_int = results.get_forecast(steps=self.forecast_periods).conf_int()
            
            logger.info(f"Successfully generated forecast for {country}")
            return forecast.tolist(), conf_int.values.tolist()
            
        except Exception as e:
            logger.error(f"Error generating forecast for {country}: {str(e)}")
            return [], []
    
    def compare_countries(self, df: pd.DataFrame, countries: List[str]) -> pd.DataFrame:
        """
        Compare education metrics across countries.
        
        Args:
            df (pd.DataFrame): Input dataset
            countries (List[str]): List of country codes to compare
            
        Returns:
            pd.DataFrame: Comparison results
        """
        try:
            comparison_data = []
            
            for country in countries:
                country_data = df[df['geo'] == country]
                
                if not country_data.empty:
                    metrics = {
                        'country': country,
                        'latest_value': country_data['values'].iloc[-1],
                        'average_value': country_data['values'].mean(),
                        'growth_rate': (country_data['values'].iloc[-1] / 
                                      country_data['values'].iloc[0] - 1)
                    }
                    comparison_data.append(metrics)
            
            comparison_df = pd.DataFrame(comparison_data)
            logger.info("Successfully compared countries")
            return comparison_df
            
        except Exception as e:
            logger.error(f"Error comparing countries: {str(e)}")
            return pd.DataFrame()

def main():
    """Main function to demonstrate usage."""
    # Example dataset
    data = {
        'time': list(range(2010, 2021)) * 2,
        'geo': ['DE'] * 11 + ['FR'] * 11,
        'values': np.random.rand(22) * 100
    }
    df = pd.DataFrame(data)
    
    analyzer = EducationAnalyzer()
    
    # Demonstrate trend analysis
    trends = analyzer.analyze_trends(df, 'DE')
    print("\nTrend Analysis for Germany:")
    for metric, value in trends.items():
        print(f"{metric}: {value:.2f}")
    
    # Demonstrate forecasting
    forecast, conf_int = analyzer.generate_forecast(df, 'DE')
    print("\nForecast for Germany:")
    print(f"Next {len(forecast)} periods: {[f'{x:.2f}' for x in forecast]}")
    
    # Demonstrate country comparison
    comparison = analyzer.compare_countries(df, ['DE', 'FR'])
    print("\nCountry Comparison:")
    print(comparison)

if __name__ == "__main__":
    main()
