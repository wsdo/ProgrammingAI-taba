"""
Module for visualizing education data analysis results.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Tuple, Optional
import plotly.express as px
import plotly.graph_objects as go
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EducationVisualizer:
    """Class to handle education data visualization."""
    
    def __init__(self):
        """Initialize the visualizer with default parameters."""
        self.style = 'seaborn'
        self.figsize = (12, 6)
        plt.style.use(self.style)
    
    def plot_trend(self, df: pd.DataFrame, countries: List[str], 
                  title: str, save_path: Optional[str] = None) -> None:
        """
        Plot trends for selected countries.
        
        Args:
            df (pd.DataFrame): Input dataset
            countries (List[str]): List of countries to plot
            title (str): Plot title
            save_path (str, optional): Path to save the plot
        """
        try:
            plt.figure(figsize=self.figsize)
            
            for country in countries:
                country_data = df[df['geo'] == country].sort_values('time')
                plt.plot(country_data['time'], country_data['values'], 
                        marker='o', label=country)
            
            plt.title(title)
            plt.xlabel('Year')
            plt.ylabel('Value')
            plt.legend()
            plt.grid(True)
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"Saved trend plot to {save_path}")
            
            plt.close()
            
        except Exception as e:
            logger.error(f"Error plotting trend: {str(e)}")
    
    def plot_forecast(self, historical: List[float], forecast: List[float], 
                     conf_int: List[List[float]], title: str,
                     save_path: Optional[str] = None) -> None:
        """
        Plot historical data and forecasts.
        
        Args:
            historical (List[float]): Historical values
            forecast (List[float]): Forecasted values
            conf_int (List[List[float]]): Confidence intervals
            title (str): Plot title
            save_path (str, optional): Path to save the plot
        """
        try:
            fig = go.Figure()
            
            # Historical data
            fig.add_trace(go.Scatter(
                y=historical,
                name='Historical',
                mode='lines+markers'
            ))
            
            # Forecast
            fig.add_trace(go.Scatter(
                y=forecast,
                name='Forecast',
                mode='lines+markers',
                line=dict(dash='dash')
            ))
            
            # Confidence intervals
            if conf_int:
                lower = [ci[0] for ci in conf_int]
                upper = [ci[1] for ci in conf_int]
                
                fig.add_trace(go.Scatter(
                    y=lower + upper[::-1],
                    fill='toself',
                    fillcolor='rgba(0,100,80,0.2)',
                    line=dict(color='rgba(255,255,255,0)'),
                    name='Confidence Interval'
                ))
            
            fig.update_layout(
                title=title,
                xaxis_title='Time Period',
                yaxis_title='Value',
                showlegend=True
            )
            
            if save_path:
                fig.write_html(save_path)
                logger.info(f"Saved forecast plot to {save_path}")
            
        except Exception as e:
            logger.error(f"Error plotting forecast: {str(e)}")
    
    def plot_comparison(self, comparison_df: pd.DataFrame, 
                       metric: str, title: str,
                       save_path: Optional[str] = None) -> None:
        """
        Plot country comparisons.
        
        Args:
            comparison_df (pd.DataFrame): Comparison data
            metric (str): Metric to compare
            title (str): Plot title
            save_path (str, optional): Path to save the plot
        """
        try:
            plt.figure(figsize=self.figsize)
            
            sns.barplot(data=comparison_df, x='country', y=metric)
            plt.title(title)
            plt.xlabel('Country')
            plt.ylabel(metric)
            plt.xticks(rotation=45)
            
            if save_path:
                plt.savefig(save_path, bbox_inches='tight')
                logger.info(f"Saved comparison plot to {save_path}")
            
            plt.close()
            
        except Exception as e:
            logger.error(f"Error plotting comparison: {str(e)}")

def main():
    """Main function to demonstrate usage."""
    # Example data
    data = {
        'time': list(range(2010, 2021)) * 2,
        'geo': ['DE'] * 11 + ['FR'] * 11,
        'values': [x + np.random.rand() * 10 for x in range(22)]
    }
    df = pd.DataFrame(data)
    
    visualizer = EducationVisualizer()
    
    # Demonstrate trend plot
    visualizer.plot_trend(df, ['DE', 'FR'], 'Education Trends')
    
    # Demonstrate forecast plot
    historical = [50 + x for x in range(10)]
    forecast = [60 + x for x in range(5)]
    conf_int = [[x-5, x+5] for x in forecast]
    visualizer.plot_forecast(historical, forecast, conf_int, 'Education Forecast')
    
    # Demonstrate comparison plot
    comparison_data = {
        'country': ['DE', 'FR'],
        'latest_value': [65, 70],
        'average_value': [55, 60]
    }
    comparison_df = pd.DataFrame(comparison_data)
    visualizer.plot_comparison(comparison_df, 'latest_value', 'Country Comparison')

if __name__ == "__main__":
    main()
