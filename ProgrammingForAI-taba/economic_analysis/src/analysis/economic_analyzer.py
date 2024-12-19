"""
Economic Data Analysis Module
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import logging

from data.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class EconomicAnalyzer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def analyze_economic_indicators(self, countries: List[str], 
                                  start_date: str, end_date: str) -> Dict:
        """
        Analyze economic indicators for specified countries
        
        Args:
            countries: List of country codes
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Load data from PostgreSQL
            gdp_data = self.db_manager.load_from_postgres(
                'economic_data.gdp',
                {'country_code': countries, 'date': [start_date, end_date]}
            )
            emp_data = self.db_manager.load_from_postgres(
                'economic_data.employment',
                {'country_code': countries, 'date': [start_date, end_date]}
            )
            inf_data = self.db_manager.load_from_postgres(
                'economic_data.inflation',
                {'country_code': countries, 'date': [start_date, end_date]}
            )
            
            # Calculate quarterly statistics
            results = {}
            for country in countries:
                country_results = {
                    'gdp_growth': self._calculate_growth_rates(
                        gdp_data[gdp_data['country_code'] == country]
                    ),
                    'employment_change': self._calculate_changes(
                        emp_data[emp_data['country_code'] == country]
                    ),
                    'inflation_change': self._calculate_changes(
                        inf_data[inf_data['country_code'] == country]
                    )
                }
                
                # Calculate correlations
                correlations = self._calculate_correlations(
                    gdp_data[gdp_data['country_code'] == country],
                    emp_data[emp_data['country_code'] == country],
                    inf_data[inf_data['country_code'] == country]
                )
                country_results['correlations'] = correlations
                
                results[country] = country_results
                
                # Save analysis results to MongoDB
                for date in pd.date_range(start_date, end_date, freq='Q'):
                    quarter_results = {
                        'gdp_growth': country_results['gdp_growth'].get(
                            date.strftime('%Y-Q%q'), None
                        ),
                        'employment_change': country_results['employment_change'].get(
                            date.strftime('%Y-Q%q'), None
                        ),
                        'inflation_change': country_results['inflation_change'].get(
                            date.strftime('%Y-Q%q'), None
                        ),
                        'correlations': correlations
                    }
                    
                    self.db_manager.save_analysis_results({country: quarter_results})
            
            # Save raw data to MongoDB for future reference
            self.db_manager.save_to_mongodb(
                gdp_df=gdp_data,
                emp_df=emp_data,
                inf_df=inf_data
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error analyzing economic indicators: {str(e)}")
            raise
            
    def _calculate_growth_rates(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate quarter-over-quarter growth rates"""
        if data.empty:
            return {}
            
        data = data.sort_values('date')
        data['growth'] = data['value'].pct_change()
        
        return {
            row['date'].strftime('%Y-Q%q'): row['growth']
            for _, row in data.iterrows()
            if not pd.isna(row['growth'])
        }
        
    def _calculate_changes(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate quarter-over-quarter changes"""
        if data.empty:
            return {}
            
        data = data.sort_values('date')
        data['change'] = data['value'].diff()
        
        return {
            row['date'].strftime('%Y-Q%q'): row['change']
            for _, row in data.iterrows()
            if not pd.isna(row['change'])
        }
        
    def _calculate_correlations(self, gdp_data: pd.DataFrame,
                              emp_data: pd.DataFrame,
                              inf_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate correlations between indicators"""
        # Merge data on date
        merged = pd.merge(
            gdp_data[['date', 'value']].rename(columns={'value': 'gdp'}),
            emp_data[['date', 'value']].rename(columns={'value': 'employment'}),
            on='date', how='inner'
        )
        merged = pd.merge(
            merged,
            inf_data[['date', 'value']].rename(columns={'value': 'inflation'}),
            on='date', how='inner'
        )
        
        if len(merged) < 2:
            return {
                'gdp_employment': None,
                'gdp_inflation': None,
                'employment_inflation': None
            }
        
        correlations = {}
        correlations['gdp_employment'] = merged['gdp'].corr(merged['employment'])
        correlations['gdp_inflation'] = merged['gdp'].corr(merged['inflation'])
        correlations['employment_inflation'] = merged['employment'].corr(merged['inflation'])
        
        return correlations
        
    def create_visualization(self, results: Dict, output_path: str) -> None:
        """Create visualization of analysis results"""
        try:
            # Create subplots
            fig = make_subplots(
                rows=3, cols=1,
                subplot_titles=('GDP Growth', 'Employment Change', 'Inflation Change')
            )
            
            # Add traces for each country
            for country, data in results.items():
                # GDP Growth
                gdp_dates = list(data['gdp_growth'].keys())
                gdp_values = list(data['gdp_growth'].values())
                fig.add_trace(
                    go.Scatter(x=gdp_dates, y=gdp_values, name=f'{country} GDP'),
                    row=1, col=1
                )
                
                # Employment Change
                emp_dates = list(data['employment_change'].keys())
                emp_values = list(data['employment_change'].values())
                fig.add_trace(
                    go.Scatter(x=emp_dates, y=emp_values, name=f'{country} Employment'),
                    row=2, col=1
                )
                
                # Inflation Change
                inf_dates = list(data['inflation_change'].keys())
                inf_values = list(data['inflation_change'].values())
                fig.add_trace(
                    go.Scatter(x=inf_dates, y=inf_values, name=f'{country} Inflation'),
                    row=3, col=1
                )
            
            # Update layout
            fig.update_layout(
                height=900,
                title_text="Economic Indicators Analysis",
                showlegend=True
            )
            
            # Save figure
            fig.write_html(output_path)
            logger.info(f"Visualization saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            raise
