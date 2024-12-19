"""
Module for analyzing education data.
This module handles statistical analysis, correlation studies, and predictive modeling
of education investment and economic growth data.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
import statsmodels.api as sm
from scipy import stats
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EducationAnalyzer:
    """Class for analyzing education data and its relationship with economic indicators."""
    
    def __init__(self):
        """Initialize the analyzer with default parameters."""
        self.scaler = StandardScaler()
        self.correlation_threshold = 0.3
        
    def calculate_descriptive_stats(self, df: pd.DataFrame, 
                                  groupby_cols: List[str]) -> pd.DataFrame:
        """
        Calculate descriptive statistics for numerical columns.
        
        Args:
            df (pd.DataFrame): Input data
            groupby_cols (List[str]): Columns to group by
            
        Returns:
            pd.DataFrame: Descriptive statistics
        """
        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            stats_df = df.groupby(groupby_cols)[numeric_cols].agg([
                'count', 'mean', 'std', 'min', 'max', 'median'
            ]).round(2)
            
            logger.info(f"Calculated descriptive statistics grouped by {groupby_cols}")
            return stats_df
        except Exception as e:
            logger.error(f"Error calculating descriptive statistics: {str(e)}")
            raise

    def analyze_trends(self, df: pd.DataFrame, 
                      target_col: str, 
                      time_col: str = 'year') -> Dict:
        """
        Analyze trends in the target column over time.
        
        Args:
            df (pd.DataFrame): Input data
            target_col (str): Column to analyze
            time_col (str): Time column name
            
        Returns:
            Dict: Trend analysis results
        """
        try:
            # Calculate year-over-year growth
            df['yoy_growth'] = df.groupby('country_code')[target_col].pct_change()
            
            # Calculate compound annual growth rate (CAGR)
            years = df[time_col].max() - df[time_col].min()
            start_values = df.groupby('country_code')[target_col].first()
            end_values = df.groupby('country_code')[target_col].last()
            cagr = (end_values / start_values) ** (1/years) - 1
            
            # Perform linear regression for trend
            trend_results = {}
            for country in df['country_code'].unique():
                country_data = df[df['country_code'] == country]
                X = country_data[time_col].values.reshape(-1, 1)
                y = country_data[target_col].values
                
                model = LinearRegression()
                model.fit(X, y)
                trend_results[country] = {
                    'slope': model.coef_[0],
                    'r2': r2_score(y, model.predict(X))
                }
            
            return {
                'yoy_growth_stats': df['yoy_growth'].describe(),
                'cagr_by_country': cagr,
                'trend_analysis': trend_results
            }
        except Exception as e:
            logger.error(f"Error analyzing trends: {str(e)}")
            raise

    def correlation_analysis(self, df: pd.DataFrame, 
                           target_cols: List[str]) -> Tuple[pd.DataFrame, Dict]:
        """
        Perform correlation analysis between target columns.
        
        Args:
            df (pd.DataFrame): Input data
            target_cols (List[str]): Columns to analyze
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Correlation matrix and significant correlations
        """
        try:
            # Calculate correlation matrix
            corr_matrix = df[target_cols].corr()
            
            # Find significant correlations
            significant_corr = {}
            for col1 in target_cols:
                for col2 in target_cols:
                    if col1 < col2:  # Avoid duplicate pairs
                        corr_value = corr_matrix.loc[col1, col2]
                        if abs(corr_value) > self.correlation_threshold:
                            significant_corr[f"{col1} vs {col2}"] = {
                                'correlation': corr_value,
                                'strength': self._interpret_correlation(corr_value)
                            }
            
            return corr_matrix, significant_corr
        except Exception as e:
            logger.error(f"Error in correlation analysis: {str(e)}")
            raise

    def _interpret_correlation(self, corr_value: float) -> str:
        """
        Interpret the strength of a correlation coefficient.
        
        Args:
            corr_value (float): Correlation coefficient
            
        Returns:
            str: Interpretation of correlation strength
        """
        abs_corr = abs(corr_value)
        if abs_corr > 0.7:
            return 'Strong'
        elif abs_corr > 0.5:
            return 'Moderate'
        elif abs_corr > 0.3:
            return 'Weak'
        else:
            return 'Very weak'

    def regression_analysis(self, df: pd.DataFrame, 
                          dependent_var: str, 
                          independent_vars: List[str]) -> Dict:
        """
        Perform regression analysis to study relationships between variables.
        
        Args:
            df (pd.DataFrame): Input data
            dependent_var (str): Dependent variable
            independent_vars (List[str]): Independent variables
            
        Returns:
            Dict: Regression analysis results
        """
        try:
            # Prepare data
            X = df[independent_vars]
            y = df[dependent_var]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Fit model
            model = LinearRegression()
            model.fit(X_train_scaled, y_train)
            
            # Make predictions
            y_pred = model.predict(X_test_scaled)
            
            # Calculate metrics
            r2 = r2_score(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            
            # Prepare detailed statistics using statsmodels
            X_train_sm = sm.add_constant(X_train_scaled)
            model_sm = sm.OLS(y_train, X_train_sm).fit()
            
            return {
                'r2_score': r2,
                'rmse': rmse,
                'coefficients': dict(zip(independent_vars, model.coef_)),
                'intercept': model.intercept_,
                'detailed_statistics': model_sm.summary()
            }
        except Exception as e:
            logger.error(f"Error in regression analysis: {str(e)}")
            raise

    def analyze_education_impact(self, education_data: pd.DataFrame, 
                               economic_data: pd.DataFrame) -> Dict:
        """
        Analyze the impact of education investment on economic indicators.
        
        Args:
            education_data (pd.DataFrame): Education investment data
            economic_data (pd.DataFrame): Economic indicator data
            
        Returns:
            Dict: Analysis results
        """
        try:
            # Merge education and economic data
            merged_data = pd.merge(
                education_data,
                economic_data,
                on=['country_code', 'year'],
                suffixes=('_edu', '_econ')
            )
            
            # Perform correlation analysis
            corr_matrix, significant_corr = self.correlation_analysis(
                merged_data,
                ['education_investment', 'gdp_growth', 'employment_rate']
            )
            
            # Perform regression analysis
            regression_results = self.regression_analysis(
                merged_data,
                'gdp_growth',
                ['education_investment', 'employment_rate']
            )
            
            # Analyze trends
            trend_results = self.analyze_trends(
                merged_data,
                'education_investment'
            )
            
            return {
                'correlation_analysis': significant_corr,
                'regression_analysis': regression_results,
                'trend_analysis': trend_results
            }
        except Exception as e:
            logger.error(f"Error analyzing education impact: {str(e)}")
            raise
