"""
Education Data Analysis Service
This module provides comprehensive services for analyzing education data using both PostgreSQL and MongoDB.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from features.education_data.database.db_manager import DatabaseManager

class EducationAnalysisService:
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    async def analyze_education_investment(self) -> Dict[str, Any]:
        """
        Analyze education investment trends and their correlation with performance metrics
        """
        # Get data from PostgreSQL
        query = """
            SELECT country, year, education_investment, student_teacher_ratio
            FROM education_metrics
            ORDER BY country, year
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        # Perform analysis
        analysis_results = {
            'timestamp': datetime.utcnow(),
            'metrics': {
                'investment_trends': self._analyze_investment_trends(df),
                'correlation_analysis': self._analyze_correlations(df),
                'country_rankings': self._calculate_country_rankings(df),
                'efficiency_metrics': self._analyze_investment_efficiency(df)
            }
        }
        
        # Store results in MongoDB
        analysis_id = self.db_manager.mongodb_db.analysis_reports.insert_one(analysis_results).inserted_id
        
        return {'analysis_id': str(analysis_id), 'results': analysis_results}

    async def analyze_education_quality(self) -> Dict[str, Any]:
        """
        Analyze education quality indicators
        """
        query = """
            SELECT country, year, 
                   student_teacher_ratio,
                   completion_rate,
                   literacy_rate,
                   test_scores
            FROM education_metrics
            ORDER BY country, year
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        quality_analysis = {
            'timestamp': datetime.utcnow(),
            'metrics': {
                'quality_scores': self._calculate_quality_scores(df),
                'performance_trends': self._analyze_performance_trends(df),
                'regional_comparison': self._compare_regional_performance(df)
            }
        }
        
        analysis_id = self.db_manager.mongodb_db.analysis_reports.insert_one(quality_analysis).inserted_id
        return {'analysis_id': str(analysis_id), 'results': quality_analysis}

    async def analyze_resource_allocation(self) -> Dict[str, Any]:
        """
        Analyze education resource allocation patterns
        """
        query = """
            SELECT country, year,
                   education_investment,
                   infrastructure_spending,
                   teacher_training_investment,
                   technology_investment
            FROM education_metrics
            ORDER BY country, year
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        resource_analysis = {
            'timestamp': datetime.utcnow(),
            'metrics': {
                'spending_distribution': self._analyze_spending_distribution(df),
                'resource_efficiency': self._calculate_resource_efficiency(df),
                'investment_priorities': self._identify_investment_priorities(df)
            }
        }
        
        analysis_id = self.db_manager.mongodb_db.analysis_reports.insert_one(resource_analysis).inserted_id
        return {'analysis_id': str(analysis_id), 'results': resource_analysis}

    async def analyze_education_outcomes(self) -> Dict[str, Any]:
        """
        Analyze education outcomes and their relationships with various factors
        """
        query = """
            SELECT country, year,
                   completion_rate,
                   employment_rate,
                   higher_education_enrollment,
                   average_income
            FROM education_metrics
            ORDER BY country, year
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        outcomes_analysis = {
            'timestamp': datetime.utcnow(),
            'metrics': {
                'success_indicators': self._analyze_success_indicators(df),
                'outcome_correlations': self._analyze_outcome_correlations(df),
                'impact_assessment': self._assess_education_impact(df)
            }
        }
        
        analysis_id = self.db_manager.mongodb_db.analysis_reports.insert_one(outcomes_analysis).inserted_id
        return {'analysis_id': str(analysis_id), 'results': outcomes_analysis}

    async def forecast_education_metrics(self) -> Dict[str, Any]:
        """
        Perform time series forecasting on key education metrics
        """
        query = """
            SELECT country, year,
                   education_investment,
                   student_teacher_ratio,
                   completion_rate
            FROM education_metrics
            ORDER BY country, year
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        forecast_analysis = {
            'timestamp': datetime.utcnow(),
            'metrics': {
                'investment_forecast': self._forecast_metric(df, 'education_investment'),
                'performance_forecast': self._forecast_metric(df, 'completion_rate'),
                'resource_forecast': self._forecast_metric(df, 'student_teacher_ratio')
            }
        }
        
        analysis_id = self.db_manager.mongodb_db.analysis_reports.insert_one(forecast_analysis).inserted_id
        return {'analysis_id': str(analysis_id), 'results': forecast_analysis}

    def _calculate_quality_scores(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate composite quality scores for education systems"""
        # Normalize metrics
        scaler = StandardScaler()
        metrics = ['student_teacher_ratio', 'completion_rate', 'test_scores']
        normalized_data = pd.DataFrame(scaler.fit_transform(df[metrics]), columns=metrics)
        
        # Calculate composite score
        weights = {'student_teacher_ratio': -0.3, 'completion_rate': 0.4, 'test_scores': 0.3}
        quality_scores = sum(normalized_data[metric] * weight for metric, weight in weights.items())
        
        return {
            'average_score': float(quality_scores.mean()),
            'score_distribution': quality_scores.describe().to_dict(),
            'top_performers': df.loc[quality_scores.nlargest(5).index, 'country'].tolist()
        }

    def _analyze_spending_distribution(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze the distribution of education spending across different categories"""
        spending_columns = ['infrastructure_spending', 'teacher_training_investment', 'technology_investment']
        spending_distribution = df[spending_columns].mean().to_dict()
        
        # Calculate spending efficiency
        total_spending = df[spending_columns].sum(axis=1)
        completion_rate = df['completion_rate']
        efficiency_score = r2_score(total_spending, completion_rate)
        
        return {
            'distribution': spending_distribution,
            'efficiency_score': float(efficiency_score),
            'recommendations': self._generate_spending_recommendations(df)
        }

    def _forecast_metric(self, df: pd.DataFrame, metric: str, periods: int = 5) -> Dict[str, Any]:
        """Forecast future values for a given metric using Holt-Winters method"""
        # Prepare time series data
        ts_data = df.groupby('year')[metric].mean()
        
        # Fit model
        model = ExponentialSmoothing(ts_data, seasonal_periods=4, trend='add', seasonal='add')
        fitted_model = model.fit()
        
        # Generate forecast
        forecast = fitted_model.forecast(periods)
        
        return {
            'forecast_values': forecast.to_dict(),
            'confidence_intervals': fitted_model.get_prediction(start=len(ts_data), end=len(ts_data)+periods-1).conf_int().to_dict(),
            'model_accuracy': {
                'mape': np.mean(np.abs((ts_data - fitted_model.fittedvalues) / ts_data)) * 100,
                'r2_score': r2_score(ts_data, fitted_model.fittedvalues)
            }
        }

    def _analyze_success_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze various indicators of educational success"""
        success_metrics = ['completion_rate', 'employment_rate', 'higher_education_enrollment']
        
        # Calculate success scores
        success_scores = df[success_metrics].mean()
        
        # Identify trends
        trends = df.groupby('year')[success_metrics].mean().pct_change()
        
        return {
            'average_scores': success_scores.to_dict(),
            'trends': trends.mean().to_dict(),
            'success_factors': self._identify_success_factors(df)
        }

    def _identify_success_factors(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify key factors contributing to educational success"""
        success_metric = 'completion_rate'
        factors = ['education_investment', 'student_teacher_ratio', 'infrastructure_spending']
        
        correlations = df[factors + [success_metric]].corr()[success_metric].drop(success_metric)
        
        return [
            {'factor': factor, 'correlation': float(corr)}
            for factor, corr in correlations.items()
        ]

    def _generate_spending_recommendations(self, df: pd.DataFrame) -> List[str]:
        """Generate recommendations for optimizing education spending"""
        spending_cols = ['infrastructure_spending', 'teacher_training_investment', 'technology_investment']
        completion_rate = df['completion_rate']
        
        correlations = df[spending_cols].corrwith(completion_rate)
        recommendations = []
        
        for category, correlation in correlations.items():
            if correlation > 0.5:
                recommendations.append(f"Increase investment in {category.replace('_', ' ')}")
            elif correlation < 0:
                recommendations.append(f"Review efficiency of {category.replace('_', ' ')}")
                
        return recommendations

    def _analyze_investment_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze investment trends over time"""
        trends = df.groupby('year')['education_investment'].agg(['mean', 'min', 'max']).to_dict('index')
        return {
            'yearly_averages': trends,
            'overall_trend': 'increasing' if df.groupby('year')['education_investment'].mean().is_monotonic_increasing else 'fluctuating'
        }
    
    def _analyze_correlations(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate correlations between metrics"""
        correlation = df['education_investment'].corr(df['student_teacher_ratio'])
        return {
            'investment_vs_student_ratio': correlation
        }
    
    def _calculate_country_rankings(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate country rankings based on investment and performance"""
        latest_year = df['year'].max()
        latest_data = df[df['year'] == latest_year]
        
        rankings = latest_data.sort_values('education_investment', ascending=False)
        return rankings[['country', 'education_investment']].to_dict('records')

    def _analyze_investment_efficiency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze the efficiency of education investment"""
        # Calculate return on investment (ROI)
        roi = df['completion_rate'] / df['education_investment']
        
        # Calculate average ROI
        average_roi = roi.mean()
        
        # Identify top performers
        top_performers = df.loc[roi.nlargest(5).index, 'country'].tolist()
        
        return {
            'average_roi': float(average_roi),
            'top_performers': top_performers
        }

    def _analyze_performance_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze trends in education performance metrics"""
        # Calculate average performance scores
        average_scores = df[['completion_rate', 'literacy_rate', 'test_scores']].mean()
        
        # Identify trends
        trends = df.groupby('year')[['completion_rate', 'literacy_rate', 'test_scores']].mean().pct_change()
        
        return {
            'average_scores': average_scores.to_dict(),
            'trends': trends.mean().to_dict()
        }

    def _compare_regional_performance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compare education performance across regions"""
        # Calculate average performance scores by region
        regional_scores = df.groupby('region')[['completion_rate', 'literacy_rate', 'test_scores']].mean()
        
        # Identify top-performing regions
        top_regions = regional_scores.nlargest(5, 'completion_rate').index.tolist()
        
        return {
            'regional_scores': regional_scores.to_dict(),
            'top_regions': top_regions
        }

    def _calculate_resource_efficiency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate the efficiency of education resource allocation"""
        # Calculate return on investment (ROI) for each resource category
        roi = df[['infrastructure_spending', 'teacher_training_investment', 'technology_investment']].corrwith(df['completion_rate'])
        
        # Calculate average ROI
        average_roi = roi.mean()
        
        # Identify top-performing resource categories
        top_categories = roi.nlargest(3).index.tolist()
        
        return {
            'average_roi': float(average_roi),
            'top_categories': top_categories
        }

    def _identify_investment_priorities(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify investment priorities based on education outcomes"""
        # Calculate correlations between investment categories and education outcomes
        correlations = df[['infrastructure_spending', 'teacher_training_investment', 'technology_investment']].corrwith(df['completion_rate'])
        
        # Identify top-performing investment categories
        top_categories = correlations.nlargest(3).index.tolist()
        
        return [
            {'category': category, 'correlation': float(correlation)}
            for category, correlation in correlations.items()
        ]

    def _analyze_outcome_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze correlations between education outcomes and various factors"""
        # Calculate correlations between education outcomes and demographic factors
        correlations = df[['completion_rate', 'employment_rate', 'higher_education_enrollment']].corrwith(df[['average_income', 'population']])
        
        # Identify top-performing demographic factors
        top_factors = correlations.nlargest(3).index.tolist()
        
        return {
            'correlations': correlations.to_dict(),
            'top_factors': top_factors
        }

    def _assess_education_impact(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess the impact of education on various outcomes"""
        # Calculate the impact of education on employment rates
        employment_impact = df['employment_rate'].corr(df['completion_rate'])
        
        # Calculate the impact of education on higher education enrollment
        enrollment_impact = df['higher_education_enrollment'].corr(df['completion_rate'])
        
        # Calculate the impact of education on average income
        income_impact = df['average_income'].corr(df['completion_rate'])
        
        return {
            'employment_impact': float(employment_impact),
            'enrollment_impact': float(enrollment_impact),
            'income_impact': float(income_impact)
        }
