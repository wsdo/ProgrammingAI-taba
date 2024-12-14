"""
Education Data Visualization Service
This module provides services for creating interactive visualizations of education data analysis.
"""
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, Any, List
import pandas as pd

from features.education_data.database.db_manager import DatabaseManager

class EducationVisualizationService:
    def __init__(self):
        self.db_manager = DatabaseManager()

    async def create_investment_trends_visualization(self) -> Dict[str, Any]:
        """
        Create an interactive visualization of education investment trends
        """
        # Get data from PostgreSQL
        query = """
            SELECT country, year, education_investment
            FROM education_metrics
            ORDER BY country, year
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        # Create line plot
        fig = px.line(df, 
                     x='year', 
                     y='education_investment',
                     color='country',
                     title='Education Investment Trends by Country')
        
        # Customize layout
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Education Investment (%)",
            hovermode='x unified'
        )
        
        # Save visualization config to MongoDB
        config = {
            'plot_type': 'line',
            'title': 'Education Investment Trends',
            'data': df.to_dict('records'),
            'layout': fig.to_dict()['layout']
        }
        
        config_id = self.db_manager.mongodb_db.visualization_configs.insert_one(config).inserted_id
        
        return {
            'config_id': str(config_id),
            'html': fig.to_html(include_plotlyjs=True, full_html=False)
        }
    
    async def create_correlation_heatmap(self) -> Dict[str, Any]:
        """
        Create a heatmap showing correlations between different metrics
        """
        # Get data from PostgreSQL
        query = """
            SELECT education_investment, student_teacher_ratio
            FROM education_metrics
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        # Calculate correlation matrix
        corr_matrix = df.corr()
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            colorscale='RdBu'
        ))
        
        fig.update_layout(
            title='Correlation Heatmap of Education Metrics',
            xaxis_title="Metrics",
            yaxis_title="Metrics"
        )
        
        # Save visualization config
        config = {
            'plot_type': 'heatmap',
            'title': 'Correlation Analysis',
            'data': corr_matrix.to_dict('records'),
            'layout': fig.to_dict()['layout']
        }
        
        config_id = self.db_manager.mongodb_db.visualization_configs.insert_one(config).inserted_id
        
        return {
            'config_id': str(config_id),
            'html': fig.to_html(include_plotlyjs=True, full_html=False)
        }
    
    async def create_country_rankings_chart(self) -> Dict[str, Any]:
        """
        Create a bar chart showing country rankings
        """
        # Get latest data from PostgreSQL
        query = """
            SELECT DISTINCT ON (country)
                country, education_investment
            FROM education_metrics
            ORDER BY country, year DESC
        """
        df = pd.read_sql(query, self.db_manager.get_postgres_connection())
        
        # Create bar chart
        fig = px.bar(df.sort_values('education_investment', ascending=True),
                    x='education_investment',
                    y='country',
                    orientation='h',
                    title='Country Rankings by Education Investment')
        
        fig.update_layout(
            xaxis_title="Education Investment (%)",
            yaxis_title="Country"
        )
        
        # Save visualization config
        config = {
            'plot_type': 'bar',
            'title': 'Country Rankings',
            'data': df.to_dict('records'),
            'layout': fig.to_dict()['layout']
        }
        
        config_id = self.db_manager.mongodb_db.visualization_configs.insert_one(config).inserted_id
        
        return {
            'config_id': str(config_id),
            'html': fig.to_html(include_plotlyjs=True, full_html=False)
        }
