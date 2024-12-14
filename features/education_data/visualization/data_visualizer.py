import pandas as pd
import numpy as np
from pathlib import Path
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataVisualizer:
    def __init__(self, data_path: str):
        """Initialize the data visualizer with the data file path"""
        self.data_path = data_path
        self.df = pd.read_csv(data_path)
        self.df['year'] = pd.to_datetime(self.df['year'], format='%Y')
        
        # Set style for static plots
        plt.style.use('default')
        sns.set_theme(style="whitegrid")
        
    def create_investment_trends_plot(self, output_dir: Path):
        """Create investment trends visualization"""
        # Select relevant columns
        investment_cols = [
            'education_expenditure.total_gdp_percentage',
            'education_expenditure.per_student_primary',
            'education_expenditure.per_student_secondary',
            'education_expenditure.per_student_tertiary'
        ]
        
        # Create plotly figure
        fig = go.Figure()
        
        for col in investment_cols:
            fig.add_trace(
                go.Scatter(
                    x=self.df['year'],
                    y=self.df[col],
                    name=col.split('.')[-1],
                    mode='lines+markers'
                )
            )
        
        fig.update_layout(
            title='Education Investment Trends Over Time',
            xaxis_title='Year',
            yaxis_title='Investment Value',
            template='plotly_white'
        )
        
        fig.write_html(output_dir / 'investment_trends.html')
        
    def create_quality_metrics_plot(self, output_dir: Path):
        """Create education quality metrics visualization"""
        quality_cols = [
            'education_quality.teacher_qualification_index',
            'education_quality.teaching_hours_yearly',
            'education_quality.digital_learning_percentage',
            'overall_quality_score'
        ]
        
        # Create subplot figure
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[col.split('.')[-1] for col in quality_cols]
        )
        
        for i, col in enumerate(quality_cols, 1):
            row = (i-1) // 2 + 1
            col_idx = (i-1) % 2 + 1
            
            fig.add_trace(
                go.Scatter(
                    x=self.df['year'],
                    y=self.df[col],
                    name=col.split('.')[-1],
                    mode='lines+markers'
                ),
                row=row, col=col_idx
            )
        
        fig.update_layout(
            height=800,
            title_text='Education Quality Metrics Over Time',
            template='plotly_white'
        )
        
        fig.write_html(output_dir / 'quality_metrics.html')
        
    def create_correlation_heatmap(self, output_dir: Path):
        """Create correlation heatmap for key metrics"""
        key_metrics = [
            'education_expenditure.total_gdp_percentage',
            'education_expenditure.per_student_tertiary',
            'education_quality.teaching_hours_yearly',
            'education_quality.teacher_qualification_index',
            'education_quality.digital_learning_percentage',
            'education_participation.dropout_rate',
            'overall_quality_score'
        ]
        
        # Calculate correlation matrix
        corr_matrix = self.df[key_metrics].corr()
        
        # Create heatmap using plotly
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix,
            x=[col.split('.')[-1] for col in corr_matrix.columns],
            y=[col.split('.')[-1] for col in corr_matrix.columns],
            colorscale='RdBu',
            zmin=-1, zmax=1
        ))
        
        fig.update_layout(
            title='Correlation Heatmap of Key Education Metrics',
            template='plotly_white'
        )
        
        fig.write_html(output_dir / 'correlation_heatmap.html')
        
    def create_education_dashboard(self, output_dir: Path):
        """Create an interactive dashboard combining multiple visualizations"""
        # Create figure with secondary y-axis
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Investment Trends',
                'Quality Metrics',
                'Participation Rates',
                'Digital Transformation'
            ),
            specs=[[{"secondary_y": True}, {"secondary_y": True}],
                  [{"secondary_y": True}, {"secondary_y": True}]]
        )
        
        # Investment Trends
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_expenditure.total_gdp_percentage'],
                name='GDP Percentage',
                mode='lines'
            ),
            row=1, col=1, secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_expenditure.per_student_tertiary'],
                name='Per Student (Tertiary)',
                mode='lines'
            ),
            row=1, col=1, secondary_y=True
        )
        
        # Quality Metrics
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_quality.teacher_qualification_index'],
                name='Teacher Qualification',
                mode='lines'
            ),
            row=1, col=2, secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['overall_quality_score'],
                name='Overall Quality',
                mode='lines'
            ),
            row=1, col=2, secondary_y=True
        )
        
        # Participation Rates
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_participation.enrollment_rate_tertiary'],
                name='Tertiary Enrollment',
                mode='lines'
            ),
            row=2, col=1, secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_participation.dropout_rate'],
                name='Dropout Rate',
                mode='lines'
            ),
            row=2, col=1, secondary_y=True
        )
        
        # Digital Transformation
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_quality.digital_learning_percentage'],
                name='Digital Learning',
                mode='lines'
            ),
            row=2, col=2, secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=self.df['year'],
                y=self.df['education_resources.digital_resources_investment'],
                name='Digital Investment',
                mode='lines'
            ),
            row=2, col=2, secondary_y=True
        )
        
        # Update layout
        fig.update_layout(
            height=1000,
            title_text='Education System Dashboard',
            template='plotly_white'
        )
        
        # Update axes labels
        fig.update_yaxes(title_text="GDP Percentage", row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="Per Student Amount", row=1, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Index Value", row=1, col=2, secondary_y=False)
        fig.update_yaxes(title_text="Quality Score", row=1, col=2, secondary_y=True)
        fig.update_yaxes(title_text="Enrollment Rate", row=2, col=1, secondary_y=False)
        fig.update_yaxes(title_text="Dropout Rate", row=2, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Digital Learning %", row=2, col=2, secondary_y=False)
        fig.update_yaxes(title_text="Investment Amount", row=2, col=2, secondary_y=True)
        
        fig.write_html(output_dir / 'education_dashboard.html')
        
    def generate_all_visualizations(self, output_dir: str = None):
        """Generate all visualizations"""
        if output_dir is None:
            output_dir = Path(self.data_path).parent
        else:
            output_dir = Path(output_dir)
            
        output_dir = output_dir / 'visualizations'
        output_dir.mkdir(exist_ok=True)
        
        # Generate all plots
        self.create_investment_trends_plot(output_dir)
        self.create_quality_metrics_plot(output_dir)
        self.create_correlation_heatmap(output_dir)
        self.create_education_dashboard(output_dir)
        
        logger.info(f"All visualizations generated in: {output_dir}")

if __name__ == "__main__":
    # Get the path to the processed data
    base_dir = Path(__file__).parent.parent
    data_file = base_dir / "processed_data" / "processed_education_data.csv"
    
    # Create visualizer and generate visualizations
    visualizer = DataVisualizer(str(data_file))
    visualizer.generate_all_visualizations(str(base_dir / "analysis_results"))
