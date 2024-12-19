"""
Module for creating data visualizations.
This module provides various visualization functions for education and economic data analysis.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataVisualizer:
    """Class for creating various data visualizations."""
    
    def __init__(self, style: str = 'seaborn'):
        """
        Initialize the visualizer with specified style.
        
        Args:
            style (str): The style to use for matplotlib plots
        """
        self.style = style
        plt.style.use(style)
        self.default_figsize = (12, 8)
        self.color_palette = sns.color_palette("husl", 8)
        
    def set_style(self, style: str) -> None:
        """
        Set the plotting style.
        
        Args:
            style (str): The style to use
        """
        self.style = style
        plt.style.use(style)

    def create_time_series_plot(self, df: pd.DataFrame,
                              x_col: str,
                              y_col: str,
                              group_col: str,
                              title: str,
                              interactive: bool = True) -> go.Figure:
        """
        Create a time series plot using Plotly.
        
        Args:
            df (pd.DataFrame): Input data
            x_col (str): Column for x-axis (typically time)
            y_col (str): Column for y-axis
            group_col (str): Column for grouping data
            title (str): Plot title
            interactive (bool): Whether to create interactive plot
            
        Returns:
            go.Figure: Plotly figure object
        """
        try:
            if interactive:
                fig = px.line(df, x=x_col, y=y_col, color=group_col,
                            title=title, template="plotly_white")
                fig.update_layout(
                    xaxis_title=x_col.replace('_', ' ').title(),
                    yaxis_title=y_col.replace('_', ' ').title(),
                    hovermode='x unified'
                )
            else:
                plt.figure(figsize=self.default_figsize)
                sns.lineplot(data=df, x=x_col, y=y_col, hue=group_col)
                plt.title(title)
                plt.xlabel(x_col.replace('_', ' ').title())
                plt.ylabel(y_col.replace('_', ' ').title())
                fig = plt.gcf()
                
            return fig
        except Exception as e:
            logger.error(f"Error creating time series plot: {str(e)}")
            raise

    def create_correlation_heatmap(self, 
                                 corr_matrix: pd.DataFrame,
                                 title: str = "Correlation Heatmap",
                                 interactive: bool = True) -> go.Figure:
        """
        Create a correlation heatmap.
        
        Args:
            corr_matrix (pd.DataFrame): Correlation matrix
            title (str): Plot title
            interactive (bool): Whether to create interactive plot
            
        Returns:
            go.Figure: Plotly figure object
        """
        try:
            if interactive:
                fig = go.Figure(data=go.Heatmap(
                    z=corr_matrix.values,
                    x=corr_matrix.columns,
                    y=corr_matrix.columns,
                    colorscale='RdBu',
                    zmin=-1, zmax=1
                ))
                fig.update_layout(
                    title=title,
                    template="plotly_white"
                )
            else:
                plt.figure(figsize=self.default_figsize)
                sns.heatmap(corr_matrix, annot=True, cmap='RdBu', vmin=-1, vmax=1)
                plt.title(title)
                fig = plt.gcf()
                
            return fig
        except Exception as e:
            logger.error(f"Error creating correlation heatmap: {str(e)}")
            raise

    def create_scatter_plot(self, df: pd.DataFrame,
                          x_col: str,
                          y_col: str,
                          color_col: Optional[str] = None,
                          title: str = "",
                          add_trendline: bool = True,
                          interactive: bool = True) -> go.Figure:
        """
        Create a scatter plot with optional trend line.
        
        Args:
            df (pd.DataFrame): Input data
            x_col (str): Column for x-axis
            y_col (str): Column for y-axis
            color_col (str, optional): Column for color coding points
            title (str): Plot title
            add_trendline (bool): Whether to add a trend line
            interactive (bool): Whether to create interactive plot
            
        Returns:
            go.Figure: Plotly figure object
        """
        try:
            if interactive:
                fig = px.scatter(df, x=x_col, y=y_col, color=color_col,
                               title=title, template="plotly_white",
                               trendline="ols" if add_trendline else None)
                fig.update_layout(
                    xaxis_title=x_col.replace('_', ' ').title(),
                    yaxis_title=y_col.replace('_', ' ').title()
                )
            else:
                plt.figure(figsize=self.default_figsize)
                if color_col:
                    sns.scatterplot(data=df, x=x_col, y=y_col, hue=color_col)
                else:
                    sns.scatterplot(data=df, x=x_col, y=y_col)
                if add_trendline:
                    sns.regplot(data=df, x=x_col, y=y_col, scatter=False, color='red')
                plt.title(title)
                fig = plt.gcf()
                
            return fig
        except Exception as e:
            logger.error(f"Error creating scatter plot: {str(e)}")
            raise

    def create_bar_chart(self, df: pd.DataFrame,
                        x_col: str,
                        y_col: str,
                        title: str,
                        orientation: str = 'v',
                        interactive: bool = True) -> go.Figure:
        """
        Create a bar chart.
        
        Args:
            df (pd.DataFrame): Input data
            x_col (str): Column for x-axis
            y_col (str): Column for y-axis
            title (str): Plot title
            orientation (str): 'v' for vertical, 'h' for horizontal
            interactive (bool): Whether to create interactive plot
            
        Returns:
            go.Figure: Plotly figure object
        """
        try:
            if interactive:
                fig = px.bar(df, x=x_col, y=y_col, title=title,
                           template="plotly_white", orientation=orientation)
                fig.update_layout(
                    xaxis_title=x_col.replace('_', ' ').title(),
                    yaxis_title=y_col.replace('_', ' ').title()
                )
            else:
                plt.figure(figsize=self.default_figsize)
                if orientation == 'h':
                    sns.barplot(data=df, y=x_col, x=y_col)
                else:
                    sns.barplot(data=df, x=x_col, y=y_col)
                plt.title(title)
                fig = plt.gcf()
                
            return fig
        except Exception as e:
            logger.error(f"Error creating bar chart: {str(e)}")
            raise

    def create_box_plot(self, df: pd.DataFrame,
                       x_col: str,
                       y_col: str,
                       title: str,
                       interactive: bool = True) -> go.Figure:
        """
        Create a box plot.
        
        Args:
            df (pd.DataFrame): Input data
            x_col (str): Column for x-axis
            y_col (str): Column for y-axis
            title (str): Plot title
            interactive (bool): Whether to create interactive plot
            
        Returns:
            go.Figure: Plotly figure object
        """
        try:
            if interactive:
                fig = px.box(df, x=x_col, y=y_col, title=title,
                           template="plotly_white")
                fig.update_layout(
                    xaxis_title=x_col.replace('_', ' ').title(),
                    yaxis_title=y_col.replace('_', ' ').title()
                )
            else:
                plt.figure(figsize=self.default_figsize)
                sns.boxplot(data=df, x=x_col, y=y_col)
                plt.title(title)
                fig = plt.gcf()
                
            return fig
        except Exception as e:
            logger.error(f"Error creating box plot: {str(e)}")
            raise

    def create_education_dashboard(self, education_data: pd.DataFrame,
                                 economic_data: pd.DataFrame) -> go.Figure:
        """
        Create a comprehensive dashboard for education analysis.
        
        Args:
            education_data (pd.DataFrame): Education investment data
            economic_data (pd.DataFrame): Economic indicator data
            
        Returns:
            go.Figure: Plotly figure object containing multiple subplots
        """
        try:
            # Create subplot grid
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Education Investment Over Time',
                    'Education vs GDP Growth',
                    'Investment Distribution by Country',
                    'Economic Indicators Correlation'
                )
            )

            # Add time series plot
            fig.add_trace(
                go.Scatter(
                    x=education_data['year'],
                    y=education_data['education_investment'],
                    mode='lines+markers',
                    name='Education Investment'
                ),
                row=1, col=1
            )

            # Add scatter plot
            fig.add_trace(
                go.Scatter(
                    x=education_data['education_investment'],
                    y=economic_data['gdp_growth'],
                    mode='markers',
                    name='Education vs GDP'
                ),
                row=1, col=2
            )

            # Add box plot
            fig.add_trace(
                go.Box(
                    x=education_data['country_code'],
                    y=education_data['education_investment'],
                    name='Investment Distribution'
                ),
                row=2, col=1
            )

            # Add correlation heatmap
            merged_data = pd.merge(
                education_data,
                economic_data,
                on=['country_code', 'year']
            )
            corr_matrix = merged_data[[
                'education_investment',
                'gdp_growth',
                'employment_rate'
            ]].corr()
            
            fig.add_trace(
                go.Heatmap(
                    z=corr_matrix.values,
                    x=corr_matrix.columns,
                    y=corr_matrix.columns,
                    colorscale='RdBu',
                    zmin=-1, zmax=1
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                title_text="Education Investment Analysis Dashboard",
                template="plotly_white"
            )

            return fig
        except Exception as e:
            logger.error(f"Error creating education dashboard: {str(e)}")
            raise
