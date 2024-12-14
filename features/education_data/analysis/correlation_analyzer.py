import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.preprocessing import StandardScaler
import networkx as nx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CorrelationAnalyzer:
    def __init__(self, data_path: str):
        """Initialize the correlation analyzer with the data file path"""
        self.data_path = data_path
        self.df = pd.read_csv(data_path)
        self.numerical_columns = [
            col for col in self.df.columns 
            if self.df[col].dtype in ['int64', 'float64'] and col != 'year'
        ]
        
    def calculate_pearson_correlation(self) -> pd.DataFrame:
        """Calculate Pearson correlation coefficients"""
        return self.df[self.numerical_columns].corr(method='pearson')
    
    def calculate_spearman_correlation(self) -> pd.DataFrame:
        """Calculate Spearman correlation coefficients"""
        return self.df[self.numerical_columns].corr(method='spearman')
    
    def calculate_partial_correlations(self) -> dict:
        """Calculate partial correlations controlling for other variables"""
        partial_corr = {}
        scaled_data = StandardScaler().fit_transform(self.df[self.numerical_columns])
        df_scaled = pd.DataFrame(scaled_data, columns=self.numerical_columns)
        
        for i, var1 in enumerate(self.numerical_columns):
            partial_corr[var1] = {}
            for j, var2 in enumerate(self.numerical_columns):
                if i < j:
                    # Get all other variables to control for
                    control_vars = [col for col in self.numerical_columns if col not in [var1, var2]]
                    
                    # Calculate partial correlation
                    x = df_scaled[var1]
                    y = df_scaled[var2]
                    z = df_scaled[control_vars]
                    
                    # Residualize both x and y on z
                    x_resid = pd.Series(x - z.dot(np.linalg.pinv(z).dot(x)))
                    y_resid = pd.Series(y - z.dot(np.linalg.pinv(z).dot(y)))
                    
                    # Calculate correlation between residuals
                    corr, p_value = stats.pearsonr(x_resid, y_resid)
                    
                    partial_corr[var1][var2] = {
                        'correlation': float(corr),
                        'p_value': float(p_value)
                    }
        
        return partial_corr
    
    def identify_key_correlations(self, threshold: float = 0.5) -> dict:
        """Identify strong correlations above the threshold"""
        corr_matrix = self.calculate_pearson_correlation()
        strong_correlations = {}
        
        for i in range(len(self.numerical_columns)):
            for j in range(i + 1, len(self.numerical_columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) >= threshold:
                    var1 = self.numerical_columns[i]
                    var2 = self.numerical_columns[j]
                    strong_correlations[f"{var1} - {var2}"] = {
                        'correlation': float(corr_value),
                        'direction': 'positive' if corr_value > 0 else 'negative',
                        'strength': 'very strong' if abs(corr_value) > 0.8 else 'strong'
                    }
        
        return strong_correlations
    
    def analyze_correlation_stability(self) -> dict:
        """Analyze the stability of correlations over time"""
        stability = {}
        years = sorted(self.df['year'].unique())
        
        for i, var1 in enumerate(self.numerical_columns):
            for j, var2 in enumerate(self.numerical_columns):
                if i < j:
                    correlations = []
                    for year in years:
                        year_data = self.df[self.df['year'] == year]
                        corr = year_data[[var1, var2]].corr().iloc[0, 1]
                        correlations.append(corr)
                    
                    stability[f"{var1} - {var2}"] = {
                        'mean_correlation': float(np.mean(correlations)),
                        'std_correlation': float(np.std(correlations)),
                        'min_correlation': float(np.min(correlations)),
                        'max_correlation': float(np.max(correlations)),
                        'correlation_trend': float(stats.linregress(range(len(correlations)), correlations)[0])
                    }
        
        return stability
    
    def generate_analysis_report(self, output_dir: str = None):
        """Generate a comprehensive correlation analysis report"""
        if output_dir is None:
            output_dir = Path(self.data_path).parent
        else:
            output_dir = Path(output_dir)
            
        output_dir.mkdir(exist_ok=True)
        
        # Collect all analyses
        analysis_results = {
            'pearson_correlation': self.calculate_pearson_correlation().to_dict(),
            'spearman_correlation': self.calculate_spearman_correlation().to_dict(),
            'partial_correlations': self.calculate_partial_correlations(),
            'key_correlations': self.identify_key_correlations(),
            'correlation_stability': self.analyze_correlation_stability()
        }
        
        # Save JSON report
        report_path = output_dir / 'correlation_analysis_report.json'
        with open(report_path, 'w') as f:
            json.dump(analysis_results, f, indent=4)
        
        # Generate text report
        text_report_path = output_dir / 'correlation_analysis_report.txt'
        with open(text_report_path, 'w') as f:
            f.write("Education Data Correlation Analysis Report\n")
            f.write("=======================================\n\n")
            
            # Key correlations
            f.write("Strong Correlations\n")
            f.write("-----------------\n")
            for pair, stats in analysis_results['key_correlations'].items():
                f.write(f"\n{pair}:\n")
                f.write(f"  Correlation: {stats['correlation']:.3f}\n")
                f.write(f"  Direction: {stats['direction']}\n")
                f.write(f"  Strength: {stats['strength']}\n")
            
            # Correlation stability
            f.write("\nCorrelation Stability Analysis\n")
            f.write("----------------------------\n")
            for pair, stats in analysis_results['correlation_stability'].items():
                f.write(f"\n{pair}:\n")
                f.write(f"  Mean Correlation: {stats['mean_correlation']:.3f}\n")
                f.write(f"  Correlation Range: [{stats['min_correlation']:.3f}, {stats['max_correlation']:.3f}]\n")
                f.write(f"  Correlation Trend: {'increasing' if stats['correlation_trend'] > 0 else 'decreasing'}\n")
        
        # Generate visualizations
        self._generate_visualizations(output_dir)
        
        logger.info(f"Analysis reports generated:\n- {report_path}\n- {text_report_path}")
        return analysis_results
    
    def _generate_visualizations(self, output_dir: Path):
        """Generate visualizations for correlation analysis"""
        # Set style
        plt.style.use('default')
        sns.set_theme()
        
        # Create correlation heatmap
        plt.figure(figsize=(15, 12))
        sns.heatmap(
            self.calculate_pearson_correlation(),
            annot=True,
            cmap='coolwarm',
            center=0,
            fmt='.2f'
        )
        plt.title('Correlation Heatmap of Education Metrics')
        plt.tight_layout()
        plt.savefig(output_dir / 'correlation_heatmap.png')
        plt.close()
        
        # Create correlation network plot for strong correlations
        strong_corr = self.identify_key_correlations()
        if strong_corr:
            plt.figure(figsize=(15, 12))
            G = nx.Graph()
            
            # Add nodes
            nodes = set()
            for pair in strong_corr.keys():
                var1, var2 = pair.split(' - ')
                nodes.add(var1)
                nodes.add(var2)
            
            for node in nodes:
                G.add_node(node)
            
            # Add edges
            for pair, stats in strong_corr.items():
                var1, var2 = pair.split(' - ')
                G.add_edge(
                    var1, var2,
                    weight=abs(stats['correlation']),
                    color='blue' if stats['correlation'] > 0 else 'red'
                )
            
            # Draw the network
            pos = nx.spring_layout(G)
            edges = G.edges()
            weights = [G[u][v]['weight'] * 2 for u, v in edges]
            colors = [G[u][v]['color'] for u, v in edges]
            
            nx.draw(
                G, pos,
                with_labels=True,
                node_color='lightblue',
                node_size=2000,
                font_size=8,
                width=weights,
                edge_color=colors
            )
            
            plt.title('Correlation Network of Education Metrics')
            plt.tight_layout()
            plt.savefig(output_dir / 'correlation_network.png')
            plt.close()

if __name__ == "__main__":
    # Get the path to the processed data
    base_dir = Path(__file__).parent.parent
    data_file = base_dir / "processed_data" / "processed_education_data.csv"
    
    # Create analyzer and generate report
    analyzer = CorrelationAnalyzer(str(data_file))
    analyzer.generate_analysis_report(str(base_dir / "analysis_results"))
