import pandas as pd
import numpy as np
from pathlib import Path
import logging
from scipy import stats
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NumpyEncoder(json.JSONEncoder):
    """Special JSON encoder for numpy types"""
    def default(self, obj):
        if isinstance(obj, (np.bool_, np.bool8)):
            return bool(obj)
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                          np.int16, np.int32, np.int64, np.uint8,
                          np.uint16, np.uint32, np.uint64)):
            return int(obj)
        if isinstance(obj, (np.float_, np.float16, np.float32,
                          np.float64)):
            return float(obj)
        if isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

class StatisticalAnalyzer:
    def __init__(self, data_path: str):
        """Initialize the statistical analyzer with the data file path"""
        self.data_path = data_path
        self.df = pd.read_csv(data_path)
        self.numerical_columns = [
            col for col in self.df.columns 
            if self.df[col].dtype in ['int64', 'float64']
        ]
        
    def generate_summary_statistics(self) -> dict:
        """Generate summary statistics for all numerical columns"""
        summary_stats = {}
        
        for col in self.numerical_columns:
            stats_dict = {
                'count': int(self.df[col].count()),
                'mean': float(self.df[col].mean()),
                'std': float(self.df[col].std()),
                'min': float(self.df[col].min()),
                'q1': float(self.df[col].quantile(0.25)),
                'median': float(self.df[col].median()),
                'q3': float(self.df[col].quantile(0.75)),
                'max': float(self.df[col].max()),
                'skewness': float(stats.skew(self.df[col].dropna())),
                'kurtosis': float(stats.kurtosis(self.df[col].dropna())),
                'missing_values': int(self.df[col].isnull().sum())
            }
            summary_stats[col] = stats_dict
            
        return summary_stats

    def analyze_categorical_data(self) -> dict:
        """Analyze categorical variables"""
        categorical_stats = {}
        categorical_columns = self.df.select_dtypes(include=['object']).columns
        
        for col in categorical_columns:
            value_counts = self.df[col].value_counts()
            unique_count = len(value_counts)
            
            stats_dict = {
                'unique_values': unique_count,
                'most_common': value_counts.index[0],
                'most_common_count': int(value_counts.iloc[0]),
                'least_common': value_counts.index[-1],
                'least_common_count': int(value_counts.iloc[-1]),
                'missing_values': int(self.df[col].isnull().sum())
            }
            categorical_stats[col] = stats_dict
            
        return categorical_stats

    def analyze_distributions(self) -> dict:
        """Analyze the distribution of numerical variables"""
        distribution_stats = {}
        
        for col in self.numerical_columns:
            # Perform Shapiro-Wilk test for normality
            _, p_value = stats.shapiro(self.df[col].dropna())
            
            distribution_dict = {
                'normality_test_p_value': float(p_value),
                'is_normal': p_value > 0.05,
                'distribution_type': 'normal' if p_value > 0.05 else 'non-normal'
            }
            distribution_stats[col] = distribution_dict
            
        return distribution_stats

    def analyze_outliers(self, threshold: float = 1.5) -> dict:
        """Detect outliers using IQR method"""
        outlier_stats = {}
        
        for col in self.numerical_columns:
            Q1 = self.df[col].quantile(0.25)
            Q3 = self.df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            outliers = self.df[
                (self.df[col] < lower_bound) | 
                (self.df[col] > upper_bound)
            ]
            
            outlier_dict = {
                'total_outliers': len(outliers),
                'outlier_percentage': float(len(outliers) / len(self.df) * 100),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
                'min_outlier': float(outliers[col].min()) if len(outliers) > 0 else None,
                'max_outlier': float(outliers[col].max()) if len(outliers) > 0 else None
            }
            outlier_stats[col] = outlier_dict
            
        return outlier_stats

    def generate_analysis_report(self, output_dir: str = None):
        """Generate a comprehensive statistical analysis report"""
        if output_dir is None:
            output_dir = Path(self.data_path).parent
        else:
            output_dir = Path(output_dir)
            
        output_dir.mkdir(exist_ok=True)
        
        # Collect all analyses
        analysis_results = {
            'summary_statistics': self.generate_summary_statistics(),
            'categorical_analysis': self.analyze_categorical_data(),
            'distribution_analysis': self.analyze_distributions(),
            'outlier_analysis': self.analyze_outliers()
        }
        
        # Save JSON report
        report_path = output_dir / 'statistical_analysis_report.json'
        with open(report_path, 'w') as f:
            json.dump(analysis_results, f, indent=4, cls=NumpyEncoder)
            
        # Generate text report
        text_report_path = output_dir / 'statistical_analysis_report.txt'
        with open(text_report_path, 'w') as f:
            f.write("Education Data Statistical Analysis Report\n")
            f.write("=======================================\n\n")
            
            # Dataset overview
            f.write("Dataset Overview\n")
            f.write("--------------\n")
            f.write(f"Total Records: {len(self.df)}\n")
            f.write(f"Total Features: {len(self.df.columns)}\n")
            f.write(f"Numerical Features: {len(self.numerical_columns)}\n")
            f.write(f"Categorical Features: {len(self.df.select_dtypes(include=['object']).columns)}\n\n")
            
            # Summary statistics
            f.write("Summary Statistics\n")
            f.write("-----------------\n")
            for col, stats in analysis_results['summary_statistics'].items():
                f.write(f"\n{col}:\n")
                for stat_name, value in stats.items():
                    f.write(f"  {stat_name}: {value:.2f}\n")
            
            # Categorical analysis
            f.write("\nCategorical Data Analysis\n")
            f.write("-----------------------\n")
            for col, stats in analysis_results['categorical_analysis'].items():
                f.write(f"\n{col}:\n")
                for stat_name, value in stats.items():
                    f.write(f"  {stat_name}: {value}\n")
            
            # Distribution analysis
            f.write("\nDistribution Analysis\n")
            f.write("--------------------\n")
            for col, stats in analysis_results['distribution_analysis'].items():
                f.write(f"\n{col}:\n")
                for stat_name, value in stats.items():
                    f.write(f"  {stat_name}: {value}\n")
            
            # Outlier analysis
            f.write("\nOutlier Analysis\n")
            f.write("---------------\n")
            for col, stats in analysis_results['outlier_analysis'].items():
                f.write(f"\n{col}:\n")
                for stat_name, value in stats.items():
                    if value is not None:
                        f.write(f"  {stat_name}: {value:.2f}\n")
                    else:
                        f.write(f"  {stat_name}: None\n")
        
        logger.info(f"Analysis reports generated:\n- {report_path}\n- {text_report_path}")
        return analysis_results

if __name__ == "__main__":
    # Get the path to the processed data
    base_dir = Path(__file__).parent.parent
    data_file = base_dir / "processed_data" / "processed_education_data.csv"
    
    # Create analyzer and generate report
    analyzer = StatisticalAnalyzer(str(data_file))
    analyzer.generate_analysis_report(str(base_dir / "analysis_results"))
