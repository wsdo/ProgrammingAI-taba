import pandas as pd
import numpy as np
from pathlib import Path
import logging
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
from scipy import stats
import json
import matplotlib.pyplot as plt
import seaborn as sns

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

class TimeSeriesAnalyzer:
    def __init__(self, data_path: str):
        """Initialize the time series analyzer with the data file path"""
        self.data_path = data_path
        self.df = pd.read_csv(data_path)
        self.df['year'] = pd.to_datetime(self.df['year'], format='%Y')
        self.df.set_index('year', inplace=True)
        
        # Key metrics to analyze
        self.key_metrics = [
            'education_expenditure.total_gdp_percentage',
            'education_participation.enrollment_rate_primary',
            'education_participation.enrollment_rate_secondary',
            'education_participation.enrollment_rate_tertiary',
            'education_participation.dropout_rate',
            'education_quality.teacher_qualification_index',
            'education_quality.digital_learning_percentage'
        ]
        
    def analyze_trends(self) -> dict:
        """Analyze trends in key education metrics"""
        trends = {}
        
        for metric in self.key_metrics:
            # Calculate yearly averages
            yearly_data = self.df[metric].resample('Y').mean()
            
            # Calculate overall trend
            X = np.arange(len(yearly_data)).reshape(-1, 1)
            y = yearly_data.values
            slope, intercept, r_value, p_value, std_err = stats.linregress(X.flatten(), y)
            
            # Calculate compound annual growth rate (CAGR)
            years = (yearly_data.index[-1] - yearly_data.index[0]).days / 365.25
            cagr = (yearly_data.iloc[-1] / yearly_data.iloc[0]) ** (1/years) - 1
            
            trends[metric] = {
                'slope': float(slope),
                'r_squared': float(r_value**2),
                'p_value': float(p_value),
                'trend_direction': 'increasing' if slope > 0 else 'decreasing',
                'trend_significance': p_value < 0.05,
                'cagr': float(cagr),
                'total_change': float(yearly_data.iloc[-1] - yearly_data.iloc[0]),
                'total_change_percentage': float((yearly_data.iloc[-1] / yearly_data.iloc[0] - 1) * 100)
            }
            
        return trends
    
    def analyze_seasonality(self) -> dict:
        """Analyze seasonality in key education metrics"""
        seasonality = {}
        
        for metric in self.key_metrics:
            try:
                # Perform seasonal decomposition
                decomposition = seasonal_decompose(
                    self.df[metric],
                    period=4,  # Assuming quarterly data
                    extrapolate_trend='freq'
                )
                
                # Calculate seasonal strength
                seasonal_strength = 1 - np.var(decomposition.resid) / np.var(decomposition.seasonal + decomposition.resid)
                
                seasonality[metric] = {
                    'has_seasonality': seasonal_strength > 0.1,
                    'seasonal_strength': float(seasonal_strength),
                    'peak_quarter': int(np.argmax(decomposition.seasonal[:4]) + 1),
                    'trough_quarter': int(np.argmin(decomposition.seasonal[:4]) + 1)
                }
            except Exception as e:
                logger.warning(f"Could not analyze seasonality for {metric}: {str(e)}")
                seasonality[metric] = None
                
        return seasonality
    
    def analyze_stationarity(self) -> dict:
        """Analyze stationarity of key education metrics"""
        stationarity = {}
        
        for metric in self.key_metrics:
            # Perform Augmented Dickey-Fuller test
            adf_result = adfuller(self.df[metric].dropna())
            
            stationarity[metric] = {
                'is_stationary': adf_result[1] < 0.05,
                'adf_statistic': float(adf_result[0]),
                'p_value': float(adf_result[1]),
                'critical_values': {
                    '1%': float(adf_result[4]['1%']),
                    '5%': float(adf_result[4]['5%']),
                    '10%': float(adf_result[4]['10%'])
                }
            }
            
        return stationarity
    
    def analyze_volatility(self) -> dict:
        """Analyze volatility in key education metrics"""
        volatility = {}
        
        for metric in self.key_metrics:
            # Calculate rolling statistics
            rolling_std = self.df[metric].rolling(window=4).std()
            
            volatility[metric] = {
                'mean_volatility': float(rolling_std.mean()),
                'max_volatility': float(rolling_std.max()),
                'min_volatility': float(rolling_std.min()),
                'volatility_trend': float(stats.linregress(
                    np.arange(len(rolling_std)),
                    rolling_std.bfill()
                )[0])
            }
            
        return volatility
    
    def generate_analysis_report(self, output_dir: str = None):
        """Generate a comprehensive time series analysis report"""
        if output_dir is None:
            output_dir = Path(self.data_path).parent
        else:
            output_dir = Path(output_dir)
            
        output_dir.mkdir(exist_ok=True)
        
        # Collect all analyses
        analysis_results = {
            'trends': self.analyze_trends(),
            'seasonality': self.analyze_seasonality(),
            'stationarity': self.analyze_stationarity(),
            'volatility': self.analyze_volatility()
        }
        
        # Save JSON report
        report_path = output_dir / 'time_series_analysis_report.json'
        with open(report_path, 'w') as f:
            json.dump(analysis_results, f, indent=4, cls=NumpyEncoder)
        
        # Generate text report
        text_report_path = output_dir / 'time_series_analysis_report.txt'
        with open(text_report_path, 'w') as f:
            f.write("Education Data Time Series Analysis Report\n")
            f.write("========================================\n\n")
            
            # Trend Analysis
            f.write("Trend Analysis\n")
            f.write("--------------\n")
            for metric, trend in analysis_results['trends'].items():
                f.write(f"\n{metric}:\n")
                f.write(f"  Direction: {trend['trend_direction']}\n")
                f.write(f"  Significance: {'significant' if trend['trend_significance'] else 'not significant'}\n")
                f.write(f"  Total Change: {trend['total_change']:.2f}\n")
                f.write(f"  Total Change (%): {trend['total_change_percentage']:.2f}%\n")
                f.write(f"  CAGR: {trend['cagr']*100:.2f}%\n")
                f.write(f"  R-squared: {trend['r_squared']:.3f}\n")
            
            # Seasonality Analysis
            f.write("\nSeasonality Analysis\n")
            f.write("-------------------\n")
            for metric, season in analysis_results['seasonality'].items():
                if season:
                    f.write(f"\n{metric}:\n")
                    f.write(f"  Has Seasonality: {season['has_seasonality']}\n")
                    f.write(f"  Seasonal Strength: {season['seasonal_strength']:.3f}\n")
                    f.write(f"  Peak Quarter: Q{season['peak_quarter']}\n")
                    f.write(f"  Trough Quarter: Q{season['trough_quarter']}\n")
            
            # Stationarity Analysis
            f.write("\nStationarity Analysis\n")
            f.write("--------------------\n")
            for metric, station in analysis_results['stationarity'].items():
                f.write(f"\n{metric}:\n")
                f.write(f"  Is Stationary: {station['is_stationary']}\n")
                f.write(f"  ADF Statistic: {station['adf_statistic']:.3f}\n")
                f.write(f"  P-value: {station['p_value']:.3f}\n")
            
            # Volatility Analysis
            f.write("\nVolatility Analysis\n")
            f.write("------------------\n")
            for metric, vol in analysis_results['volatility'].items():
                f.write(f"\n{metric}:\n")
                f.write(f"  Mean Volatility: {vol['mean_volatility']:.3f}\n")
                f.write(f"  Volatility Range: [{vol['min_volatility']:.3f}, {vol['max_volatility']:.3f}]\n")
                f.write(f"  Volatility Trend: {'increasing' if vol['volatility_trend'] > 0 else 'decreasing'}\n")
        
        # Generate visualizations
        self._generate_visualizations(output_dir)
        
        logger.info(f"Analysis reports generated:\n- {report_path}\n- {text_report_path}")
        return analysis_results
    
    def _generate_visualizations(self, output_dir: Path):
        """Generate visualizations for time series analysis"""
        # Set style
        plt.style.use('default')
        sns.set_theme()
        
        # Create trends plot
        plt.figure(figsize=(15, 10))
        for metric in self.key_metrics:
            plt.plot(self.df.index, self.df[metric], label=metric.split('.')[-1])
        plt.title('Education Metrics Trends Over Time')
        plt.xlabel('Year')
        plt.ylabel('Value')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(output_dir / 'trends_plot.png')
        plt.close()
        
        # Create volatility plot
        plt.figure(figsize=(15, 10))
        for metric in self.key_metrics:
            rolling_std = self.df[metric].rolling(window=4).std()
            plt.plot(self.df.index, rolling_std, label=metric.split('.')[-1])
        plt.title('Education Metrics Volatility Over Time')
        plt.xlabel('Year')
        plt.ylabel('Rolling Standard Deviation')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(output_dir / 'volatility_plot.png')
        plt.close()

if __name__ == "__main__":
    # Get the path to the processed data
    base_dir = Path(__file__).parent.parent
    data_file = base_dir / "processed_data" / "processed_education_data.csv"
    
    # Create analyzer and generate report
    analyzer = TimeSeriesAnalyzer(str(data_file))
    analyzer.generate_analysis_report(str(base_dir / "analysis_results"))
