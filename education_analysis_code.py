"""
Education Data Analysis Project
This script contains the complete code for analyzing education data across different countries.
"""

# 1. Required Imports
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
from statsmodels.tsa.statespace.sarimax import SARIMAX
import eurostat
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# 设置日志
logging.basicConfig(
    filename='education_data_collection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 2. 数据收集函数
def collect_education_data():
    """
    从Eurostat收集教育数据
    返回包含不同教育指标的字典
    """
    datasets = {
        'education_investment': 'educ_uoe_fine09',  # 教育投资
        'student_teacher_ratio': 'educ_uoe_perp04', # 师生比
        'completion_rate': 'edat_lfse_03',          # 完成率
        'literacy_rate': 'edat_lfse_01'            # 识字率
    }
    
    data = {}
    for metric, code in datasets.items():
        try:
            logging.info(f"开始收集{metric}数据...")
            df = eurostat.get_data_df(code)
            
            # 处理地理和时间信息
            if 'geo\\TIME_PERIOD' in df.columns:
                df[['geo', 'time']] = df['geo\\TIME_PERIOD'].str.split('\\', expand=True)
                df = df.drop('geo\\TIME_PERIOD', axis=1)
            
            # 标准化数据格式
            year_columns = [col for col in df.columns if col.isdigit()]
            id_vars = [col for col in df.columns if col not in year_columns]
            
            # 转换数据格式
            df = df.melt(
                id_vars=id_vars,
                value_vars=year_columns,
                var_name='year',
                value_name=metric
            )
            
            # 数据清理
            df[metric] = pd.to_numeric(df[metric], errors='coerce')
            data[metric] = df
            logging.info(f"{metric}数据收集成功")
            
        except Exception as e:
            logging.error(f"收集{metric}数据时出错: {str(e)}")
            print(f"收集{metric}数据时出错: {str(e)}")
    
    return data

# 3. 数据预处理函数
def preprocess_data(data):
    """
    预处理和合并所有教育数据
    """
    processed_data = None
    
    for metric, df in data.items():
        logging.info(f"处理{metric}数据...")
        
        # 确定合并列
        common_cols = ['geo', 'time', 'year']
        merge_cols = [col for col in common_cols if col in df.columns]
        
        if not merge_cols:
            logging.warning(f"{metric}数据集中未找到合并列")
            continue
        
        # 准备当前数据集
        cols_to_keep = merge_cols + [metric]
        current_df = df[cols_to_keep].copy()
        
        # 合并数据
        if processed_data is None:
            processed_data = current_df
        else:
            processed_data = processed_data.merge(
                current_df,
                on=merge_cols,
                how='outer'
            )
    
    if processed_data is not None:
        # 数据清理和标准化
        processed_data = processed_data.dropna()
        processed_data['year'] = pd.to_numeric(processed_data['year'])
        processed_data = processed_data.rename(columns={'geo': 'country'})
        processed_data = processed_data.sort_values('year')
        
        logging.info("数据预处理完成")
    else:
        logging.warning("预处理后无有效数据")
        processed_data = pd.DataFrame()
    
    return processed_data

# 4. 数据分析函数
def analyze_education_metrics(df):
    """
    分析教育指标
    """
    results = {}
    
    # 4.1 基本统计分析
    for column in df.select_dtypes(include=[np.number]).columns:
        if column != 'year':
            stats = df.groupby('year')[column].agg(['mean', 'std', 'min', 'max'])
            results[f'{column}_stats'] = stats
    
    # 4.2 趋势分析
    for column in df.select_dtypes(include=[np.number]).columns:
        if column != 'year':
            fig = px.line(df.groupby('year')[column].mean().reset_index(),
                         x='year', y=column,
                         title=f'{column} Trend Over Time')
            results[f'{column}_trend'] = fig
    
    # 4.3 国家间比较
    latest_year = df['year'].max()
    for column in df.select_dtypes(include=[np.number]).columns:
        if column != 'year':
            latest_data = df[df['year'] == latest_year].sort_values(column, ascending=False)
            fig = px.bar(latest_data.head(10),
                        x='country', y=column,
                        title=f'Top 10 Countries - {column} ({latest_year})')
            results[f'{column}_comparison'] = fig
    
    return results

# 5. 时间序列预测
def forecast_metrics(df, target_column, periods=5):
    """
    使用SARIMA模型进行时间序列预测
    """
    try:
        # 准备时间序列数据
        ts_data = df.groupby('year')[target_column].mean()
        
        # 拟合SARIMA模型
        model = SARIMAX(ts_data, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
        results = model.fit()
        
        # 预测
        forecast = results.forecast(periods)
        
        # 创建预测可视化
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ts_data.index, y=ts_data.values,
                                mode='lines', name='Historical Data'))
        fig.add_trace(go.Scatter(x=forecast.index, y=forecast.values,
                                mode='lines', name='Forecast',
                                line=dict(dash='dash')))
        fig.update_layout(title=f'{target_column} Forecast',
                         xaxis_title='Year',
                         yaxis_title=target_column)
        
        return {
            'forecast_data': forecast,
            'forecast_plot': fig,
            'model_summary': results.summary()
        }
    
    except Exception as e:
        logging.error(f"预测{target_column}时出错: {str(e)}")
        return None

# 6. 主函数
def main():
    """
    运行完整的数据分析流程
    """
    try:
        # 收集数据
        print("开始收集教育数据...")
        education_data = collect_education_data()
        
        # 预处理数据
        print("\n开始预处理数据...")
        processed_data = preprocess_data(education_data)
        
        if processed_data.empty:
            raise ValueError("没有可用的处理后数据")
        
        # 分析数据
        print("\n开始分析数据...")
        analysis_results = analyze_education_metrics(processed_data)
        
        # 预测
        print("\n开始预测分析...")
        forecast_results = {}
        for column in processed_data.select_dtypes(include=[np.number]).columns:
            if column != 'year':
                forecast_results[column] = forecast_metrics(processed_data, column)
        
        return {
            'processed_data': processed_data,
            'analysis_results': analysis_results,
            'forecast_results': forecast_results
        }
        
    except Exception as e:
        logging.error(f"分析过程中出错: {str(e)}")
        print(f"错误: {str(e)}")
        return None

if __name__ == "__main__":
    results = main()
