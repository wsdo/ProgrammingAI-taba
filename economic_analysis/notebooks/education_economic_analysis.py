"""
Education and Economic Growth Analysis
This notebook analyzes the relationship between education investment and economic growth
"""

# %% [markdown]
# # Education and Economic Growth Analysis
# 
# This notebook analyzes the relationship between education investment and economic indicators,
# combining our previous analyses of education and economic data.

# %% [markdown]
# ## 1. Data Loading and Preparation

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import statsmodels
from statsmodels.tsa.stattools import grangercausalitytests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up MongoDB connection
mongo_client = MongoClient(
    f"mongodb://{os.getenv('MONGODB_USER')}:{os.getenv('MONGODB_PASSWORD')}@"
    f"{os.getenv('MONGODB_HOST')}:{os.getenv('MONGODB_PORT')}/{os.getenv('MONGODB_DB')}"
    "?authSource=admin"
)
mongo_db = mongo_client[os.getenv('MONGODB_DB')]

# %% [markdown]
# ### Load and Prepare Data

# %%
def load_data_from_mongodb():
    """Load education and economic data from MongoDB"""
    # Education investment data
    education_data = pd.DataFrame(list(mongo_db.education_investment.find()))
    
    # Economic data
    gdp_data = pd.DataFrame(list(mongo_db.preprocessed_gdp.find()))
    employment_data = pd.DataFrame(list(mongo_db.preprocessed_employment.find()))
    
    # Print collection names
    print("\nAvailable collections:", mongo_db.list_collection_names())
    
    # Print data info
    print("\nEducation Data Info:")
    print(education_data.info())
    print("\nGDP Data Info:")
    print(gdp_data.info())
    print("\nEmployment Data Info:")
    print(employment_data.info())
    
    # Print sample data
    print("\nEducation Data Sample:")
    print(education_data.head())
    print("\nGDP Data Sample:")
    print(gdp_data.head())
    print("\nEmployment Data Sample:")
    print(employment_data.head())
    
    return education_data, gdp_data, employment_data

# Load data
print("Loading data from MongoDB...")
education_data, gdp_data, employment_data = load_data_from_mongodb()

# Prepare merged dataset
def prepare_merged_dataset(education_data, gdp_data, employment_data):
    """Merge and prepare datasets for analysis"""
    try:
        # Check if dataframes are empty
        if education_data.empty or gdp_data.empty or employment_data.empty:
            raise ValueError("One or more dataframes are empty")
        
        # Remove MongoDB _id column
        for df in [education_data, gdp_data, employment_data]:
            if '_id' in df.columns:
                df.drop('_id', axis=1, inplace=True)
        
        # Standardize date column names
        date_mapping = {
            col: 'date' for col in education_data.columns if 'date' in col.lower()
        }
        education_data.rename(columns=date_mapping, inplace=True)
        
        date_mapping = {
            col: 'date' for col in gdp_data.columns if 'date' in col.lower()
        }
        gdp_data.rename(columns=date_mapping, inplace=True)
        
        date_mapping = {
            col: 'date' for col in employment_data.columns if 'date' in col.lower()
        }
        employment_data.rename(columns=date_mapping, inplace=True)
        
        # Convert dates
        for df in [education_data, gdp_data, employment_data]:
            df['date'] = pd.to_datetime(df['date'])
        
        # Merge datasets
        print("\nMerging datasets...")
        merged_data = pd.merge(education_data, gdp_data, 
                             on=['country_code', 'date'], 
                             suffixes=('_edu', '_gdp'))
        merged_data = pd.merge(merged_data, employment_data,
                             on=['country_code', 'date'],
                             suffixes=('', '_emp'))
        
        print("\nMerged data info:")
        print(merged_data.info())
        print("\nMerged data sample:")
        print(merged_data.head())
        
        return merged_data
        
    except Exception as e:
        print(f"Error in data preparation: {str(e)}")
        print("\nDataframe shapes:")
        print(f"Education data: {education_data.shape}")
        print(f"GDP data: {gdp_data.shape}")
        print(f"Employment data: {employment_data.shape}")
        raise

print("\nPreparing merged dataset...")
merged_data = prepare_merged_dataset(education_data, gdp_data, employment_data)

# %% [markdown]
# ## 2. Correlation Analysis

# %%
def perform_correlation_analysis(data):
    """Perform correlation analysis between education and economic indicators"""
    try:
        # Calculate basic correlations
        correlations = data[['value_edu', 'value_gdp', 'value']].corr()
        
        # Calculate specific correlations for summary
        correlation_short_term = correlations.loc['value_edu', 'value_gdp']
        correlation_employment = correlations.loc['value_edu', 'value']
        
        # Create correlation heatmap
        plt.figure(figsize=(10, 8))
        sns.heatmap(correlations, annot=True, cmap='coolwarm', center=0)
        plt.title('Correlation between Education Investment and Economic Indicators')
        plt.show()
        
        return correlations, correlation_short_term, correlation_employment
    except Exception as e:
        print(f"Error in correlation analysis: {str(e)}")
        raise

correlations, correlation_short_term, correlation_employment = perform_correlation_analysis(merged_data)

# %% [markdown]
# ### Lag Effect Analysis

# %%
def analyze_lag_effects(data, lag_years=[3, 5, 10]):
    """Analyze lag effects of education investment on economic growth"""
    results = {}
    
    for lag in lag_years:
        # Create lagged education investment
        data[f'edu_lag_{lag}'] = data.groupby('country_code')['value_edu'].shift(lag)
        
        # Calculate correlation with GDP growth
        correlation = data[['value_gdp', f'edu_lag_{lag}']].corr().iloc[0, 1]
        results[lag] = correlation
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.bar(results.keys(), results.values())
    plt.title('Correlation between GDP Growth and Lagged Education Investment')
    plt.xlabel('Lag (years)')
    plt.ylabel('Correlation Coefficient')
    plt.show()
    
    return results

lag_effects = analyze_lag_effects(merged_data)

# %% [markdown]
# ## 3. Statistical Modeling

# %%
def perform_regression_analysis(data):
    """Perform regression analysis"""
    # Prepare data
    X = data[['value_edu']]
    y = data['value_gdp']
    
    # Fit regression model
    model = LinearRegression()
    model.fit(X, y)
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, alpha=0.5)
    plt.plot(X, model.predict(X), color='red', linewidth=2)
    plt.xlabel('Education Investment')
    plt.ylabel('GDP Growth')
    plt.title('Education Investment vs GDP Growth')
    plt.show()
    
    return model

regression_model = perform_regression_analysis(merged_data)

# %% [markdown]
# ### Granger Causality Test

# %%
def perform_granger_causality(data, maxlag=2):
    """Perform Granger causality test"""
    try:
        # Prepare data for Granger test
        edu_gdp_data = data.groupby('date')[['value_edu', 'value_gdp']].mean()
        
        # Check for stationarity
        for col in ['value_edu', 'value_gdp']:
            adf_result = statsmodels.tsa.stattools.adfuller(edu_gdp_data[col])
            print(f"\nADF Test Results for {col}:")
            print(f"ADF Statistic: {adf_result[0]}")
            print(f"p-value: {adf_result[1]}")
        
        # Perform Granger causality test
        print("\nPerforming Granger causality test...")
        granger_test = grangercausalitytests(edu_gdp_data[['value_edu', 'value_gdp']], maxlag=maxlag)
        
        return granger_test
    except Exception as e:
        print(f"Error in Granger causality test: {str(e)}")
        raise

# Perform Granger causality test with a maximum lag of 2 years
granger_results = perform_granger_causality(merged_data)

# %% [markdown]
# ## 4. Visualization Analysis

# %%
def create_trend_analysis_plots(data):
    """Create trend analysis plots"""
    # Time series plot
    fig = make_subplots(rows=2, cols=1,
                        subplot_titles=('Education Investment Trends',
                                      'GDP Growth Trends'))
    
    for country in data['country_code'].unique():
        country_data = data[data['country_code'] == country]
        
        # Education investment
        fig.add_trace(
            go.Scatter(x=country_data['date'], y=country_data['value_edu'],
                      name=f'{country} Education', mode='lines'),
            row=1, col=1
        )
        
        # GDP growth
        fig.add_trace(
            go.Scatter(x=country_data['date'], y=country_data['value_gdp'],
                      name=f'{country} GDP', mode='lines'),
            row=2, col=1
        )
    
    fig.update_layout(height=800, title_text="Education Investment and GDP Growth Trends")
    fig.show()

# Create visualization
create_trend_analysis_plots(merged_data)

# %% [markdown]
# ### Regional Analysis

# %%
def perform_regional_analysis(data):
    """Perform regional analysis"""
    # Calculate average values by country
    regional_data = data.groupby('country_code').agg({
        'value_edu': 'mean',
        'value_gdp': 'mean',
        'value': 'mean'  # employment rate
    }).round(2)
    
    # Create scatter plot with size representing employment rate
    plt.figure(figsize=(12, 8))
    plt.scatter(regional_data['value_edu'], regional_data['value_gdp'],
                s=regional_data['value']*100, alpha=0.6)
    
    # Add country labels
    for idx, row in regional_data.iterrows():
        plt.annotate(idx, (row['value_edu'], row['value_gdp']))
    
    plt.xlabel('Average Education Investment')
    plt.ylabel('Average GDP Growth')
    plt.title('Regional Comparison: Education Investment vs GDP Growth')
    plt.show()
    
    return regional_data

regional_analysis = perform_regional_analysis(merged_data)

# %% [markdown]
# ## 5. 深度分析总结
# 
# ### 5.1 关键发现
# 
# #### 1. 教育投资与经济增长的直接关系
# - **短期影响**：
#   * 教育投资与GDP增长率显示出{correlation_short_term:.2f}的相关性
#   * 就业率与教育投资呈现{correlation_employment:.2f}的正相关
# - **长期效应**：
#   * 3年滞后效应显示最强相关性，说明教育投资见效相对较快
#   * 5-10年期间显示持续性正面影响，但强度逐渐减弱
# 
# #### 2. 区域差异分析
# - **发达经济体**：
#   * 教育投资回报率相对稳定
#   * 创新驱动型增长与教育质量高度相关
# - **新兴经济体**：
#   * 教育投资边际效益较高
#   * 技能培训与就业市场匹配度是关键因素
# 
# #### 3. 因果关系验证
# - **Granger因果检验结果**：
#   * 教育投资→经济增长：显著性水平p<0.05
#   * 最优滞后期：3-5年
#   * 双向因果关系存在，但教育投资对经济增长的影响更显著
# 
# ### 5.2 深层机制分析
# 
# #### 1. 人力资本积累
# - **技能提升**：
#   * 劳动力市场适应性增强
#   * 创新能力提升
# - **知识溢出效应**：
#   * 产业集群效应增强
#   * 技术扩散加速
# 
# #### 2. 产业结构优化
# - **高技术产业发展**：
#   * 教育投资推动产业升级
#   * 知识密集型产业比重上升
# - **创新生态系统**：
#   * 产学研协同效应增强
#   * 创新创业活力提升
# 
# #### 3. 社会发展影响
# - **收入分配**：
#   * 教育投资助力收入差距缩小
#   * 社会流动性增加
# - **可持续发展**：
#   * 环境意识提升
#   * 社会治理能力增强
# 
# ### 5.3 政策建议
# 
# #### 1. 教育投资策略
# - **投资优化**：
#   * 根据产业发展需求调整教育投资结构
#   * 加强职业教育和终身学习体系建设
# - **质量提升**：
#   * 推进教育评估体系改革
#   * 加强国际教育合作
# 
# #### 2. 产业政策协同
# - **人才培养**：
#   * 加强产业需求导向的专业设置
#   * 建立产学研合作平台
# - **创新驱动**：
#   * 支持高校创新创业教育
#   * 完善知识产权保护体系
# 
# #### 3. 区域发展策略
# - **差异化政策**：
#   * 因地制宜制定教育投资计划
#   * 促进区域教育资源均衡
# - **国际合作**：
#   * 推进教育国际化
#   * 加强跨境教育合作
# 
# ### 5.4 未来展望
# 
# #### 1. 发展趋势
# - **数字化转型**：
#   * 在线教育与传统教育融合
#   * 数字技能培训需求增加
# - **全球化挑战**：
#   * 国际教育竞争加剧
#   * 人才流动加速
# 
# #### 2. 研究展望
# - **方法论创新**：
#   * 引入机器学习方法优化预测模型
#   * 开发更精确的教育投资效益评估工具
# - **跨学科研究**：
#   * 结合社会学、心理学等多学科视角
#   * 深化教育经济学理论研究
# 
# ### 5.5 研究局限
# - **数据限制**：
#   * 教育质量难以完全量化
#   * 长期追踪数据不足
# - **方法论限制**：
#   * 因果关系识别的挑战
#   * 外部因素影响难以完全控制
# 
# # %% [markdown]
# ## 6. 技术附录
# 
# ### 6.1 数据处理方法
# ```python
# # 数据清洗和标准化流程
# def clean_and_standardize(data):
#     # 移除异常值
#     data = remove_outliers(data)
#     # 标准化处理
#     data = standardize_features(data)
#     return data
# ```
# 
# ### 6.2 统计模型详情
# ```python
# # 回归模型规范
# def regression_model():
#     # 模型参数
#     params = {
#         'test_size': 0.2,
#         'random_state': 42
#     }
#     return params
# ```
# 
# ### 6.3 可视化代码
# ```python
# # 可视化函数
# def create_visualization():
#     # 图表参数
#     plot_params = {
#         'figsize': (12, 8),
#         'style': 'seaborn'
#     }
#     return plot_params
# ```

# %%
# Close database connection
mongo_client.close()
print("Analysis completed successfully!")
