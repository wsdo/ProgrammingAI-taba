# %% [markdown]
# # 教育投资分析
# 
# 本笔记本分析欧盟国家的教育投资数据，包括：
# 1. 投资趋势分析
# 2. 经济指标相关性
# 3. 政策影响评估
# 4. 投资效率评估

# %%
# 导入所需库
import sys
import os
import json
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import matplotlib
from tqdm import tqdm
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# %%
# 设置日志记录
logging.basicConfig(
    filename='analysis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# %%
# 加载环境变量和设置路径
load_dotenv(Path('..').resolve() / '.env')
project_root = Path('..').resolve()
sys.path.append(str(project_root))

# %%
# 导入项目模块
from src.data_processing.db_manager import DatabaseManager
from src.data_processing.data_cleaner import DataCleaner
from src.data_collection.eurostat_collector import EurostatCollector

# %%
# 设置绘图风格
plt.style.use('seaborn-v0_8')
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 12
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

# %%
def check_data_quality(df, name):
    """检查数据质量并记录结果"""
    logging.info(f"开始检查{name}数据质量")
    results = {
        "总行数": len(df),
        "缺失值": df.isnull().sum().to_dict(),
        "数据类型": df.dtypes.to_dict()
    }
    logging.info(f"{name}数据质量检查结果: {results}")
    return results

# %%
def create_interactive_trend_plot(data, countries, country_names):
    """创建交互式趋势图"""
    fig = go.Figure()
    
    for country in countries:
        country_data = data[data['geo_time_period'] == country]
        if not country_data.empty:
            fig.add_trace(go.Scatter(
                x=country_data['year'],
                y=country_data['value'],
                name=country_names[country],
                mode='lines+markers'
            ))
    
    fig.update_layout(
        title='欧盟主要国家教育投资趋势 (2010-2020)',
        xaxis_title='年份',
        yaxis_title='投资值 (占GDP百分比)',
        hovermode='x unified'
    )
    
    return fig

# %%
def analyze_policy_impact(policy_docs, education_data):
    """分析政策影响"""
    impact_results = []
    
    for doc in policy_docs:
        if 'year' in doc and 'country' in doc:
            country_data = education_data[
                education_data['geo_time_period'] == doc['country']
            ]
            
            if not country_data.empty:
                before_policy = country_data[country_data['year'] < doc['year']]['value'].mean()
                after_policy = country_data[country_data['year'] >= doc['year']]['value'].mean()
                change_pct = ((after_policy - before_policy) / before_policy) * 100
                
                impact_results.append({
                    'country': doc['country'],
                    'policy_year': doc['year'],
                    'before_avg': before_policy,
                    'after_avg': after_policy,
                    'change_pct': change_pct
                })
    
    return pd.DataFrame(impact_results)

# %%
# 初始化数据收集器和管理器
collector = EurostatCollector()
db_manager = DatabaseManager()
cleaner = DataCleaner()

# %%
# 收集数据
print("步骤 1: 数据收集")
print("-" * 50)

with tqdm(total=3, desc="收集数据") as pbar:
    education_data_raw = collector.get_education_investment_data()
    pbar.update(1)
    
    economic_data_raw = collector.get_economic_indicators()
    pbar.update(1)
    
    policy_docs = collector.get_education_policies()
    pbar.update(1)

# %%
# 数据库操作
print("\n步骤 2: 数据存储")
print("-" * 50)

try:
    db_manager.connect_postgres()
    db_manager.connect_mongo()
    db_manager.create_tables()
    
    # 保存数据
    db_manager.insert_education_data(education_data_raw)
    db_manager.insert_economic_data(economic_data_raw)
    if db_manager.mongo_db is not None:
        db_manager.save_to_mongo('education_policies', policy_docs)
except Exception as e:
    logging.error(f"数据库操作失败: {str(e)}")
    raise

# %%
# 数据检索
print("\n步骤 3: 数据分析")
print("-" * 50)

education_data = db_manager.get_education_data()
economic_data = db_manager.get_economic_data()

# %%
# 数据质量检查
quality_results = {
    'education': check_data_quality(education_data, "教育投资数据"),
    'economic': check_data_quality(economic_data, "经济指标数据")
}

# %%
# 数据清理和合并
education_data_cleaned = cleaner.clean_education_data(education_data)

merged_data = pd.merge(
    education_data_cleaned,
    economic_data,
    left_on=['geo_time_period', 'year'],
    right_on=['country_code', 'year'],
    how='inner'
)

# %%
# 设置分析参数
major_countries = ['DE', 'FR', 'IT', 'ES', 'PL']
country_names = {
    'DE': '德国',
    'FR': '法国',
    'IT': '意大利',
    'ES': '西班牙',
    'PL': '波兰'
}

# %%
# 创建输出目录
output_dir = Path('output')
output_dir.mkdir(exist_ok=True)

# %%
# 1. 投资趋势分析
trend_fig = create_interactive_trend_plot(
    education_data_cleaned, 
    major_countries, 
    country_names
)
trend_fig.write_html(output_dir / 'investment_trends.html')

# %%
# 2. CAGR分析
cagr_results = {}
for country in major_countries:
    country_data = education_data_cleaned[
        education_data_cleaned['geo_time_period'] == country
    ]
    if len(country_data) >= 2:
        country_data = country_data.sort_values('year')
        first_year = country_data.iloc[0]
        last_year = country_data.iloc[-1]
        years = last_year['year'] - first_year['year']
        if years > 0:
            cagr = (((last_year['value'] / first_year['value']) ** (1/years)) - 1) * 100
            cagr_results[country] = cagr

# %%
# 3. 相关性分析
correlation_vars = ['value', 'gdp_growth', 'employment_rate', 'gdp_per_capita']
correlations = merged_data[correlation_vars].corr()

plt.figure(figsize=(10, 8))
sns.heatmap(correlations, annot=True, cmap='coolwarm', center=0)
plt.title('教育投资与经济指标的相关性')
plt.tight_layout()
plt.savefig(output_dir / 'correlation_heatmap.png', dpi=300, bbox_inches='tight')
plt.close()

# %%
# 4. 政策影响分析
if policy_docs:
    policy_impact = analyze_policy_impact(policy_docs, education_data_cleaned)
    policy_impact.to_csv(output_dir / 'policy_impact.csv', index=False)

# %%
# 5. 投资效率分析
if not merged_data.empty:
    merged_data['investment_efficiency'] = merged_data['gdp_per_capita'] / merged_data['value']
    latest_year = merged_data['year'].max()
    efficiency_fig = px.bar(
        merged_data[merged_data['year'] == latest_year].nlargest(5, 'investment_efficiency'),
        x='geo_time_period',
        y='investment_efficiency',
        title=f'各国投资效率对比 ({latest_year})'
    )
    efficiency_fig.write_html(output_dir / 'investment_efficiency.html')

# %%
# 导出分析结果
analysis_results = {
    'data_quality': quality_results,
    'cagr_analysis': cagr_results,
    'correlations': correlations.to_dict(),
    'investment_efficiency': merged_data['investment_efficiency'].describe().to_dict()
}

with open(output_dir / 'analysis_results.json', 'w', encoding='utf-8') as f:
    json.dump(analysis_results, f, ensure_ascii=False, indent=2)

logging.info("分析完成")
print("\n分析已完成！结果保存在 output 目录中。")

# %%
# 清理工作
print("\n步骤 4: 清理")
print("-" * 50)
db_manager.close_connections()
