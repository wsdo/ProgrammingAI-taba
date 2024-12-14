"""
执行教育数据分析的主程序
"""
import os
from education_analysis_segments import *
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def main():
    """主执行函数"""
    try:
        print("="*50)
        print("教育数据分析开始")
        print("="*50)

        # 1. 设置环境变量
        print("\n1. 配置环境变量...")
        os.environ.update({
            'POSTGRES_USER': 'nci',
            'POSTGRES_PASSWORD': 'yHyULpyUXZ4y32gdEi80',
            'POSTGRES_HOST': '47.91.31.227',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'education_db',
            'MONGODB_HOST': '47.91.31.227',
            'MONGODB_PORT': '27017',
            'MONGODB_USER': 'nci',
            'MONGODB_PASSWORD': 'xJcTB7fnyA17GNuQk3Aa',
            'MONGODB_DB': 'education_db'
        })

        # 2. 数据库连接
        print("\n2. 连接数据库...")
        pg_conn = get_postgres_connection()
        mongo_db = get_mongodb_connection()

        if pg_conn is None or mongo_db is None:
            raise Exception("数据库连接失败")

        # 3. 数据收集和分析
        print("\n3. 开始数据收集和分析...")
        
        # 3.1 定义分析参数
        metrics = ['education_investment', 'student_teacher_ratio', 'completion_rate']
        eu_countries = ['DE', 'FR', 'IT', 'ES', 'NL']
        year_range = (2010, 2023)

        # 3.2 收集和分析每个指标
        for metric in metrics:
            print(f"\n{'='*20} {metric} 分析 {'='*20}")
            
            # 收集数据
            print(f"\n3.2.1 收集 {metric} 数据...")
            df = collect_eurostat_data(metric)
            
            if df is not None:
                # 基本统计
                print("\n3.2.2 基本统计分析:")
                print("\n数据样本:")
                print(df.head())
                print("\n描述性统计:")
                print(df['value'].describe())
                
                # 数据分布
                print("\n3.2.3 按国家分组统计:")
                country_stats = df.groupby('country')['value'].agg(['mean', 'min', 'max'])
                print(country_stats)
                
                # 绘制趋势图
                print("\n3.2.4 生成趋势图...")
                plot_metric_trends(df, metric)

        # 4. 国家详细分析
        print("\n4. 开始国家详细分析...")
        
        for country in eu_countries:
            print(f"\n{'='*20} {country} 国家分析 {'='*20}")
            
            # 4.1 综合指标分析
            print("\n4.1 综合指标分析:")
            metrics_analysis = analyze_education_metrics(mongo_db, country, year_range)
            
            if metrics_analysis:
                for metric, stats in metrics_analysis.items():
                    print(f"\n{metric} 统计:")
                    print(f"平均值: {stats['mean']:.2f}")
                    print(f"中位数: {stats['median']:.2f}")
                    print(f"标准差: {stats['std']:.2f}")
                    if 'trend' in stats:
                        print(f"趋势斜率: {stats['trend']['slope']:.4f}")
                    if 'avg_yoy_change' in stats:
                        print(f"年均变化率: {stats['avg_yoy_change']*100:.2f}%")

            # 4.2 预测分析
            print("\n4.2 预测分析:")
            for metric in metrics:
                forecast = generate_forecasts(mongo_db, metric, country)
                if forecast:
                    print(f"\n{metric} 未来5年预测:")
                    for year, value in zip(forecast['years'], forecast['values']):
                        print(f"{year}年: {value:.2f}")
                    plot_forecast(forecast, metric, country)

        # 5. 国家间比较
        print("\n5. 国家间比较分析...")
        
        for metric in metrics:
            print(f"\n{'='*20} {metric} 国家间比较 {'='*20}")
            comparison = compare_countries(mongo_db, eu_countries, metric, year_range)
            
            if comparison:
                print("\n5.1 统计比较:")
                comparison_df = pd.DataFrame.from_dict(comparison, orient='index')
                print(comparison_df)
                
                print("\n5.2 生成比较图表...")
                plot_country_comparison(comparison, metric)

        # 6. 总结报告
        print("\n6. 分析总结")
        print("="*50)
        print("6.1 主要发现:")
        print("- 已完成所有教育指标的数据收集和分析")
        print("- 生成了各国教育指标的统计分析")
        print("- 完成了时间序列预测")
        print("- 进行了国家间的比较分析")
        
        print("\n6.2 数据覆盖:")
        print(f"- 分析的国家: {', '.join(eu_countries)}")
        print(f"- 分析的指标: {', '.join(metrics)}")
        print(f"- 时间范围: {year_range[0]}-{year_range[1]}")

    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    
    finally:
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.close()
            print("\n数据库连接已关闭")

if __name__ == "__main__":
    main()
