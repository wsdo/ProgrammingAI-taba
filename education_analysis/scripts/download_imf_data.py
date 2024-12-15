import pandas as pd
import requests
import logging
from pathlib import Path
import os
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def download_imf_data():
    """
    从IMF数据库下载经济发展数据
    """
    # 创建数据目录
    data_dir = Path(__file__).parent.parent / 'data' / 'raw'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 目标国家
    countries = ['DEU', 'FRA', 'ITA', 'ESP', 'POL']  # ISO 3字符代码
    
    try:
        # 下载GDP增长率数据
        gdp_data = []
        for country in countries:
            url = f"https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH?periods=2010:2023&iso={country}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                country_data = data['values'][country]
                for year, value in country_data.items():
                    gdp_data.append({
                        'country': country,
                        'year': int(year),
                        'gdp_growth': value
                    })
            else:
                logging.error(f"Failed to fetch GDP data for {country}")
        
        gdp_df = pd.DataFrame(gdp_data)
        
        # 下载就业率数据
        employment_data = []
        for country in countries:
            url = f"https://www.imf.org/external/datamapper/api/v1/LUR?periods=2010:2023&iso={country}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                country_data = data['values'][country]
                for year, value in country_data.items():
                    employment_data.append({
                        'country': country,
                        'year': int(year),
                        'employment_rate': 100 - value  # 将失业率转换为就业率
                    })
            else:
                logging.error(f"Failed to fetch employment data for {country}")
        
        employment_df = pd.DataFrame(employment_data)
        
        # 合并数据
        economic_df = pd.merge(gdp_df, employment_df, on=['country', 'year'], how='outer')
        
        # 保存数据
        timestamp = datetime.now().strftime('%Y%m%d')
        output_file = data_dir / f'imf_economic_data_{timestamp}.csv'
        economic_df.to_csv(output_file, index=False)
        logging.info(f"Data saved to {output_file}")
        
        return economic_df
        
    except Exception as e:
        logging.error(f"Error downloading IMF data: {str(e)}")
        return None

if __name__ == "__main__":
    download_imf_data()
