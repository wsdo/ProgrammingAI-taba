"""
数据清洗工具模块
负责数据预处理、清洗和标准化
"""

import logging
import numpy as np
import pandas as pd
from typing import Union, Optional
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCleaner:
    """数据清洗类"""
    
    def __init__(self):
        """初始化数据清洗器"""
        self.scaler = StandardScaler()
        
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗数据
        
        Args:
            df: 输入数据框
            
        Returns:
            清洗后的数据框
        """
        try:
            # 1. 处理缺失值
            df = self._handle_missing_values(df)
            
            # 2. 处理异常值
            df = self._handle_outliers(df)
            
            # 3. 数据格式统一化
            df = self._standardize_formats(df)
            
            return df
            
        except Exception as e:
            logger.error(f"数据清洗过程中发生错误: {str(e)}")
            return df
    
    def standardize_data(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """标准化数据
        
        Args:
            df: 输入数据框
            columns: 需要标准化的列
            
        Returns:
            标准化后的数据框
        """
        try:
            df_copy = df.copy()
            df_copy[columns] = self.scaler.fit_transform(df_copy[columns])
            return df_copy
            
        except Exception as e:
            logger.error(f"数据标准化过程中发生错误: {str(e)}")
            return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理缺失值
        
        策略：
        1. 对于时间序列数据，使用前向/后向填充
        2. 对于数值型数据，使用中位数填充
        3. 对于分类数据，使用众数填充
        """
        try:
            # 数值型列使用中位数填充
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
            
            # 分类列使用众数填充
            categorical_columns = df.select_dtypes(exclude=[np.number]).columns
            df[categorical_columns] = df[categorical_columns].fillna(df[categorical_columns].mode().iloc[0])
            
            return df
            
        except Exception as e:
            logger.error(f"处理缺失值时发生错误: {str(e)}")
            return df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理异常值
        
        使用IQR方法检测和处理异常值
        """
        try:
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            
            for column in numeric_columns:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # 将异常值替换为边界值
                df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
            
            return df
            
        except Exception as e:
            logger.error(f"处理异常值时发生错误: {str(e)}")
            return df
    
    def _standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """统一数据格式
        
        1. 统一日期格式
        2. 统一数值精度
        3. 统一字符串格式
        """
        try:
            # 处理日期列
            date_columns = df.select_dtypes(include=['datetime64']).columns
            for col in date_columns:
                df[col] = pd.to_datetime(df[col])
            
            # 处理数值列（统一精度为4位小数）
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].round(4)
            
            return df
            
        except Exception as e:
            logger.error(f"统一数据格式时发生错误: {str(e)}")
            return df
