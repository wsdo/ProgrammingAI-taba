import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        """Initialize the data preprocessor"""
        self.categorical_columns = ['country', 'quarter']
        self.numerical_columns = [
            'education_expenditure.total_gdp_percentage',
            'education_expenditure.per_student_primary',
            'education_expenditure.per_student_secondary',
            'education_expenditure.per_student_tertiary',
            'education_expenditure.research_development',
            'education_resources.student_teacher_ratio_primary',
            'education_resources.student_teacher_ratio_secondary',
            'education_resources.teacher_salary_index',
            'education_resources.digital_resources_investment',
            'education_participation.enrollment_rate_primary',
            'education_participation.enrollment_rate_secondary',
            'education_participation.enrollment_rate_tertiary',
            'education_participation.dropout_rate',
            'education_quality.teacher_qualification_index',
            'education_quality.average_class_size',
            'education_quality.teaching_hours_yearly',
            'education_quality.digital_learning_percentage'
        ]

    def flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def convert_to_dataframe(self, data: List[Dict]) -> pd.DataFrame:
        """Convert list of dictionaries to pandas DataFrame"""
        # Flatten nested dictionaries
        flattened_data = [self.flatten_dict(record) for record in data]
        return pd.DataFrame(flattened_data)

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset"""
        logger.info("Handling missing values...")
        
        # For numerical columns, fill missing values with median
        for col in self.numerical_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        
        # For categorical columns, fill missing values with mode
        for col in self.categorical_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].mode()[0])
        
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records"""
        logger.info("Removing duplicates...")
        initial_size = len(df)
        df = df.drop_duplicates(subset=['country', 'year', 'quarter'])
        removed = initial_size - len(df)
        logger.info(f"Removed {removed} duplicate records")
        return df

    def normalize_numerical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize numerical features to 0-1 range"""
        logger.info("Normalizing numerical features...")
        for col in self.numerical_columns:
            if col in df.columns:
                min_val = df[col].min()
                max_val = df[col].max()
                if max_val > min_val:
                    df[f"{col}_normalized"] = (df[col] - min_val) / (max_val - min_val)
        return df

    def add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features for analysis"""
        logger.info("Adding derived features...")
        
        # Add year-quarter combined field
        df['time_period'] = df['year'].astype(str) + '-' + df['quarter']
        
        # Calculate total investment per student
        if all(col in df.columns for col in ['education_expenditure.per_student_primary', 
                                           'education_expenditure.per_student_secondary',
                                           'education_expenditure.per_student_tertiary']):
            df['total_investment_per_student'] = df[[
                'education_expenditure.per_student_primary',
                'education_expenditure.per_student_secondary',
                'education_expenditure.per_student_tertiary'
            ]].mean(axis=1)
        
        # Calculate overall education quality score
        quality_columns = [col for col in df.columns if col.startswith('education_quality')]
        if quality_columns:
            df['overall_quality_score'] = df[quality_columns].mean(axis=1)
        
        return df

    def process_data(self, data: List[Dict]) -> pd.DataFrame:
        """Main method to process the data"""
        logger.info("Starting data preprocessing...")
        
        # Convert to DataFrame
        df = self.convert_to_dataframe(data)
        
        # Apply preprocessing steps
        df = self.handle_missing_values(df)
        df = self.remove_duplicates(df)
        df = self.normalize_numerical_features(df)
        df = self.add_derived_features(df)
        
        logger.info("Data preprocessing completed")
        return df

    def save_processed_data(self, df: pd.DataFrame, output_file: str):
        """Save processed data to file"""
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to {output_file}")

    def generate_preprocessing_report(self, df: pd.DataFrame, output_file: str = 'preprocessing_report.txt'):
        """Generate a report of the preprocessing steps and data statistics"""
        with open(output_file, 'w') as f:
            f.write("Education Data Preprocessing Report\n")
            f.write("=================================\n\n")
            
            # Basic statistics
            f.write(f"Dataset Statistics:\n")
            f.write(f"Total Records: {len(df)}\n")
            f.write(f"Time Period: {df['year'].min()} to {df['year'].max()}\n")
            f.write(f"Countries: {len(df['country'].unique())}\n\n")
            
            # Missing values summary
            f.write("Missing Values Summary:\n")
            missing_summary = df.isnull().sum()
            for col, count in missing_summary[missing_summary > 0].items():
                f.write(f"- {col}: {count} missing values\n")
            f.write("\n")
            
            # Data ranges
            f.write("Data Ranges for Key Metrics:\n")
            for col in self.numerical_columns:
                if col in df.columns:
                    f.write(f"- {col}:\n")
                    f.write(f"  Min: {df[col].min():.2f}\n")
                    f.write(f"  Max: {df[col].max():.2f}\n")
                    f.write(f"  Mean: {df[col].mean():.2f}\n")
                    f.write(f"  Std: {df[col].std():.2f}\n\n")
            
        logger.info(f"Preprocessing report generated: {output_file}")
