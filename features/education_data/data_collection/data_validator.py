import pandas as pd
import logging
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        self.validation_rules = {
            'education_expenditure': {
                'total_gdp_percentage': (0, 15),
                'per_student_primary': (1000, 20000),
                'per_student_secondary': (1000, 25000),
                'per_student_tertiary': (1000, 30000),
                'research_development': (0, 5)
            },
            'education_resources': {
                'student_teacher_ratio_primary': (5, 30),
                'student_teacher_ratio_secondary': (5, 25),
                'teacher_salary_index': (50, 200),
                'digital_resources_investment': (0, 2000)
            },
            'education_participation': {
                'enrollment_rate_primary': (80, 100),
                'enrollment_rate_secondary': (70, 100),
                'enrollment_rate_tertiary': (20, 100),
                'dropout_rate': (0, 30)
            },
            'education_quality': {
                'teacher_qualification_index': (0, 1),
                'average_class_size': (10, 40),
                'teaching_hours_yearly': (500, 1200),
                'digital_learning_percentage': (0, 100)
            }
        }

    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """
        验证单条数据记录
        :param record: 数据记录
        :return: 错误信息列表
        """
        errors = []
        
        # 验证必要字段
        required_fields = ['country', 'year', 'education_expenditure', 
                         'education_resources', 'education_participation', 
                         'education_quality']
        
        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")
                continue
        
        # 验证数值范围
        for category, rules in self.validation_rules.items():
            if category in record:
                for field, (min_val, max_val) in rules.items():
                    if field in record[category]:
                        value = record[category][field]
                        if not isinstance(value, (int, float)):
                            errors.append(f"Invalid type for {category}.{field}: expected number")
                        elif not min_val <= value <= max_val:
                            errors.append(
                                f"Value out of range for {category}.{field}: "
                                f"{value} (expected between {min_val} and {max_val})"
                            )
        
        return errors

    def validate_dataset(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        验证整个数据集
        :param data: 数据集列表
        :return: 验证结果统计
        """
        total_records = len(data)
        records_with_errors = 0
        all_errors = []
        
        for record in data:
            errors = self.validate_record(record)
            if errors:
                records_with_errors += 1
                all_errors.extend(errors)
        
        return {
            'total_records': total_records,
            'valid_records': total_records - records_with_errors,
            'records_with_errors': records_with_errors,
            'error_rate': round(records_with_errors / total_records * 100, 2) if total_records > 0 else 0,
            'total_errors': len(all_errors),
            'error_details': all_errors[:10] if all_errors else []  # 只显示前10个错误
        }

    def generate_validation_report(self, data: List[Dict[str, Any]], output_file: str = 'validation_report.txt'):
        """
        生成验证报告
        :param data: 数据集
        :param output_file: 输出文件名
        """
        validation_results = self.validate_dataset(data)
        
        with open(output_file, 'w') as f:
            f.write("Education Data Validation Report\n")
            f.write("===============================\n\n")
            
            f.write(f"Total Records: {validation_results['total_records']}\n")
            f.write(f"Valid Records: {validation_results['valid_records']}\n")
            f.write(f"Records with Errors: {validation_results['records_with_errors']}\n")
            f.write(f"Error Rate: {validation_results['error_rate']}%\n")
            f.write(f"Total Errors: {validation_results['total_errors']}\n\n")
            
            if validation_results['error_details']:
                f.write("Sample Error Details:\n")
                for error in validation_results['error_details']:
                    f.write(f"- {error}\n")
        
        logger.info(f"Validation report generated: {output_file}")
        return validation_results
