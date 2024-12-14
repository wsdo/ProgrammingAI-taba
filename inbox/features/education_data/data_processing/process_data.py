import json
import logging
import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from data_collection.data_validator import DataValidator
from data_processing.data_preprocessor import DataPreprocessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data(file_path: str) -> list:
    """Load data from JSON file"""
    with open(file_path, 'r') as f:
        return json.load(f)

def main():
    # Setup paths
    base_dir = Path(__file__).parent.parent
    input_file = base_dir / 'data_collection' / 'eu_education_data.json'
    output_dir = base_dir / 'processed_data'
    output_dir.mkdir(exist_ok=True)

    # Load data
    logger.info(f"Loading data from {input_file}")
    data = load_data(str(input_file))

    # Validate data
    logger.info("Starting data validation...")
    validator = DataValidator()
    validation_results = validator.generate_validation_report(
        data, 
        str(output_dir / 'validation_report.txt')
    )

    # Process data if validation is successful
    if validation_results['error_rate'] < 10:  # Allow up to 10% error rate
        logger.info("Starting data preprocessing...")
        preprocessor = DataPreprocessor()
        
        # Process data
        df = preprocessor.process_data(data)
        
        # Generate preprocessing report
        preprocessor.generate_preprocessing_report(
            df,
            str(output_dir / 'preprocessing_report.txt')
        )
        
        # Save processed data
        preprocessor.save_processed_data(
            df,
            str(output_dir / 'processed_education_data.csv')
        )
        
        logger.info("Data processing completed successfully")
    else:
        logger.error(f"Validation failed with error rate: {validation_results['error_rate']}%")
        logger.error("Data processing aborted")

if __name__ == "__main__":
    main()
