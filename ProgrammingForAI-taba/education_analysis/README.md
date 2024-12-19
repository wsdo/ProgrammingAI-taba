# Education Investment and Economic Growth Analysis

## Project Overview
This project analyzes the relationship between education investment and economic growth across different countries. We collect and analyze education spending data and related economic indicators to understand their correlation and potential causal relationships.

## Research Question
Does education investment significantly promote economic growth?

## Data Sources
1. Structured Data:
   - Eurostat Education Database
     * Education expenditure (educ_uoe_fina01)
     * Teaching staff data (educ_uoe_perp01)
     * Student enrollment (educ_uoe_enrt01)
   - World Bank & IMF Economic Data
     * GDP growth rates
     * Employment rates

2. Unstructured Data:
   - EU Education Policy Documents
   - Economic Development Reports

## Project Structure
```
education_analysis/
├── data/                      # Data storage
│   ├── raw/                   # Raw data files
│   └── processed/             # Processed data files
│
├── notebooks/                 # Jupyter notebooks
│   ├── data_collection.ipynb  # Data collection process
│   ├── data_analysis.ipynb    # Data analysis process
│   └── visualization.ipynb    # Data visualization
│
├── src/                       # Source code
│   ├── data_collection/       # Data collection modules
│   ├── data_processing/       # Data processing modules
│   ├── analysis/             # Analysis modules
│   └── visualization/        # Visualization modules
│
├── tests/                    # Test cases
├── docs/                     # Documentation
├── requirements.txt          # Project dependencies
└── README.md                # Project documentation
```

## Setup and Installation
1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Update database credentials and API keys

## Usage
1. Data Collection:
   - Run notebooks in `notebooks/data_collection.ipynb`
   - Check collected data in `data/raw/`

2. Data Analysis:
   - Execute analysis notebooks in `notebooks/`
   - View results in `data/processed/`

## Documentation
- Detailed analysis process in notebooks
- API documentation in `docs/`
- Data dictionary in `docs/data_dictionary.md`

## Contributing
This project is part of an academic research study. Please contact the authors for any contributions or questions.

## License
This project is for academic purposes only. All rights reserved.
