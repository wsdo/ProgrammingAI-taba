"""
MongoDB Schema and Data Models
"""

from typing import Dict, List
from datetime import datetime

# Example document structures for MongoDB collections

COUNTRY_DOCUMENT = {
    "_id": "DE",  # country_code as _id
    "name": "Germany",
    "region": "Western Europe",
    "metadata": {
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
}

ECONOMIC_DATA_DOCUMENT = {
    "_id": "DE_2023",  # Composite key: country_code + year
    "country_code": "DE",
    "year": 2023,
    "gdp": {
        "Q1": {
            "value": 765189.78,
            "date": "2023-01-01",
            "metadata": {
                "unit": "Million EUR",
                "source": "IMF",
                "collected_at": datetime.utcnow()
            }
        },
        "Q2": {
            "value": 766890.42,
            "date": "2023-04-01",
            "metadata": {
                "unit": "Million EUR",
                "source": "IMF",
                "collected_at": datetime.utcnow()
            }
        }
    },
    "employment": {
        "Q1": {
            "value": 75.8,
            "date": "2023-01-01",
            "metadata": {
                "unit": "Percentage",
                "source": "IMF",
                "collected_at": datetime.utcnow()
            }
        },
        "Q2": {
            "value": 76.2,
            "date": "2023-04-01",
            "metadata": {
                "unit": "Percentage",
                "source": "IMF",
                "collected_at": datetime.utcnow()
            }
        }
    },
    "inflation": {
        "Q1": {
            "value": 2.1,
            "date": "2023-01-01",
            "metadata": {
                "unit": "Percentage",
                "source": "IMF",
                "collected_at": datetime.utcnow()
            }
        },
        "Q2": {
            "value": 1.9,
            "date": "2023-04-01",
            "metadata": {
                "unit": "Percentage",
                "source": "IMF",
                "collected_at": datetime.utcnow()
            }
        }
    },
    "metadata": {
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "source": "IMF",
        "status": "active"
    }
}

ANALYSIS_RESULTS_DOCUMENT = {
    "_id": "DE_2023_Q4_analysis",  # Composite key: country_code + year + quarter + type
    "country_code": "DE",
    "year": 2023,
    "quarter": 4,
    "type": "quarterly_analysis",
    "results": {
        "gdp_growth": 0.5,
        "employment_change": 0.4,
        "inflation_change": -0.2,
        "correlations": {
            "gdp_employment": 0.75,
            "gdp_inflation": -0.32,
            "employment_inflation": -0.45
        }
    },
    "metadata": {
        "created_at": datetime.utcnow(),
        "analysis_version": "1.0",
        "status": "final"
    }
}

# MongoDB indexes configuration
INDEXES = {
    "countries": [
        {"key": [("region", 1)]},
        {"key": [("name", 1)]}
    ],
    "economic_data": [
        {"key": [("country_code", 1), ("year", 1)]},
        {"key": [("year", 1)]},
        {"key": [("metadata.status", 1)]}
    ],
    "analysis_results": [
        {"key": [("country_code", 1), ("year", 1), ("quarter", 1)]},
        {"key": [("type", 1)]},
        {"key": [("metadata.status", 1)]}
    ]
}

# MongoDB collection configurations
COLLECTIONS_CONFIG = {
    "countries": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["_id", "name", "region"],
                "properties": {
                    "_id": {"bsonType": "string", "pattern": "^[A-Z]{2}$"},
                    "name": {"bsonType": "string"},
                    "region": {"bsonType": "string"}
                }
            }
        }
    },
    "economic_data": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["_id", "country_code", "year"],
                "properties": {
                    "_id": {"bsonType": "string"},
                    "country_code": {"bsonType": "string", "pattern": "^[A-Z]{2}$"},
                    "year": {"bsonType": "int"}
                }
            }
        }
    }
}
