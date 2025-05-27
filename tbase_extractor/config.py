"""Configuration constants and settings for tbase_extractor."""
import os
from typing import Dict, List

# Application constants
APP_VERSION = "0.1.0"
DOB_FORMAT = "%Y-%m-%d"
DEFAULT_ID_COLUMN = "PatientID"
DEFAULT_FN_COLUMN = "FirstName"
DEFAULT_LN_COLUMN = "LastName"
DEFAULT_DOB_COLUMN = "DOB"

# Logging configuration
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
LOGGER_NAME = "tbase_extractor.main"

# File handling
DEFAULT_FILE_ENCODING = 'utf-8'
VALID_OUTPUT_FORMATS = ['json', 'csv', 'tsv', 'txt', 'stdout']
FILE_EXTENSION_MAP = {
    '.json': 'json',
    '.csv': 'csv',
    '.tsv': 'tsv',
    '.txt': 'txt'
}

# Database configuration defaults
DEFAULT_SQL_DRIVER = "{SQL Server Native Client 10.0}"

# Metadata parameter keys (for consistency)
METADATA_PARAM_KEYS = [
    'first_name', 'last_name', 'dob', 'patient_id', 'query_name', 
    'table_name', 'table_schema', 'input_csv', 'id_column'
]

# Status constants
STATUS_SUCCESS = "success"
STATUS_SUCCESS_NO_DATA = "success_no_data"
STATUS_BATCH_SUCCESS_ALL = "batch_success_all_processed"
STATUS_BATCH_PARTIAL_SUCCESS = "batch_partial_success"
STATUS_BATCH_NO_DATA = "batch_processed_no_data_or_all_failed"
STATUS_BATCH_INPUT_ERROR = "batch_input_error_or_empty"

# Filename sanitization
VALID_FILENAME_CHARS = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_')

def get_env_or_default(key: str, default: str = "") -> str:
    """Get environment variable with optional default."""
    return os.getenv(key, default)

def sanitize_filename(filename: str) -> str:
    """Sanitize filename by keeping only alphanumeric chars, dashes, and underscores."""
    return ''.join(c for c in filename if c in VALID_FILENAME_CHARS)
