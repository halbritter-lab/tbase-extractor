"""Utility functions for tbase-extractor"""
import os
import sys
import csv
import logging
import importlib.resources as resources
from pathlib import Path
from typing import Optional, List, Dict, Any

def resolve_templates_dir() -> str:
    """
    Resolves the path to the SQL templates directory robustly.
    Handles both development and installed package scenarios.
    
    Returns:
        str: Absolute path to the sql_templates directory
        
    Raises:
        RuntimeError: If the sql_templates directory cannot be found
    """
    print("[DEBUG utils] Attempting to resolve templates directory...", file=sys.stderr)
    
    # Strategy 1: Try importlib.resources (works for installed package)
    try:
        print("[DEBUG utils] Strategy 1: Using importlib.resources...", file=sys.stderr)
        templates = resources.files('tbase_extractor.sql_templates')
        if templates and hasattr(templates, 'is_dir') and templates.is_dir():
            # Convert to string path that can be used with os.path functions
            templates_str = str(templates)
            # Verify the path actually exists and is a directory
            if os.path.isdir(templates_str):
                print(f"[DEBUG utils] Found templates via resources: {templates_str}", file=sys.stderr)
                return templates_str
            else:
                print(f"[DEBUG utils] Resources path exists but is not a directory: {templates_str}", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG utils] resources.files() failed: {e}", file=sys.stderr)

    # Strategy 2: Try relative to this file (development mode)
    try:
        print("[DEBUG utils] Strategy 2: Checking relative to utils.py...", file=sys.stderr)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        dev_path = os.path.join(current_dir, "sql_templates")
        if os.path.isdir(dev_path):
            print(f"[DEBUG utils] Found templates dir: {dev_path}", file=sys.stderr)
            return dev_path
        else:
            print(f"[DEBUG utils] Development path not found: {dev_path}", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG utils] Development path check failed: {e}", file=sys.stderr)

    # Strategy 3: Try relative to project root (if running from repo root)
    try:
        print("[DEBUG utils] Strategy 3: Checking project root...", file=sys.stderr)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        root_path = os.path.join(project_root, "sql_templates")
        if os.path.isdir(root_path):
            print(f"[DEBUG utils] Found templates in project root: {root_path}", file=sys.stderr)
            return root_path
        else:
            print(f"[DEBUG utils] Project root path not found: {root_path}", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG utils] Project root check failed: {e}", file=sys.stderr)

    error_msg = "Could not locate sql_templates directory after trying:\n"
    error_msg += "1. Package resources (installed package)\n"
    error_msg += "2. Development path (relative to utils.py)\n"
    error_msg += "3. Project root path\n\n"
    error_msg += "Please ensure sql_templates directory exists and is readable."
    raise RuntimeError(error_msg)
    
def read_ids_from_csv(csv_file_path: str, id_column_name: str, logger: logging.Logger) -> List[str]:
    """
    Reads a list of IDs from a specified column in a CSV file.
    
    The CSV file must have a header row with a column containing the IDs.
    By default, this column is expected to be named 'PatientID', but this
    can be customized using the id_column_name parameter.
    
    The function handles various error conditions gracefully, including:
    - Missing CSV file
    - Improperly formatted CSV
    - Missing column header
    - Empty or invalid ID values
    
    Args:
        csv_file_path (str): Path to the CSV file containing IDs
        id_column_name (str): Name of the column containing the IDs
        logger (logging.Logger): Logger for error reporting
        
    Returns:
        List[str]: List of IDs extracted from the CSV file
    """
    ids = []
    if not os.path.exists(csv_file_path):
        logger.error(f"CSV file not found: {csv_file_path}")
        return ids
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig', newline='') as infile:  # utf-8-sig for BOM
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                logger.error(f"CSV file '{csv_file_path}' appears to be empty or improperly formatted.")
                return ids
                
            if id_column_name not in reader.fieldnames:
                logger.error(f"ID column '{id_column_name}' not found in CSV header. Available columns: {reader.fieldnames}")
                return ids
            
            for row_num, row in enumerate(reader, 1):
                patient_id_str = row.get(id_column_name)
                if patient_id_str and patient_id_str.strip():
                    ids.append(patient_id_str.strip())
                else:
                    logger.warning(f"Missing or empty ID in CSV file '{csv_file_path}' at row {row_num}.")
    except csv.Error as e:
        logger.error(f"Error reading CSV file '{csv_file_path}': {e}")
        return []  # Return empty list on CSV error
    except IOError as e:
        logger.error(f"IOError reading CSV file '{csv_file_path}': {e}")
        return []
    
    if not ids:
        logger.warning(f"No IDs extracted from CSV file '{csv_file_path}' with ID column '{id_column_name}'.")
    else:
        logger.info(f"Successfully extracted {len(ids)} IDs from '{csv_file_path}'.")
    
    return ids

def read_patient_data_from_csv(csv_file_path: str, fn_column: str, ln_column: str, dob_column: str, logger: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
    """
    Read patient demographic data from a CSV file.
    
    Args:
        csv_file_path (str): Path to the CSV file
        fn_column (str): Column name for first name
        ln_column (str): Column name for last name
        dob_column (str): Column name for date of birth
        logger (Optional[logging.Logger]): Logger for error reporting
        
    Returns:
        List[Dict[str, Any]]: List of dictionaries containing patient data with row numbers
    """
    patients_data = []
    
    if not os.path.exists(csv_file_path):
        if logger:
            logger.error(f"CSV file not found: {csv_file_path}")
        return patients_data
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            
            # Validate required columns exist
            headers = reader.fieldnames
            if not headers:
                if logger:
                    logger.error("CSV file appears to be empty")
                return patients_data
            
            required_columns = {fn_column, ln_column, dob_column}
            missing_columns = required_columns - set(headers)
            if missing_columns:
                if logger:
                    logger.error(f"Missing required columns in CSV: {', '.join(missing_columns)}")
                return patients_data
            
            for row_num, row in enumerate(reader, start=1):
                # Extract relevant fields and clean data
                patient_data = {
                    "first_name": row[fn_column].strip(),
                    "last_name": row[ln_column].strip(),
                    "date_of_birth": row[dob_column].strip(),
                    "_row_number": row_num,  # Store row number for traceability
                    "_raw_data": dict(row)  # Store complete row data
                }
                patients_data.append(patient_data)
                
    except Exception as e:
        if logger:
            logger.error(f"Error reading CSV file {csv_file_path}: {str(e)}")
        raise
    
    return patients_data
