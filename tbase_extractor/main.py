"""Main module for the tbase_extractor package."""
import argparse
import os
import sys
import pyodbc
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple
from dotenv import load_dotenv
import logging
from .utils import resolve_templates_dir, read_ids_from_csv
from .sql_interface.db_interface import SQLInterface
from .sql_interface.query_manager import QueryManager, QueryTemplateNotFoundError
from .sql_interface.output_formatter import OutputFormatter
from .matching import FuzzyMatcher, PatientSearchStrategy
from .matching.models import MatchInfo, MatchCandidate
from .config import DOB_FORMAT, LOG_FORMAT, LOGGER_NAME, DEFAULT_FILE_ENCODING
from .output_handler import handle_output, determine_output_format
from .metadata import create_metadata_dict

load_dotenv()

def parse_dob_str(dob_str: Optional[str], logger: logging.Logger) -> Optional[date]:
    """Parses a DOB string and returns a date object or None if invalid."""
    if not dob_str:
        return None
    try:
        return datetime.strptime(dob_str, DOB_FORMAT).date()
    except ValueError:
        logger.error(f"Invalid Date of Birth format for '{dob_str}'. Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")
        raise ValueError(f"Invalid Date of Birth format: {dob_str}")



def setup_arg_parser():
    parser = argparse.ArgumentParser(
        description="Connects to a SQL database to execute predefined queries using templates.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        '--debug', '-v',
        action='store_true',
        help='Enable verbose debug output for troubleshooting.'
    )
    
    subparsers = parser.add_subparsers(
        dest='action',
        help='The main action to perform. Use one of the subcommands below.',
        required=True,
        metavar='ACTION'
    )

    # --- Sub-command: list-tables ---
    subparsers.add_parser('list-tables', help='List all available base tables in the database.')

    # --- Sub-command: query ---
    parser_query = subparsers.add_parser('query', help='Execute a predefined query template.')
    parser_query.add_argument('--query-name', '-q',
        required=True,
        choices=['get_patient_by_id', 'patient-by-name-dob', 'patient-fuzzy-search', 'get-table-columns', 'batch-search-demographics'],
        help="REQUIRED. The name of the predefined query template to execute.\n"
             "Must correspond to a file in the 'sql_templates' directory.\n"
             "Examples:\n"
             "  'get_patient_by_id': Get details for a specific patient by ID.\n"
             "  'patient-by-name-dob': Get patient details by Name and Date of Birth.\n"
             "  'patient-fuzzy-search': Search for patients with fuzzy name matching and year tolerance.\n"
             "  'batch-search-demographics': Batch search patients from CSV using EXACT Name and/or DOB matches.\n"
             "  'get-table-columns': Get column details for a specific table and schema."
    )
    
    parser_query.add_argument(
        '--patient-id', '-i', type=int, metavar='ID',
        help='Patient ID (required for the \"get_patient_by_id\" query unless --input-csv is used).'
    )
    
    parser_query.add_argument(
        '--input-csv', '-ic', type=str, metavar='CSV_FILE_PATH',
        help='Path to a CSV file containing patient identifiers for batch processing.'
    )
    
    parser_query.add_argument(
        '--id-column', '-idc', type=str, metavar='COLUMN_NAME', default='PatientID',
        help='The name of the column in the CSV file that contains the patient identifiers (default: "PatientID").'
    )
    
    # CSV column name arguments for batch demographic search
    parser_query.add_argument(
        '--fn-column', '-fnc', type=str, metavar='COLUMN_NAME', default='FirstName',
        help='Column name for First Name in CSV for batch-search-demographics (default: "FirstName").'
    )
    
    parser_query.add_argument(
        '--ln-column', '-lnc', type=str, metavar='COLUMN_NAME', default='LastName',
        help='Column name for Last Name in CSV for batch-search-demographics (default: "LastName").'
    )
    
    parser_query.add_argument(
        '--dob-column', '-dc', type=str, metavar='COLUMN_NAME', default='DOB',
        help='Column name for Date of Birth (YYYY-MM-DD) in CSV for batch-search-demographics (default: "DOB").'
    )
    
    parser_query.add_argument(
        '--first-name', '-fn', type=str, metavar='NAME',
        help='Patient First Name (Vorname) for exact or fuzzy matching.'
    )
    
    parser_query.add_argument(
        '--last-name', '-ln', type=str, metavar='NAME',
        help='Patient Last Name (Name) for exact or fuzzy matching.'
    )
    
    parser_query.add_argument(
        '--dob', '-d', type=str, metavar='YYYY-MM-DD',
        help='Patient Date of Birth (Geburtsdatum) in %%Y-%%m-%%d format.'
    )
    
    parser_query.add_argument(
        '--output', '-o', type=str, metavar='FILE_PATH',
        help='Optional path to save results as a JSON, CSV, or TSV file.'
    )
    
    # Fuzzy search specific arguments (for patient-fuzzy-search only)
    parser_query.add_argument(
        '--fuzzy-threshold', type=float, default=0.85, metavar='0.0-1.0',
        help='Similarity threshold for fuzzy name matching (0.0 to 1.0, default: 0.85) for "patient-fuzzy-search".'
    )
    
    parser_query.add_argument(
        '--dob-year-tolerance', type=int, default=1, metavar='YEARS',
        help='Maximum year difference allowed for DOB matching (default: 1) for "patient-fuzzy-search".'
    )
    
    parser_query.add_argument(
        '--min-match-score', type=float, default=0.3, metavar='SCORE',
        help='Minimum overall match score required (0.0 to 1.0, default: 0.3) for "patient-fuzzy-search".'
    )

    parser_query.add_argument(
        '--format', '-f',
        type=str,
        choices=['json', 'csv', 'tsv', 'stdout'],
        default=None,
        help='Output format: json, csv, tsv, or stdout (pretty table to console). Inferred from -o extension if not set.'
    )
    
    # New arguments for split file output
    parser_query.add_argument(
        '--split-output', '-so',
        action='store_true',
        help='Save the output as multiple files (one per row) instead of a single file.'
    )
    
    parser_query.add_argument(
        '--filename-template', '-ft',
        type=str,
        default="{PatientID}",
        help='Template for naming individual output files when using --split-output. Use field names in curly braces, e.g., "{PatientID}" or "{Vorname}_{Name}". Default: "{PatientID}"'
    )
    
    parser_query.add_argument('--table-name', '-tn',
        required=False,
        help="The name of the table to query (e.g., 'Patient').")
    
    parser_query.add_argument('--table-schema', '-ts',
        required=False,
        help="The schema of the table to query (e.g., 'dbo').")
    
    return parser



def setup_logging(debug: bool = False, log_file: str = None):
    log_level = logging.DEBUG if debug else logging.INFO
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding=DEFAULT_FILE_ENCODING))
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        handlers=handlers,
        force=True
    )

def main():
    # 1. Setup (templates_dir, parser, args, logging)
    try:
        templates_dir = resolve_templates_dir()
    except RuntimeError as e:
        # No logger yet, so print to stderr
        print(f"Critical Error: {e}", file=sys.stderr)
        sys.exit(1)

    parser = setup_arg_parser()
    args = parser.parse_args()

    debug = getattr(args, 'debug', False)
    log_file = os.getenv('SQL_APP_LOGFILE', None)
    setup_logging(debug, log_file)  # Ensure logger is configured
    logger = logging.getLogger(LOGGER_NAME)  # Use the configured logger name

    logger.debug(f"Parsed arguments: {args}")

    query_manager = QueryManager(templates_dir, debug=debug)  # Pass debug to QM
    results = None
    query_display_name = args.action  # Default display name

    # 2. Database Interaction and Action Handling
    try:
        with SQLInterface(debug=debug) as db:  # Pass debug to SQLInterface
            if not db.connection:
                logger.error("Aborting: Database connection failed.")
                sys.exit(1)

            # Record query start time for metadata
            query_start_time = datetime.utcnow()

            if args.action == 'list-tables':
                handler = ACTION_HANDLERS.get('list-tables')
                if handler:
                    results, query_display_name = handler(args, query_manager, db, logger)
                else:  # Should not happen if parser is set up correctly
                    logger.error(f"No handler for action: {args.action}")
                    sys.exit(1)
            
            elif args.action == 'query':
                query_handler_map = ACTION_HANDLERS.get('query', {})
                handler = query_handler_map.get(args.query_name)
                if handler:
                    # Pass parser to handlers that might call parser.error()
                    results, query_display_name = handler(args, query_manager, db, logger, parser)
                else:
                    logger.error(f"Query name '{args.query_name}' is not recognized or implemented.")
                    sys.exit(1)
            else:  # Should not happen due to argparse
                logger.critical(f"Unknown action: {args.action}")
                sys.exit(1)

            # Calculate execution time for metadata
            execution_duration_ms = int((datetime.utcnow() - query_start_time).total_seconds() * 1000)

    except QueryTemplateNotFoundError as e:
        logger.error(f"Query Template Error: {e}", exc_info=debug)
        sys.exit(1)
    except pyodbc.Error as db_err:
        logger.exception(f"A database error occurred: {db_err}")
        sys.exit(1)
    except RuntimeError as e:  # Catch errors raised by handlers
        logger.error(f"Runtime error during execution: {e}", exc_info=debug)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)    # 3. Output Handling
    if results is not None:  # results can be an empty list
        output_file_path = getattr(args, 'output', None)
        user_format_arg = getattr(args, 'format', None)
        
        # Use the new utility function
        effective_format = determine_output_format(user_format_arg, output_file_path)
        
        # Create metadata using the new utility function
        metadata_dict = create_metadata_dict(
            query_start_time, execution_duration_ms, args, 
            query_display_name, results
        )
        
        # Get split output parameters
        split_output = getattr(args, 'split_output', False)
        filename_template = getattr(args, 'filename_template', "{PatientID}")
        
        # Call handle_output with the new parameters
        handle_output(results, output_file_path, query_display_name, effective_format, 
                     metadata_dict, split_output, filename_template)
    
    logger.info(f"--- {query_display_name} finished ---")


if __name__ == "__main__":
    main()

def handle_list_tables(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger) -> tuple[Optional[list], str]:
    """Handle the list-tables action."""
    query_display_name = 'List Tables'
    logger.info(f"Attempting to execute: {query_display_name}")
    sql, params = query_manager.get_list_tables_query()
    
    if db.execute_query(sql, params):
        logger.debug("Query executed successfully. Fetching results...")
        fetched_data = db.fetch_results()
        if fetched_data is not None:
            if not fetched_data:
                logger.info("Query executed successfully, but returned no results.")
            return fetched_data, query_display_name
        else:
            logger.error("Error occurred while fetching results.")
            raise RuntimeError("Error occurred while fetching results.")
    else:
        logger.error("Aborting: Query execution failed.")
        raise RuntimeError("Query execution failed.")

def handle_get_patient_by_id(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """
    Handle the get_patient_by_id query for single ID or batch CSV input.
    
    Supports two modes of operation:
    1. Single patient ID lookup (using --patient-id)
    2. Batch processing of multiple IDs from a CSV file (using --input-csv)
    
    For batch processing, the CSV file must contain a header row with a column
    containing patient IDs. The column name defaults to "PatientID" but can be
    specified with --id-column.
    """
    original_query_display_name = "Query 'get_patient_by_id'"
    
    # Check if we're doing batch CSV processing
    if args.input_csv:
        # Batch CSV processing
        if args.patient_id is not None:
            logger.error("Cannot use --patient-id (-i) and --input-csv (-ic) simultaneously.")
            parser.error("Cannot use --patient-id (-i) and --input-csv (-ic) simultaneously for query 'get_patient_by_id'.")
        
        query_display_name = f"Batch Query 'get_patient_by_id' from {os.path.basename(args.input_csv)}"
        logger.info(f"Attempting to execute: {query_display_name}")
        
        # Read patient IDs from CSV
        patient_id_strings = read_ids_from_csv(args.input_csv, args.id_column, logger)
        if not patient_id_strings:
            logger.error(f"No valid patient IDs found in '{args.input_csv}' or error reading file.")
            # Store this info for metadata later
            args.batch_info = {'total_ids_in_csv': 0, 'ids_processed_successfully': 0, 'ids_failed': 0}
            return [], query_display_name  # Return empty list, no data
        
        all_results = []
        successful_count = 0
        failed_ids_details = {}  # Store {id_str: reason}
        
        for id_str in patient_id_strings:
            try:
                current_patient_id = int(id_str)
            except ValueError:
                logger.warning(f"Invalid PatientID format '{id_str}' from CSV. Skipping.")
                failed_ids_details[id_str] = "Invalid ID format"
                continue
            
            logger.debug(f"Batch processing: Fetching data for Patient ID {current_patient_id}")
            sql, params = query_manager.get_patient_by_id_query(current_patient_id)
            
            if db.execute_query(sql, params):
                fetched_data = db.fetch_results()
                if fetched_data is not None:
                    all_results.extend(fetched_data)
                    successful_count += 1
                    if not fetched_data:
                        logger.info(f"Query for Patient ID {current_patient_id} (from CSV) returned no data.")
                else:
                    logger.error(f"Error fetching results for Patient ID {current_patient_id} (from CSV).")
                    failed_ids_details[str(current_patient_id)] = "Fetch error"  # Use str for key consistency
            else:
                logger.error(f"Query execution failed for Patient ID {current_patient_id} (from CSV).")
                failed_ids_details[str(current_patient_id)] = "Execution error"
        
        logger.info(f"Batch processing summary: Successfully fetched data for {successful_count} out of {len(patient_id_strings)} IDs from CSV.")
        if failed_ids_details:
            logger.warning(f"Failed to process {len(failed_ids_details)} IDs: {failed_ids_details}")
        
        # Store batch processing info in args for metadata
        args.batch_info = {
            'csv_file_path': args.input_csv,
            'id_column_name': args.id_column,
            'total_ids_in_csv': len(patient_id_strings),
            'ids_processed_successfully': successful_count,
            'ids_failed_count': len(failed_ids_details),
            'failed_ids_details': failed_ids_details 
        }
        return all_results, query_display_name
    
    else:
        # Single ID processing (existing logic)
        query_display_name = original_query_display_name
        if args.patient_id is None:
            logger.error("Argument --patient-id/-i is REQUIRED for query 'get_patient_by_id' (or use --input-csv).")
            parser.error("Argument --patient-id/-i is REQUIRED for query 'get_patient_by_id' (or use --input-csv).")
        
        logger.info(f"Attempting to execute: {query_display_name} for Patient ID {args.patient_id}")
        sql, params = query_manager.get_patient_by_id_query(args.patient_id)
        
        if db.execute_query(sql, params):
            logger.debug("Query executed successfully. Fetching results...")
            fetched_data = db.fetch_results()
            if fetched_data is not None:
                if not fetched_data:
                    logger.info(f"Query executed successfully, but no data found for Patient ID {args.patient_id}.")
                args.batch_info = None  # Indicate not a batch operation
                return fetched_data, query_display_name
            else:
                logger.error("Error occurred while fetching results.")
                raise RuntimeError("Error occurred while fetching results.")
        else:
            logger.error("Aborting: Query execution failed.")
            raise RuntimeError("Query execution failed.")

def handle_patient_by_name_dob(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """Handle the patient-by-name-dob query."""
    query_display_name = f"Query 'patient-by-name-dob'"
    if not all([args.first_name, args.last_name, args.dob]):
        logger.error("Arguments --first-name/-fn, --last-name/-ln, and --dob/-d are REQUIRED for query 'patient-by-name-dob'.")
        parser.error("Arguments --first-name/-fn, --last-name/-ln, and --dob/-d are REQUIRED for query 'patient-by-name-dob'.")

    try:
        dob_object = parse_dob_str(args.dob, logger)
    except ValueError:
        parser.error(f"Invalid Date of Birth format for --dob/-d. Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")

    logger.info(f"Attempting to execute: {query_display_name} for Name={args.first_name} {args.last_name}, DOB={args.dob}")
    sql, params = query_manager.get_patient_by_name_dob_query(args.first_name, args.last_name, dob_object)
    
    if db.execute_query(sql, params):
        logger.debug("Query executed successfully. Fetching results...")
        fetched_data = db.fetch_results()
        if fetched_data is not None:
            if not fetched_data:
                logger.info(f"Query executed successfully, but no data found for FirstName='{args.first_name}', LastName='{args.last_name}', DOB='{args.dob}'.")
            return fetched_data, query_display_name
        else:
            logger.error("Error occurred while fetching results.")
            raise RuntimeError("Error occurred while fetching results.")
    else:
        logger.error("Aborting: Query execution failed.")
        raise RuntimeError("Query execution failed.")

def handle_patient_fuzzy_search(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """Handle the patient-fuzzy-search query."""
    query_display_name = "Fuzzy Patient Search"
    if not any([args.first_name, args.last_name, args.dob]):
        logger.error("At least one search parameter (--first-name/-fn, --last-name/-ln, or --dob/-d) is REQUIRED for fuzzy search.")
        parser.error("At least one search parameter is REQUIRED for fuzzy search.")

    dob_object = None
    if args.dob:
        try:
            dob_object = parse_dob_str(args.dob, logger)
        except ValueError:
            parser.error(f"Invalid Date of Birth format for --dob/-d. Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")

    search_params = {
        'first_name': args.first_name,
        'last_name': args.last_name,
        'dob': dob_object
    }
    logger.info(f"Attempting to execute: {query_display_name} with params {search_params}")

    fuzzy_matcher = FuzzyMatcher(
        string_similarity_threshold=args.fuzzy_threshold,
        date_year_tolerance=args.dob_year_tolerance
    )
    strategy = PatientSearchStrategy(db, query_manager, fuzzy_matcher)
    results = strategy.search(search_params, min_overall_score=args.min_match_score)
    
    if not results:
        logger.info("Fuzzy search completed, but no matching patients were found.")
    else:
        logger.info(f"Fuzzy search found {len(results)} potential matches.")
    return results, query_display_name

def handle_get_table_columns(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """
    Handle the get-table-columns query.
    
    Executes a query to fetch column information for a specific table and schema,
    then processes the results into a summary format.
    
    Returns:
        tuple: (List containing a single dictionary with the table summary, query display name)
               The dictionary includes table name, schema, column count, and a formatted list of columns.
    """
    # Update the display name to be more descriptive
    query_display_name = f"Table Column Details for '{args.table_schema}.{args.table_name}'"
    
    if not args.table_name or not args.table_schema:
        logger.error("Arguments --table-name and --table-schema are REQUIRED for query 'get-table-columns'.")
        parser.error("Arguments --table-name and --table-schema are REQUIRED for query 'get-table-columns'.")
    
    logger.info(f"Attempting to execute: get-table-columns query for {args.table_schema}.{args.table_name}")
    sql, params = query_manager.get_table_columns_query(args.table_name, args.table_schema)
    
    if db.execute_query(sql, params):
        logger.debug("Query executed successfully. Fetching results...")
        fetched_column_data = db.fetch_results()
        
        if fetched_column_data is None:
            logger.error("Error occurred while fetching table column results.")
            raise RuntimeError("Error occurred while fetching table column results.")
        
        # Process the fetched data into a summary format
        summary_dict = {
            "Table Name": args.table_name,
            "Table Schema": args.table_schema,
        }
        
        if not fetched_column_data:
            logger.info(f"Query executed successfully, but no columns found for {args.table_schema}.{args.table_name}.")
            summary_dict["Column Count"] = 0
            summary_dict["Columns"] = "No columns found or table not accessible."
            summary_dict["Status"] = "Table metadata not found or table empty/inaccessible"
        else:
            # Calculate column count
            column_count = len(fetched_column_data)
            
            # Create a formatted string listing each column and its type on a new line
            columns_list = [f"{col['COLUMN_NAME']} ({col['DATA_TYPE']})" for col in fetched_column_data]
            columns_summary_str = "\n".join(columns_list)
            
            summary_dict["Column Count"] = column_count
            summary_dict["Columns"] = columns_summary_str
            
            logger.info(f"Successfully retrieved {column_count} columns for {args.table_schema}.{args.table_name}")
        
        # Return a list containing the single summary dictionary
        return [summary_dict], query_display_name
    else:
        logger.error("Aborting: Query execution failed.")
        raise RuntimeError("Query execution failed.")

def handle_batch_exact_search_demographics(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """
    Handle batch patient search using EXACT demographics from a CSV file.
    """
    query_display_name = f"Batch Exact Demographic Search from {os.path.basename(args.input_csv)}" if args.input_csv else "Batch Exact Demographic Search"
    
    if not args.input_csv:
        logger.error("--input-csv (-ic) is REQUIRED for 'batch-search-demographics' query.")
        parser.error("--input-csv (-ic) is REQUIRED for 'batch-search-demographics' query.")

    logger.info(f"Attempting to execute: {query_display_name}")

    # Read patient data from CSV
    from .utils import read_patient_data_from_csv
    patient_data_rows = read_patient_data_from_csv(
        args.input_csv, args.fn_column, args.ln_column, args.dob_column, logger
    )

    if not patient_data_rows:
        logger.warning(f"No patient data rows read from '{args.input_csv}'. Aborting search.")
        args.batch_info = {
            'csv_file_path': args.input_csv,
            'fn_column_name': args.fn_column,
            'ln_column_name': args.ln_column,
            'dob_column_name': args.dob_column,
            'total_rows_in_csv_input': 0
        }
        return [], query_display_name

    all_results = []
    rows_parsed_for_processing = len(patient_data_rows)
    rows_with_input_errors = 0
    successful_searches_conducted = 0
    failed_row_details: Dict[int, str] = {}
    
    # Load the base SQL for selecting patient fields
    try:
        base_select_sql = query_manager.load_query_template('get_patient_base_fields')
    except QueryTemplateNotFoundError:
        logger.error("SQL template 'get_patient_base_fields.sql' not found. Using hardcoded SELECT.")
        base_select_sql = """
        SELECT p.PatientID, p.Vorname, p.Name, p.Geburtsdatum, 
               p.Grunderkrankung, p.ET_Grunderkrankung, p.Dauernotiz, p.Dauernotiz_Diagnose 
        FROM dbo.Patient p"""

    for patient_data_row in patient_data_rows:
        original_row_num = patient_data_row['original_row_number']
        first_name_csv = patient_data_row.get('first_name')
        last_name_csv = patient_data_row.get('last_name')
        dob_str_csv = patient_data_row.get('dob')
        
        dob_object = None
        current_row_error_message = None

        if dob_str_csv:
            try:
                dob_object = parse_dob_str(dob_str_csv, logger)
            except ValueError:
                current_row_error_message = f"Invalid DOB format: '{dob_str_csv}'"
                logger.warning(f"CSV row {original_row_num}: {current_row_error_message}. DOB will be ignored for this search.")

        # Build dynamic WHERE clause based on provided non-empty fields
        conditions = []
        params = []
        input_criteria_for_match_info = {}

        if first_name_csv:
            conditions.append("p.Vorname = ?")
            params.append(first_name_csv)
            input_criteria_for_match_info['FirstName'] = first_name_csv
        if last_name_csv:
            conditions.append("p.Name = ?")
            params.append(last_name_csv)
            input_criteria_for_match_info['LastName'] = last_name_csv
        if dob_object:
            conditions.append("p.Geburtsdatum = ?")
            params.append(dob_object)
            input_criteria_for_match_info['DOB'] = dob_object

        if not conditions:
            msg = "No valid search criteria (FirstName, LastName, or valid DOB) provided."
            if current_row_error_message:
                msg = f"{current_row_error_message} and no other search parameters."
            logger.warning(f"CSV row {original_row_num}: {msg} Skipping search.")
            failed_row_details[original_row_num] = msg
            rows_with_input_errors += 1
            continue

        # Construct and execute the query
        final_sql = f"{base_select_sql} WHERE {' AND '.join(conditions)}"
        logger.debug(f"CSV row {original_row_num}: Executing SQL: {final_sql} with params: {params}")

        if db.execute_query(final_sql, tuple(params)):
            successful_searches_conducted += 1
            fetched_db_records = db.fetch_results()
            if fetched_db_records:
                logger.info(f"CSV row {original_row_num}: Found {len(fetched_db_records)} exact match(es).")
                for db_rec in fetched_db_records:
                    match_infos = []
                    if 'FirstName' in input_criteria_for_match_info:
                        match_infos.append(MatchInfo(
                            "FirstName", 
                            input_criteria_for_match_info['FirstName'],
                            db_rec.get('Vorname'),
                            "Exact",
                            1.0
                        ))
                    if 'LastName' in input_criteria_for_match_info:
                        match_infos.append(MatchInfo(
                            "LastName",
                            input_criteria_for_match_info['LastName'],
                            db_rec.get('Name'),
                            "Exact",
                            1.0
                        ))
                    if 'DOB' in input_criteria_for_match_info:
                        match_infos.append(MatchInfo(
                            "DOB",
                            input_criteria_for_match_info['DOB'],
                            db_rec.get('Geburtsdatum'),
                            "Exact",
                            1.0
                        ))
                    
                    candidate = MatchCandidate(
                        db_record=db_rec,
                        match_fields_info=match_infos,
                        overall_score=1.0,  # Exact match based on provided criteria
                        primary_match_type="Exact Match (CSV Batch)",
                        csv_input_row_number=original_row_num,
                        csv_input_data={
                            'first_name': first_name_csv,
                            'last_name': last_name_csv,
                            'dob_str': dob_str_csv
                        }
                    )
                    all_results.append(candidate)
            else:
                logger.info(f"CSV row {original_row_num}: No exact matches found for the provided criteria.")
        else:
            logger.error(f"CSV row {original_row_num}: Query execution failed.")
            failed_row_details[original_row_num] = "Query execution error"
            rows_with_input_errors += 1

    # Populate batch_info for metadata
    args.batch_info = {
        'csv_file_path': args.input_csv,
        'fn_column_name': args.fn_column,
        'ln_column_name': args.ln_column,
        'dob_column_name': args.dob_column,
        'rows_parsed_for_processing': rows_parsed_for_processing,
        'rows_with_input_errors': rows_with_input_errors,
        'successful_searches_conducted': successful_searches_conducted,
        'total_exact_matches_found': len(all_results),
        'failed_row_details': failed_row_details if failed_row_details else {}
    }

    logger.info(f"Batch exact demographic search summary: "
                f"{rows_parsed_for_processing} rows parsed, "
                f"{successful_searches_conducted} searches conducted, "
                f"{rows_with_input_errors} rows had input/query issues, "
                f"{len(all_results)} total exact matches found.")

    return all_results, query_display_name

# Action handlers dictionary mapping actions to their handler functions
ACTION_HANDLERS = {
    'list-tables': handle_list_tables,
    'query': {
        'get_patient_by_id': handle_get_patient_by_id,
        'patient-by-name-dob': handle_patient_by_name_dob,
        'patient-fuzzy-search': handle_patient_fuzzy_search,
        'get-table-columns': handle_get_table_columns,
        'batch-search-demographics': lambda args, query_manager, db, logger, parser: (
            handle_batch_exact_search_demographics(
                sql_interface=db,
                query_manager=query_manager,
                input_csv=args.input_csv,
                fn_column=args.fn_column,
                ln_column=args.ln_column,
                dob_column=args.dob_column,
                logger=logger
            ),
            'Batch Demographic Search'  # Display name for the query
        )
    }
}

def handle_batch_exact_search_demographics(
    sql_interface: SQLInterface,
    query_manager: QueryManager,
    input_csv: str,
    fn_column: str,
    ln_column: str,
    dob_column: str,
    logger: logging.Logger
) -> List[MatchCandidate]:
    """
    Handle batch demographic search using CSV data.
    
    Args:
        sql_interface: SQLInterface instance for database queries
        query_manager: QueryManager instance for managing SQL templates
        input_csv: Path to input CSV file
        fn_column: Column name for first name
        ln_column: Column name for last name
        dob_column: Column name for date of birth
        logger: Logger instance
        
    Returns:
        List[MatchCandidate]: List of match candidates with CSV traceability
    """
    from .utils import read_patient_data_from_csv
    
    # Read patient data from CSV
    patients_data = read_patient_data_from_csv(input_csv, fn_column, ln_column, dob_column, logger)
    if not patients_data:
        logger.warning(f"No valid patient data found in CSV file: {input_csv}")
        return []
    
    match_candidates = []
    query_template = query_manager.load_query_template('get_patient_by_name_dob')
    
    # Process each row independently
    for patient in patients_data:
        first_name = patient["first_name"]
        last_name = patient["last_name"]
        dob_str = patient["date_of_birth"]
        
        try:
            # Parse and validate DOB
            dob_date = parse_dob_str(dob_str, logger)
            if not dob_date:
                continue
                
            # Execute exact match query
            params = [first_name, last_name, dob_str]
            if sql_interface.execute_query(query_template, params):
                results = sql_interface.fetch_results()
                if not results:
                    continue
                
                # Create match candidates with CSV traceability
                for db_record in results:
                    match_candidate = MatchCandidate(
                        db_record=dict(db_record),
                        csv_input_row_number=patient["_row_number"],
                        csv_input_data=patient["_raw_data"]
                    )
                    
                    # Add match info for each field
                    match_candidate.match_fields_info.extend([
                        MatchInfo(
                            field_name="first_name",
                            input_value=first_name,
                            db_value=db_record["Vorname"],
                            match_type="Exact"
                        ),
                        MatchInfo(
                            field_name="last_name",
                            input_value=last_name,
                            db_value=db_record["Name"],
                            match_type="Exact"
                        ),
                        MatchInfo(
                            field_name="date_of_birth",
                            input_value=dob_str,
                            db_value=db_record["Geburtsdatum"],
                            match_type="Exact"
                        )
                    ])
                    
                    match_candidate.calculate_overall_score_and_type(
                        field_weights={"first_name": 0.3, "last_name": 0.4, "date_of_birth": 0.3},
                        score_mapping={"Exact": 1.0, "Fuzzy": "use_similarity"}
                    )
                    
                    match_candidates.append(match_candidate)
                
        except Exception as e:
            logger.error(f"Error processing row {patient['_row_number']}: {str(e)}")
            continue
    
    return match_candidates