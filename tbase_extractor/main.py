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
from .sql_interface.dynamic_query_manager import HybridQueryManager
from .sql_interface.flexible_query_builder import FlexibleQueryManager
from .sql_interface.output_formatter import OutputFormatter
from .matching import FuzzyMatcher, PatientSearchStrategy
from .config import LOGGER_NAME
from .metadata import create_metadata_dict
from .output_handler import handle_output
import io

load_dotenv()

DOB_FORMAT = "%Y-%m-%d"  # Define the expected date format

def parse_dob_str(dob_str: Optional[str], logger: logging.Logger) -> Optional[date]:
    """Parses a DOB string and returns a date object or None if invalid."""
    if not dob_str:
        return None
    try:
        return datetime.strptime(dob_str, DOB_FORMAT).date()
    except ValueError:
        logger.error(f"Invalid Date of Birth format for '{dob_str}'. Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")
        raise ValueError(f"Invalid Date of Birth format: {dob_str}")

def determine_output_format(user_format: Optional[str], output_file_path: Optional[str], logger: logging.Logger) -> str:
    """Determines the effective output format."""
    if user_format:
        return user_format
    if output_file_path:
        _filename, ext = os.path.splitext(output_file_path)
        ext = ext.lower()
        if ext == '.json':
            return 'json'
        elif ext == '.csv':
            return 'csv'
        elif ext == '.tsv':
            return 'tsv'
        elif ext == '.txt':
            return 'txt'
        else:
            if ext:
                logger.warning(
                    f"Output file extension '{ext}' for '{output_file_path}' is not recognized. "
                    f"Defaulting to 'json' format."
                )
            else:
                logger.warning(
                    f"No output file extension provided for '{output_file_path}'. "
                    f"Defaulting to 'json' format."
                )
            return 'json'
    return 'stdout'  # Default if no file and no format specified

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
        dest='action', help='The main action to perform. Use one of the subcommands below.', required=True, metavar='ACTION'
    )    # --- Sub-command: list-tables ---
    parser_list_tables = subparsers.add_parser('list-tables', help='List all available base tables in the database.')
    
    # Add dynamic query options to list-tables
    parser_list_tables.add_argument('--schema', type=str, default='dbo',
        help="Database schema name (default: dbo).")
    
    parser_list_tables.add_argument('--use-dynamic-builder', action='store_true',
        help="Use dynamic query builder instead of static templates.")

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
             "  'batch-search-demographics': Search for patients using a CSV file with demographic data.\n"
             "  'get-table-columns': Get column details for a specific table and schema."
    )
    parser_query.add_argument(
        '--patient-id', '-i', type=int, metavar='ID',
        help='Patient ID (required for the \"get_patient_by_id\" query unless --input-csv is used).'
    )
    parser_query.add_argument(
        '--input-csv', '-ic', type=str, metavar='CSV_FILE_PATH',
        help='Path to a CSV file containing patient identifiers for batch processing. '
             'When used with query_name "get_patient_by_id", this will process multiple IDs.'
    )
    parser_query.add_argument(
        '--id-column', '-idc', type=str, metavar='COLUMN_NAME', default='PatientID',
        help='The name of the column in the CSV file that contains the patient identifiers (default: "PatientID").'
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
    
    # Batch demographics search arguments
    parser_query.add_argument(
        '--fn-column', type=str, metavar='COLUMN_NAME', default='FirstName',
        help="The name of the column in the CSV file containing first names (default: 'FirstName')."
    )
    parser_query.add_argument(
        '--ln-column', type=str, metavar='COLUMN_NAME', default='LastName',
        help="The name of the column in the CSV file containing last names (default: 'LastName')."
    )
    parser_query.add_argument(
        '--dob-column', type=str, metavar='COLUMN_NAME', default='DOB',
        help="The name of the column in the CSV file containing dates of birth (default: 'DOB')."
    )
    
    # Fuzzy search specific arguments
    parser_query.add_argument(
        '--fuzzy-threshold', type=float, default=0.85, metavar='0.0-1.0',
        help='Similarity threshold for fuzzy name matching (0.0 to 1.0, default: 0.85).'
    )
    parser_query.add_argument(
        '--dob-year-tolerance', type=int, default=1, metavar='YEARS',
        help='Maximum year difference allowed for DOB matching (default: 1).'
    )
    parser_query.add_argument(
        '--min-match-score', type=float, default=0.3, metavar='SCORE',        help='Minimum overall match score required (0.0 to 1.0, default: 0.3).'
    )
    
    parser_query.add_argument(
        '--format', '-f',
        type=str,
        choices=['json', 'csv', 'tsv', 'txt', 'stdout'],
        default=None,        help='Output format: json, csv, tsv, txt, or stdout (pretty table to console). Inferred from -o extension if not set.'
    )
    parser_query.add_argument(
        '--optimize-txt', action='store_true',
        help='When using TXT format, optimize output by showing patient info once followed by varying data (like diagnoses).'
    )
    
    parser_query.add_argument('--table-name', '-tn',
        required=False,
        help="The name of the table to query (e.g., 'Patient').")
    
    parser_query.add_argument('--table-schema', '-ts',
        required=False,
        help="The schema of the table to query (e.g., 'dbo').")
    
    # Dynamic query builder arguments
    parser_query.add_argument('--patient-table', type=str, default='Patient',
        help="Name of the patient table (default: Patient).")
    
    parser_query.add_argument('--diagnose-table', type=str, default='Diagnose',
        help="Name of the diagnose table (default: Diagnose).")
    
    parser_query.add_argument('--schema', type=str, default='dbo',
        help="Database schema name (default: dbo).")
    
    parser_query.add_argument('--include-diagnoses', action='store_true',
        help="Include diagnosis information in patient queries (adds LEFT JOIN to diagnose table).")
    
    parser_query.add_argument('--use-dynamic-builder', action='store_true',
        help="Use dynamic query builder instead of static templates.")
    
    # --- Sub-command: discover-patient-tables ---
    parser_discover = subparsers.add_parser('discover-patient-tables', 
        help='Discover tables that contain patient ID columns.')
    parser_discover.add_argument('--schema', type=str, default='dbo',
        help="Database schema name to search (default: dbo).")
    parser_discover.add_argument(
        '--format', '-f',
        type=str,
        choices=['json', 'csv', 'tsv', 'txt', 'stdout'],
        default='stdout',
        help='Output format: json, csv, tsv, txt, or stdout (default: stdout).'
    )
    parser_discover.add_argument(
        '--optimize-txt', action='store_true',
        help='When using TXT format, optimize output by showing patient info once followed by varying data.'
    )
    parser_discover.add_argument(
        '--output', '-o', type=str, metavar='FILE_PATH',
        help='Optional path to save results as a JSON, CSV, or TSV file.'
    )

    # --- Sub-command: query-custom-tables ---
    parser_custom = subparsers.add_parser('query-custom-tables',
        help='Query arbitrary patient-related tables using flexible specifications.')
    parser_custom.add_argument(
        '--patient-id', '-i', type=int, metavar='ID',
        help='Patient ID to query for (required unless --input-csv is used).'
    )
    parser_custom.add_argument(
        '--input-csv', '-ic', type=str, metavar='CSV_FILE_PATH',
        help='Path to a CSV file containing patient identifiers for batch processing.'
    )
    parser_custom.add_argument(
        '--id-column', '-idc', type=str, metavar='COLUMN_NAME', default='PatientID',
        help='The name of the column in the CSV file that contains the patient identifiers (default: "PatientID").'
    )
    parser_custom.add_argument(
        '--tables', '-t', type=str, nargs='+', required=True, metavar='TABLE_SPEC',
        help='Table specifications in format: schema.table:alias[columns]@patient_id_column. '
             'Examples: "dbo.Patient:p", "dbo.Diagnose:d[ICD10,Bezeichnung]", "lab.Results:r@PatID"'
    )
    parser_custom.add_argument(
        '--join-type', type=str, choices=['LEFT', 'INNER', 'RIGHT', 'FULL'], default='LEFT',
        help='Type of join to use between tables (default: LEFT).'
    )
    parser_custom.add_argument(
        '--order-by', type=str, nargs='*', metavar='COLUMN',
        help='Columns to order results by (e.g., "p.PatientID", "d.Date").'
    )
    parser_custom.add_argument(
        '--limit', type=int, metavar='N',        help='Maximum number of rows to return.'
    )
    parser_custom.add_argument(
        '--format', '-f',
        type=str,
        choices=['json', 'csv', 'tsv', 'txt', 'stdout'],
        default=None,
        help='Output format: json, csv, tsv, txt, or stdout (pretty table to console). Inferred from -o extension if not set.'
    )
    parser_custom.add_argument(
        '--optimize-txt', action='store_true',
        help='When using TXT format, optimize output by showing patient info once followed by varying data (like diagnoses).'
    )
    parser_custom.add_argument(
        '--output', '-o', type=str, metavar='FILE_PATH',
        help='Optional path to save results as a JSON, CSV, TSV, or TXT file.'
    )
    parser_custom.add_argument(
        '--split-output', action='store_true',
        help='Create separate output files for each patient when processing multiple patients.'
    )
    parser_custom.add_argument(
        '--filename-template', type=str, default="{PatientID}",
        help='Template for output filenames when using --split-output (default: "{PatientID}").'
    )
    
    return parser

def setup_logging(debug: bool = False, log_file: str = None):
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
        force=True
    )

def main():
    # 1. Setup (templates_dir, parser, args, logging)
    try:
        templates_dir = resolve_templates_dir()
    except RuntimeError as e:
        # No logger yet, so print to stderr        print(f"Critical Error: {e}", file=sys.stderr)
        sys.exit(1)

    parser = setup_arg_parser()
    args = parser.parse_args()
    
    debug = getattr(args, 'debug', False)
    log_file = os.getenv('SQL_APP_LOGFILE', None)
    setup_logging(debug, log_file)  # Ensure logger is configured
    logger = logging.getLogger("tbase_extractor.main")  # Use a specific logger name
    logger.debug(f"Parsed arguments: {args}")

    # Determine query manager based on use_dynamic_builder flag
    use_dynamic = getattr(args, 'use_dynamic_builder', False)
    if use_dynamic:
        patient_table = getattr(args, 'patient_table', 'Patient')
        diagnose_table = getattr(args, 'diagnose_table', 'Diagnose')
        schema = getattr(args, 'schema', 'dbo')
        query_manager = HybridQueryManager(templates_dir, patient_table, diagnose_table, schema, debug=debug)
        logger.info(f"Using dynamic query builder with patient_table='{patient_table}', diagnose_table='{diagnose_table}', schema='{schema}'")
    else:
        query_manager = QueryManager(templates_dir, debug=debug)  # Pass debug to QM
        logger.info("Using static template-based query manager")
    
    results = None
    query_display_name = args.action  # Default display name

    # 2. Database Interaction and Action Handling
    try:
        with SQLInterface(debug=debug) as db:  # Pass debug to SQLInterface
            if not db.connection:
                logger.error("Aborting: Database connection failed.")
                sys.exit(1)            # Record query start time for metadata
            query_start_time = datetime.utcnow()

            if args.action == 'list-tables':
                handler = ACTION_HANDLERS.get('list-tables')
                if handler:
                    results, query_display_name = handler(args, query_manager, db, logger)
                else:  # Should not happen if parser is set up correctly
                    logger.error(f"No handler for action: {args.action}")
                    sys.exit(1)
            
            elif args.action == 'discover-patient-tables':
                handler = ACTION_HANDLERS.get('discover-patient-tables')
                if handler:
                    results, query_display_name = handler(args, query_manager, db, logger)
                else:
                    logger.error(f"No handler for action: {args.action}")
                    sys.exit(1)
            
            elif args.action == 'query-custom-tables':
                handler = ACTION_HANDLERS.get('query-custom-tables')
                if handler:
                    results, query_display_name = handler(args, query_manager, db, logger, parser)
                else:
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
        effective_format = determine_output_format(user_format_arg, output_file_path, logger)
        
        # Create metadata using the new utility function
        metadata_dict = create_metadata_dict(
            query_start_time, execution_duration_ms, args, 
            query_display_name, results
        )
        
        # Add batch-specific metadata if available (populated by the handler)
        if hasattr(args, 'batch_info') and args.batch_info:
            metadata_dict['batch_processing_summary'] = args.batch_info
            # Refine status for batch
            if results:
                if args.batch_info['ids_processed_successfully'] == args.batch_info['total_ids_in_csv']:
                    metadata_dict['status'] = "batch_success_all_processed"
                elif args.batch_info['ids_processed_successfully'] > 0:
                    metadata_dict['status'] = "batch_partial_success"
                else:  # No IDs processed successfully, but CSV might have had IDs
                    metadata_dict['status'] = "batch_processed_no_data_or_all_failed"
            elif args.batch_info['total_ids_in_csv'] > 0:  # CSV had IDs, but no results (all failed or no data)
                metadata_dict['status'] = "batch_processed_no_data_or_all_failed"
            else:  # No IDs in CSV or CSV error
                metadata_dict['status'] = "batch_input_error_or_empty"
        elif results:  # Single query with results
            metadata_dict['status'] = "success"
        else:  # Single query, no results
            metadata_dict['status'] = "success_no_data"        # Get split output parameters
        split_output = getattr(args, 'split_output', False)
        filename_template = getattr(args, 'filename_template', "{PatientID}")
        
        # Get optimize_txt flag for TXT format optimization
        optimize_txt = getattr(args, 'optimize_txt', False)
        
        # Call handle_output with the new parameters
        handle_output(results, output_file_path, query_display_name, effective_format, 
                     metadata_dict, split_output, filename_template, optimize_txt)
    
    logger.info(f"--- {query_display_name} finished ---")


if __name__ == "__main__":
    main()

def handle_list_tables(args: argparse.Namespace, query_manager, db: SQLInterface, logger: logging.Logger) -> tuple[Optional[list], str]:
    """Handle the list-tables action."""
    query_display_name = 'List Tables'
    logger.info(f"Attempting to execute: {query_display_name}")
    
    # Check if we're using dynamic query manager
    use_dynamic = getattr(args, 'use_dynamic_builder', False)
    if hasattr(query_manager, 'get_list_tables_query') and hasattr(query_manager.get_list_tables_query, '__code__') and 'use_dynamic' in query_manager.get_list_tables_query.__code__.co_varnames:
        sql, params = query_manager.get_list_tables_query(use_dynamic=use_dynamic)
    else:
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
            
            # Check if we're using dynamic query manager and pass include_diagnoses parameter
            include_diagnoses = getattr(args, 'include_diagnoses', False)
            if hasattr(query_manager, 'get_patient_by_id_query') and 'include_diagnoses' in query_manager.get_patient_by_id_query.__code__.co_varnames:
                sql, params = query_manager.get_patient_by_id_query(current_patient_id, include_diagnoses=include_diagnoses)
            else:
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
        
        # Check if we're using dynamic query manager and pass include_diagnoses parameter
        include_diagnoses = getattr(args, 'include_diagnoses', False)
        if hasattr(query_manager, 'get_patient_by_id_query') and 'include_diagnoses' in query_manager.get_patient_by_id_query.__code__.co_varnames:
            sql, params = query_manager.get_patient_by_id_query(args.patient_id, include_diagnoses=include_diagnoses)
        else:
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
    
    # Check if we're using dynamic query manager and pass include_diagnoses parameter
    include_diagnoses = getattr(args, 'include_diagnoses', False)
    if hasattr(query_manager, 'get_patient_by_name_dob_query') and 'include_diagnoses' in query_manager.get_patient_by_name_dob_query.__code__.co_varnames:
        sql, params = query_manager.get_patient_by_name_dob_query(args.first_name, args.last_name, dob_object, include_diagnoses=include_diagnoses)
    else:
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
    
    # Check if we're using dynamic query manager and pass include_diagnoses parameter
    include_diagnoses = getattr(args, 'include_diagnoses', False)
    results = strategy.search(search_params, min_overall_score=args.min_match_score, include_diagnoses=include_diagnoses)
    
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
            
            logger.info(f"Successfully retrieved {column_count} columns for {args.table_schema}.{args.table_name}")        # Return a list containing the single summary dictionary
        return [summary_dict], query_display_name
    else:
        logger.error("Aborting: Query execution failed.")
        raise RuntimeError("Query execution failed.")

def handle_discover_patient_tables(args: argparse.Namespace, query_manager, db: SQLInterface, logger: logging.Logger) -> tuple[Optional[list], str]:
    """Handle the discover-patient-tables action."""
    query_display_name = 'Discover Patient Tables'
    logger.info(f"Attempting to execute: {query_display_name}")
    
    # Create flexible query manager
    flexible_manager = FlexibleQueryManager(debug=getattr(args, 'debug', False))
    
    # Get discovery query
    sql, params = flexible_manager.discover_patient_tables(args.schema)
    
    if db.execute_query(sql, params):
        logger.debug("Query executed successfully. Fetching results...")
        fetched_data = db.fetch_results()
        if fetched_data is not None:
            if not fetched_data:
                logger.info("Query executed successfully, but found no tables with patient ID columns.")
            else:
                table_count = len(fetched_data)
                logger.info(f"Successfully discovered {table_count} tables with patient ID columns in schema '{args.schema}'")
            return fetched_data, query_display_name
        else:
            logger.error("Error occurred while fetching results.")
            raise RuntimeError("Error occurred while fetching results.")
    else:
        logger.error("Aborting: Query execution failed.")
        raise RuntimeError("Query execution failed.")

def handle_query_custom_tables(args: argparse.Namespace, query_manager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """Handle the query-custom-tables action."""
    original_query_display_name = "Query Custom Tables"
    
    # Create flexible query manager
    flexible_manager = FlexibleQueryManager(debug=getattr(args, 'debug', False))
    
    # Check if we're doing batch CSV processing
    if args.input_csv:
        # Batch CSV processing
        if args.patient_id is not None:
            logger.error("Cannot use --patient-id (-i) and --input-csv (-ic) simultaneously.")
            parser.error("Cannot use --patient-id (-i) and --input-csv (-ic) simultaneously for query-custom-tables.")
        
        query_display_name = f"Batch Query Custom Tables from {os.path.basename(args.input_csv)}"
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
            
            try:
                sql, params = flexible_manager.query_patient_tables(
                    patient_id=current_patient_id,
                    table_specs=args.tables,
                    join_type=args.join_type,
                    order_by=args.order_by,
                    limit=args.limit
                )
                
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
            except Exception as e:
                logger.error(f"Error processing Patient ID {current_patient_id}: {e}")
                failed_ids_details[str(current_patient_id)] = f"Processing error: {str(e)}"
        
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
        # Single ID processing
        query_display_name = original_query_display_name
        if args.patient_id is None:
            logger.error("Argument --patient-id/-i is REQUIRED for query-custom-tables (or use --input-csv).")
            parser.error("Argument --patient-id/-i is REQUIRED for query-custom-tables (or use --input-csv).")
        
        logger.info(f"Attempting to execute: {query_display_name} for Patient ID {args.patient_id}")
        logger.info(f"Table specifications: {args.tables}")
        
        try:
            sql, params = flexible_manager.query_patient_tables(
                patient_id=args.patient_id,
                table_specs=args.tables,
                join_type=args.join_type,
                order_by=args.order_by,
                limit=args.limit
            )
            
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
        except Exception as e:
            logger.error(f"Error processing custom table query: {e}")
            raise RuntimeError(f"Error processing custom table query: {e}")

def handle_batch_search_demographics(args: argparse.Namespace, query_manager: QueryManager, db: SQLInterface, logger: logging.Logger, parser: argparse.ArgumentParser) -> tuple[Optional[list], str]:
    """
    Handle the batch-search-demographics query.
    
    This function takes a CSV file containing patient demographic data (first name, last name, date of birth)
    and performs searches for each patient, optionally using fuzzy matching.
    """
    query_display_name = f"Batch Demographic Search"
    
    if not args.input_csv:
        logger.error("Argument --input-csv/-ic is REQUIRED for query 'batch-search-demographics'.")
        parser.error("Argument --input-csv/-ic is REQUIRED for query 'batch-search-demographics'.")
    
    logger.info(f"Attempting to execute: {query_display_name} from {os.path.basename(args.input_csv)}")
    
    # Read demographics data from CSV
    from .utils import read_patient_data_from_csv
    patients_data = read_patient_data_from_csv(
        args.input_csv, 
        args.fn_column, 
        args.ln_column, 
        args.dob_column, 
        logger
    )
    
    if not patients_data:
        logger.error(f"No valid patient data found in '{args.input_csv}' or error reading file.")
        args.batch_info = {
            'csv_file_path': args.input_csv, 
            'total_rows_in_csv': 0, 
            'rows_processed_successfully': 0,
            'rows_failed': 0
        }
        return [], query_display_name
    
    all_results = []
    successful_count = 0
    failed_rows_details = {}
    
    # Use fuzzy matching if specified
    use_fuzzy = getattr(args, 'fuzzy_threshold', None) is not None
    for patient in patients_data:
        row_num = patient.get('_row_number', 0)
        try:
            first_name = patient.get('first_name')
            last_name = patient.get('last_name')
            dob_str = patient.get('date_of_birth')
            
            if not (first_name and last_name):
                logger.warning(f"Row {row_num}: Missing first name or last name. Skipping.")
                failed_rows_details[row_num] = "Missing first name or last name"
                continue
            
            # Try to parse the date of birth
            dob_object = None
            if dob_str:
                try:
                    dob_object = parse_dob_str(dob_str, logger)
                except ValueError:
                    logger.warning(f"Row {row_num}: Invalid DOB format '{dob_str}'. Skipping.")
                    failed_rows_details[row_num] = f"Invalid DOB format: {dob_str}"
                    continue
            
            logger.debug(f"Batch processing: Searching for patient FirstName={first_name}, LastName={last_name}, DOB={dob_str}")
            
            if use_fuzzy:
                # Use fuzzy search for this patient
                fuzzy_matcher = FuzzyMatcher(
                    string_similarity_threshold=args.fuzzy_threshold,
                    date_year_tolerance=args.dob_year_tolerance
                )
                
                search_params = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'dob': dob_object
                }
                
                strategy = PatientSearchStrategy(db, query_manager, fuzzy_matcher)
                include_diagnoses = getattr(args, 'include_diagnoses', False)
                results = strategy.search(search_params, min_overall_score=args.min_match_score, include_diagnoses=include_diagnoses)
                
                if results:
                    all_results.extend(results)
                    successful_count += 1
                else:
                    logger.info(f"Fuzzy search for Row {row_num} completed, but no matching patients were found.")
            else:
                # Use exact match through patient-by-name-dob query
                if not dob_object:
                    logger.warning(f"Row {row_num}: DOB is required for exact match. Skipping.")
                    failed_rows_details[row_num] = "DOB is required for exact match"
                    continue
                
                # Check if we're using dynamic query manager and pass include_diagnoses parameter
                include_diagnoses = getattr(args, 'include_diagnoses', False)
                if hasattr(query_manager, 'get_patient_by_name_dob_query') and 'include_diagnoses' in query_manager.get_patient_by_name_dob_query.__code__.co_varnames:
                    sql, params = query_manager.get_patient_by_name_dob_query(first_name, last_name, dob_object, include_diagnoses=include_diagnoses)
                else:
                    sql, params = query_manager.get_patient_by_name_dob_query(first_name, last_name, dob_object)
                
                if db.execute_query(sql, params):
                    fetched_data = db.fetch_results()
                    if fetched_data is not None:
                        if fetched_data:  # Only count as successful if data was found
                            all_results.extend(fetched_data)
                            successful_count += 1
                        else:
                            logger.info(f"Row {row_num}: No data found for FirstName='{first_name}', LastName='{last_name}', DOB='{dob_str}'")
                            failed_rows_details[row_num] = "No matching patient found"
                    else:
                        logger.error(f"Error fetching results for Row {row_num}")
                        failed_rows_details[row_num] = "Error fetching results"
                else:
                    logger.error(f"Query execution failed for Row {row_num}")
                    failed_rows_details[row_num] = "Query execution failed"
        
        except Exception as e:
            logger.error(f"Error processing Row {row_num}: {e}")
            failed_rows_details[row_num] = f"Processing error: {str(e)}"
    
    # Log summary
    logger.info(f"Batch processing summary: Successfully processed {successful_count} out of {len(patients_data)} rows from CSV.")
    if failed_rows_details:
        logger.warning(f"Failed to process {len(failed_rows_details)} rows: {failed_rows_details}")
    
    # Store batch processing info in args for metadata
    args.batch_info = {
        'csv_file_path': args.input_csv,
        'total_rows_in_csv': len(patients_data),
        'rows_processed_successfully': successful_count,
        'rows_failed_count': len(failed_rows_details),
        'failed_rows_details': failed_rows_details
    }
    
    return all_results, query_display_name
# Action handlers dictionary mapping actions to their handler functions
ACTION_HANDLERS = {
    'list-tables': handle_list_tables,
    'discover-patient-tables': handle_discover_patient_tables,
    'query-custom-tables': handle_query_custom_tables,
    'query': {
        'get_patient_by_id': handle_get_patient_by_id,
        'patient-by-name-dob': handle_patient_by_name_dob,
        'patient-fuzzy-search': handle_patient_fuzzy_search,
        'get-table-columns': handle_get_table_columns,
        'batch-search-demographics': handle_batch_search_demographics,
    }
}