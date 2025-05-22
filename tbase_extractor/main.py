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
    )

    # --- Sub-command: list-tables ---
    subparsers.add_parser('list-tables', help='List all available base tables in the database.')

    # --- Sub-command: query ---
    parser_query = subparsers.add_parser('query', help='Execute a predefined query template.')
    parser_query.add_argument('--query-name', '-q',
        required=True,
        choices=['get_patient_by_id', 'patient-by-name-dob', 'patient-fuzzy-search', 'get-table-columns'],
        help="REQUIRED. The name of the predefined query template to execute.\n"
             "Must correspond to a file in the 'sql_templates' directory.\n"
             "Examples:\n"
             "  'get_patient_by_id': Get details for a specific patient by ID.\n"
             "  'patient-by-name-dob': Get patient details by Name and Date of Birth.\n"
             "  'patient-fuzzy-search': Search for patients with fuzzy name matching and year tolerance.\n"
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
        '--min-match-score', type=float, default=0.3, metavar='SCORE',
        help='Minimum overall match score required (0.0 to 1.0, default: 0.3).'
    )

    parser_query.add_argument(
        '--format', '-f',
        type=str,
        choices=['json', 'csv', 'tsv', 'stdout'],
        default=None,
        help='Output format: json, csv, tsv, or stdout (pretty table to console). Inferred from -o extension if not set.'
    )
    parser_query.add_argument('--table-name', '-tn',
        required=False,
        help="The name of the table to query (e.g., 'Patient').")
    parser_query.add_argument('--table-schema', '-ts',
        required=False,
        help="The schema of the table to query (e.g., 'dbo').")
    return parser

def handle_output(results_envelope, output_file_path, query_display_name, effective_format, metadata_dict=None):
    logger = logging.getLogger("main")
    output_formatter = OutputFormatter()
    formatted = ''
    metadata_summary = ''
    if metadata_dict:
        metadata_lines = [f"# {k}: {v}" for k, v in metadata_dict.items()]
        metadata_summary = '\n'.join(metadata_lines)
    if output_file_path:
        with open(output_file_path, 'w', encoding='utf-8', newline='') as f:
            if effective_format == 'json':
                f.write(output_formatter.format_as_json(results_envelope))
            elif effective_format == 'csv':
                if metadata_summary:
                    f.write(metadata_summary + '\n')
                f.write(output_formatter.format_as_csv(results_envelope))
            elif effective_format == 'tsv':
                if metadata_summary:
                    f.write(metadata_summary + '\n')
                f.write(output_formatter.format_as_tsv(results_envelope))
            elif effective_format == 'stdout':
                if metadata_summary:
                    f.write(metadata_summary + '\n')
                import io
                buf = io.StringIO()
                output_formatter.format_as_console_table(results_envelope, stream=buf)
                f.write(buf.getvalue())
            else:
                logger.error(f"Unknown output format: {effective_format}")
                print(f"Unknown output format: {effective_format}", file=sys.stderr)
                return
        logger.info(f"Saved results for '{query_display_name}' to {output_file_path}")
    else:
        if effective_format == 'json':
            print(output_formatter.format_as_json(results_envelope))
        elif effective_format == 'csv':
            if metadata_summary:
                print(metadata_summary)
            print(output_formatter.format_as_csv(results_envelope))
        elif effective_format == 'tsv':
            if metadata_summary:
                print(metadata_summary)
            print(output_formatter.format_as_tsv(results_envelope))
        elif effective_format == 'stdout':
            if metadata_summary:
                print(metadata_summary)
            output_formatter.format_as_console_table(results_envelope, stream=sys.stdout)
        else:
            logger.error(f"Unknown output format: {effective_format}")
            print(f"Unknown output format: {effective_format}", file=sys.stderr)
            return

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
        # No logger yet, so print to stderr
        print(f"Critical Error: {e}", file=sys.stderr)
        sys.exit(1)

    parser = setup_arg_parser()
    args = parser.parse_args()

    debug = getattr(args, 'debug', False)
    log_file = os.getenv('SQL_APP_LOGFILE', None)
    setup_logging(debug, log_file)  # Ensure logger is configured
    logger = logging.getLogger("tbase_extractor.main")  # Use a specific logger name

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
        sys.exit(1)

    # 3. Output Handling
    if results is not None:  # results can be an empty list
        output_file_path = getattr(args, 'output', None)
        user_format_arg = getattr(args, 'format', None)
        
        # Use the new utility function
        effective_format = determine_output_format(user_format_arg, output_file_path, logger)
          # Enhanced metadata
        metadata_dict = {
            'query_timestamp_utc': query_start_time.isoformat(),
            'query_name': args.query_name if args.action == 'query' else args.action,
            'query_display_name': query_display_name,
            'tool_version': "0.1.0",  # Get from package version
            'execution_duration_ms': execution_duration_ms,
            'row_count_fetched': len(results),
        }
        
        # Base parameters for metadata
        base_params_for_metadata = {
            k: str(v) for k, v in vars(args).items() 
            if k in ['first_name', 'last_name', 'dob', 'patient_id', 'query_name', 'table_name', 'table_schema', 
                    'input_csv', 'id_column']
            and v is not None
        }
        metadata_dict['parameters'] = base_params_for_metadata
        
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
            metadata_dict['status'] = "success_no_data"
        
        handle_output(results, output_file_path, query_display_name, effective_format, metadata_dict)
    
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

# Action handlers dictionary mapping actions to their handler functions
ACTION_HANDLERS = {
    'list-tables': handle_list_tables,
    'query': {
        'get_patient_by_id': handle_get_patient_by_id,
        'patient-by-name-dob': handle_patient_by_name_dob,
        'patient-fuzzy-search': handle_patient_fuzzy_search,
        'get-table-columns': handle_get_table_columns,
    }
}