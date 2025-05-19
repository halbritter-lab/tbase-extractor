# main.py
import argparse
import os
import sys
import pyodbc
from datetime import datetime, date # Import datetime for strptime
from dotenv import load_dotenv
import logging

load_dotenv()

try:
    from sql_interface.db_interface import SQLInterface
    from sql_interface.query_manager import QueryManager, QueryTemplateNotFoundError
    from sql_interface.output_formatter import OutputFormatter
    # from sql_interface.exceptions import * # Keep if using custom exceptions
except ImportError as e:
    print(f"Error: Failed to import necessary modules from 'sql_interface'.", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    sys.exit(1)

SQL_TEMPLATES_DIR = 'sql_templates'
DOB_FORMAT = "%Y-%m-%d" # Define the expected date format

def setup_arg_parser():
    """Configures and returns the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Connects to a SQL database to execute predefined queries using templates.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--debug', '-v',
        action='store_true',
        help='Enable verbose debug output.'
    )
    subparsers = parser.add_subparsers(
        dest='action', help='The main action to perform.', required=True, metavar='ACTION'
    )

    # --- Sub-command: list-tables ---
    subparsers.add_parser('list-tables', help='List all available base tables.')

    # --- Sub-command: query ---
    parser_query = subparsers.add_parser('query', help='Execute a predefined query template.')
    parser_query.add_argument(
        '--query-name', '-q',
        required=True,
        # --- UPDATED CHOICES ---
        choices=['patient-details', 'patient-by-name-dob'], # Removed 'patient-visits'
        help="REQUIRED. The name of the predefined query template to execute.\n"
             "Must correspond to a file in the 'sql_templates' directory.\n"
             "Examples:\n"
             "  'patient-details': Get details for a specific patient by ID.\n"
             "  'patient-by-name-dob': Get patient details by Name and Date of Birth." # Updated help text
    )
    # --- Parameter arguments ---
    parser_query.add_argument(
        '--patient-id', '-i', type=int, metavar='ID',
        help='Patient ID (required for \'patient-details\' query).'
    )
    # --- NEW ARGUMENTS for Name/DOB query ---
    parser_query.add_argument(
        '--first-name', '-fn', type=str, metavar='NAME',
        help='Patient First Name (Vorname) (required for \'patient-by-name-dob\' query).'
    )
    parser_query.add_argument(
        '--last-name', '-ln', type=str, metavar='NAME',
        help='Patient Last Name (Name) (required for \'patient-by-name-dob\' query).'
    )
    parser_query.add_argument(
        '--dob', '-d', type=str, metavar='YYYY-MM-DD',
        help=f'Patient Date of Birth (Geburtsdatum) in {DOB_FORMAT} format '
             f'(required for \'patient-by-name-dob\' query).'
    )
    # --- Output argument (common) ---
    parser_query.add_argument(
        '--output', '-o', type=str, metavar='FILE_PATH',
        help='Optional path to save results as a JSON file.'
    )
    parser_query.add_argument(
        '--format', '-f',
        type=str,
        choices=['json', 'csv', 'tsv', 'stdout'],
        default=None,  # Changed from 'stdout' to None
        help='Output format: json, csv, tsv, or stdout (pretty table to console). Inferred from -o extension if not set.'
    )
    parser_query.add_argument(
        '--debug', '-v',
        action='store_true',
        help='Enable verbose debug output.'
    )
    return parser

def handle_output(results_envelope, output_file_path, query_display_name, effective_format, metadata_dict=None):
    """Handles formatting and saving/printing the results with format selection and metadata."""
    import logging
    logger = logging.getLogger("main")
    output_formatter = OutputFormatter()
    formatted = ''
    # Metadata summary (optional, for stdout/csv/tsv)
    metadata_summary = ''
    if metadata_dict:
        metadata_lines = [f"# {k}: {v}" for k, v in metadata_dict.items()]
        metadata_summary = '\n'.join(metadata_lines)
    # Output routing
    if output_file_path:
        # Write to file
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
                # Write pretty table to file
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
        # Output to stdout
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

# Logging configuration (set up early)
def setup_logging(debug: bool = False, log_file: str = None):
    """Configure logging for the application."""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
        force=True  # Overwrite any prior config (for repeated runs in notebooks etc.)
    )

def main():
    """Main execution function."""
    parser = setup_arg_parser()
    args = parser.parse_args()

    debug = getattr(args, 'debug', False)
    # Optional: allow user to specify log file via env or argument
    log_file = os.getenv('SQL_APP_LOGFILE', None)
    setup_logging(debug, log_file)
    logger = logging.getLogger("main")

    logger.debug(f"Parsed arguments: {args}")

    query_manager = QueryManager(SQL_TEMPLATES_DIR)
    sql: str = ""
    params: tuple = ()
    query_info: tuple | None = None
    query_display_name = args.action

    # --- 1. Determine SQL Query and Parameters ---
    try:
        if args.action == 'list-tables':
            query_info = query_manager.get_list_tables_query()
            query_display_name = 'List Tables'

        elif args.action == 'query':
            query_display_name = f"Query '{args.query_name}'"

            if args.query_name == 'patient-details':
                if args.patient_id is None:
                    logger.error("Argument --patient-id/-i is REQUIRED for query 'patient-details'.")
                    parser.error("Argument --patient-id/-i is REQUIRED for query 'patient-details'.")
                query_info = query_manager.get_patient_by_id_query(args.patient_id)

            # --- NEW BLOCK for Name/DOB Query ---
            elif args.query_name == 'patient-by-name-dob':
                # Check required arguments
                if not all([args.first_name, args.last_name, args.dob]):
                    logger.error("Arguments --first-name/-fn, --last-name/-ln, and --dob/-d are REQUIRED for query 'patient-by-name-dob'.")
                    parser.error("Arguments --first-name/-fn, --last-name/-ln, and --dob/-d are REQUIRED "
                                 "for query 'patient-by-name-dob'.")

                # Validate DOB format
                dob_object: date | None = None
                try:
                    dob_object = datetime.strptime(args.dob, DOB_FORMAT).date()
                except ValueError:
                    logger.error(f"Invalid Date of Birth format for --dob/-d. Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")
                    parser.error(f"Invalid Date of Birth format for --dob/-d. "
                                 f"Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")

                # We know dob_object is a date object here if no error occurred
                query_info = query_manager.get_patient_by_name_dob_query(
                    args.first_name, args.last_name, dob_object
                )
            # --- REMOVED 'patient-visits' block ---

            else:
                logger.error(f"Query name '{args.query_name}' is not recognized or implemented.")
                print(f"Error: Query name '{args.query_name}' is not recognized or implemented.", file=sys.stderr)
                sys.exit(1)

            # Check if query loading/preparation failed
            if query_info is None:
                logger.error(f"Failed to load or prepare query '{args.query_name}'.")
                print(f"Error: Failed to load or prepare query '{args.query_name}'.", file=sys.stderr)
                sys.exit(1)

        if query_info is None:
             logger.error(f"Could not determine query for action '{args.action}'.")
             print(f"Error: Could not determine query for action '{args.action}'.", file=sys.stderr)
             sys.exit(1)

        sql, params = query_info

    except QueryTemplateNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during query preparation: {e}", file=sys.stderr)
        sys.exit(1)

    if debug:
        logger.debug(f"SQL to execute:\n{sql}")
        logger.debug(f"Query parameters: {params}")

    # --- 2. Connect, Execute, Fetch ---
    results = None
    logger.info(f"Attempting to execute: {query_display_name}")
    if params:
         logger.info(f"With parameters: {params}") # DOB will print as YYYY-MM-DD
    if debug:
        logger.debug("Attempting database connection...")

    try:
        with SQLInterface(debug=debug) as db:
            if not db.connection:
                logger.error("Aborting: Database connection failed.")
                print("Aborting: Database connection failed.", file=sys.stderr)
                sys.exit(1)

            if db.execute_query(sql, params):
                if debug:
                    logger.debug("Query executed successfully. Fetching results...")
                fetched_data = db.fetch_results()
                if fetched_data is not None:
                    results = fetched_data
                    if debug:
                        logger.debug(f"Number of rows fetched: {len(results)}")
                    if not results:
                         # --- UPDATED 'no results' message ---
                         if args.action == 'query':
                            if args.query_name == 'patient-details':
                                logger.info(f"Query executed successfully, but no data found for Patient ID {args.patient_id}.")
                            elif args.query_name == 'patient-by-name-dob':
                                logger.info(f"Query executed successfully, but no data found for "
                                      f"FirstName='{args.first_name}', LastName='{args.last_name}', DOB='{args.dob}'.")
                            else: # General case
                                 logger.info("Query executed successfully, but returned no results.")
                         else: # For list-tables etc. if they return empty
                            logger.info("Query executed successfully, but returned no results.")
                else:
                    logger.error("Error occurred while fetching results.")
                    print("Error occurred while fetching results.", file=sys.stderr)
            else:
                logger.error("Aborting: Query execution failed.")
                print("Aborting: Query execution failed.", file=sys.stderr)
                sys.exit(1)

    except pyodbc.Error as db_err:
        logger.exception(f"A database error occurred: {db_err}")
        print(f"\nA database error occurred: {db_err}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"An unexpected error occurred during database interaction: {e}")
        print(f"\nAn unexpected error occurred during database interaction: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # --- 3. Handle Output ---
    if results is not None:
        output_file_path = getattr(args, 'output', None)
        user_format = getattr(args, 'format', None)
        # --- Output format inference logic ---
        output_destination_is_file = output_file_path is not None
        effective_format = None
        logger = logging.getLogger("main")
        if user_format is not None:
            effective_format = user_format
        elif output_destination_is_file:
            _filename, ext = os.path.splitext(output_file_path)
            ext = ext.lower()
            if ext == '.json':
                effective_format = 'json'
            elif ext == '.csv':
                effective_format = 'csv'
            elif ext == '.tsv':
                effective_format = 'tsv'
            else:
                effective_format = 'json'
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
        else:
            effective_format = 'stdout'
        # Optionally, build metadata_dict (example: row count)
        metadata_dict = {'row_count': len(results)} if results is not None else None
        handle_output(results, output_file_path, query_display_name, effective_format, metadata_dict)
    print(f"\n--- {query_display_name} finished ---")

if __name__ == "__main__":
    main()