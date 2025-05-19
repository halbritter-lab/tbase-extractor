# main.py
import argparse
import os
import sys
import pyodbc
from datetime import datetime, date # Import datetime for strptime
from dotenv import load_dotenv

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
        default='stdout',
        help='Output format: json, csv, tsv, or stdout (pretty table to console). Default: stdout.'
    )
    parser_query.add_argument(
        '--debug', '-v',
        action='store_true',
        help='Enable verbose debug output.'
    )
    return parser

def handle_output(results: list, output_file: str | None, query_name: str, output_format: str):
    """Handles formatting and saving/printing the results with format selection."""
    output_formatter = OutputFormatter()
    formatted = ''
    if output_format == 'json':
        formatted = output_formatter.format_as_json(results)
    elif output_format == 'csv':
        formatted = output_formatter.format_as_csv(results)
    elif output_format == 'tsv':
        formatted = output_formatter.format_as_tsv(results)
    elif output_format == 'stdout':
        # Pretty table to console
        if results:
            print("\n--- Query Results ---")
            output_formatter.format_as_console_table(results)
        else:
            print("No results to display.")
        return
    else:
        print(f"Unknown output format: {output_format}", file=sys.stderr)
        return

    if output_file:
        print(f"\nSaving results for '{query_name}' to {output_file}...")
        try:
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                print(f"Creating directory: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                f.write(formatted)
            print(f"Successfully saved results to {output_file}")
        except IOError as e:
            print(f"Error: Could not write to file {output_file}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Error: An unexpected error occurred during output formatting or saving: {e}", file=sys.stderr)
    elif formatted:
        print(formatted)

def main():
    """Main execution function."""
    parser = setup_arg_parser()
    args = parser.parse_args()

    debug = getattr(args, 'debug', False)

    if debug:
        print(f"[DEBUG] Parsed arguments: {args}")

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
                    parser.error("Argument --patient-id/-i is REQUIRED for query 'patient-details'.")
                query_info = query_manager.get_patient_by_id_query(args.patient_id)

            # --- NEW BLOCK for Name/DOB Query ---
            elif args.query_name == 'patient-by-name-dob':
                # Check required arguments
                if not all([args.first_name, args.last_name, args.dob]):
                    parser.error("Arguments --first-name/-fn, --last-name/-ln, and --dob/-d are REQUIRED "
                                 "for query 'patient-by-name-dob'.")

                # Validate DOB format
                dob_object: date | None = None
                try:
                    dob_object = datetime.strptime(args.dob, DOB_FORMAT).date()
                except ValueError:
                    parser.error(f"Invalid Date of Birth format for --dob/-d. "
                                 f"Please use '{DOB_FORMAT}' (e.g., 1990-12-31).")

                # We know dob_object is a date object here if no error occurred
                query_info = query_manager.get_patient_by_name_dob_query(
                    args.first_name, args.last_name, dob_object
                )
            # --- REMOVED 'patient-visits' block ---

            else:
                print(f"Error: Query name '{args.query_name}' is not recognized or implemented.", file=sys.stderr)
                sys.exit(1)

            # Check if query loading/preparation failed
            if query_info is None:
                print(f"Error: Failed to load or prepare query '{args.query_name}'.", file=sys.stderr)
                sys.exit(1)

        if query_info is None:
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
        print(f"[DEBUG] SQL to execute:\n{sql}")
        print(f"[DEBUG] Query parameters: {params}")

    # --- 2. Connect, Execute, Fetch ---
    results = None
    print(f"\nAttempting to execute: {query_display_name}")
    if params:
         print(f"With parameters: {params}") # DOB will print as YYYY-MM-DD
    if debug:
        print("[DEBUG] Attempting database connection...")

    try:
        with SQLInterface(debug=debug) as db:
            if not db.connection:
                print("Aborting: Database connection failed.", file=sys.stderr)
                sys.exit(1)

            if db.execute_query(sql, params):
                if debug:
                    print("[DEBUG] Query executed successfully. Fetching results...")
                fetched_data = db.fetch_results()
                if fetched_data is not None:
                    results = fetched_data
                    if debug:
                        print(f"[DEBUG] Number of rows fetched: {len(results)}")
                    if not results:
                         # --- UPDATED 'no results' message ---
                         if args.action == 'query':
                            if args.query_name == 'patient-details':
                                print(f"Query executed successfully, but no data found for Patient ID {args.patient_id}.")
                            elif args.query_name == 'patient-by-name-dob':
                                print(f"Query executed successfully, but no data found for "
                                      f"FirstName='{args.first_name}', LastName='{args.last_name}', DOB='{args.dob}'.")
                            else: # General case
                                 print("Query executed successfully, but returned no results.")
                         else: # For list-tables etc. if they return empty
                            print("Query executed successfully, but returned no results.")
                else:
                    print("Error occurred while fetching results.", file=sys.stderr)
            else:
                print("Aborting: Query execution failed.", file=sys.stderr)
                sys.exit(1)

    except pyodbc.Error as db_err:
        print(f"\nA database error occurred: {db_err}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nAn unexpected error occurred during database interaction: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # --- 3. Handle Output ---
    if results is not None:
        output_file_path = getattr(args, 'output', None)
        output_format = getattr(args, 'format', 'stdout')
        handle_output(results, output_file_path, query_display_name, output_format)

    print(f"\n--- {query_display_name} finished ---")

if __name__ == "__main__":
    main()