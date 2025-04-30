import argparse
import os
import sys
import pyodbc  # Import pyodbc to catch its specific errors if necessary
from dotenv import load_dotenv

# --- Load Environment Variables ---
# It's good practice to load this early, before importing custom modules
# that might rely on these variables during their import time (though less common).
load_dotenv()

# --- Import Custom Modules ---
# These imports should happen *after* load_dotenv if the modules rely on
# environment variables at import time (e.g., for defining constants).
# In this case, the modules load env vars within their methods/init, so order is flexible.
try:
    from sql_interface.db_interface import SQLInterface
    from sql_interface.query_manager import QueryManager, QueryTemplateNotFoundError
    from sql_interface.output_formatter import OutputFormatter
    # Import custom exceptions if you created exceptions.py
    # from sql_interface.exceptions import *
except ImportError as e:
    print(f"Error: Failed to import necessary modules from 'sql_interface'.", file=sys.stderr)
    print(f"Ensure the 'sql_interface' directory exists and contains __init__.py.", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    sys.exit(1)

# --- Constants ---
SQL_TEMPLATES_DIR = 'sql_templates' # Relative path to the SQL templates directory

def setup_arg_parser():
    """Configures and returns the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Connects to a SQL database to execute predefined queries using templates.",
        formatter_class=argparse.RawTextHelpFormatter # Allows for newlines in help text
    )

    # Use subparsers to create distinct actions like 'list-tables' and 'query'
    subparsers = parser.add_subparsers(
        dest='action',
        help='The main action to perform.',
        required=True, # An action must be specified
        metavar='ACTION'
    )

    # --- Sub-command: list-tables ---
    parser_list = subparsers.add_parser(
        'list-tables',
        help='List all available base tables in the connected database.'
        # No specific arguments needed for list-tables in this version
    )
    # Add arguments specific to list-tables here if needed in the future (e.g., --schema-filter)

    # --- Sub-command: query ---
    parser_query = subparsers.add_parser(
        'query',
        help='Execute a predefined query template.'
    )
    parser_query.add_argument(
        '--query-name', '-q',
        required=True,
        # Define available query templates here. These should match filenames in sql_templates (without .sql)
        choices=['patient-details', 'patient-visits'], # Example: Add more as you create templates
        help="REQUIRED. The name of the predefined query template to execute.\n"
             "Must correspond to a file in the 'sql_templates' directory.\n"
             "Examples:\n"
             "  'patient-details': Get details for a specific patient.\n"
             "  'patient-visits': Get patient details and their visit info (JOIN)."
    )
    # Arguments for parameters needed by the queries
    parser_query.add_argument(
        '--patient-id', '-i',
        type=int,
        metavar='ID',
        help='Patient ID (required for \'patient-details\' and \'patient-visits\' queries).'
    )
    # Add more specific parameter arguments here as needed by other query templates
    # e.g., --start-date, --order-number, etc.

    # Output option common to the 'query' action
    parser_query.add_argument(
        '--output', '-o',
        type=str,
        metavar='FILE_PATH',
        help='Optional path to save results as a JSON file.'
    )
    # Add arguments for other output formats here if OutputFormatter supports them (e.g., --format csv)

    return parser

def handle_output(results: list, output_file: str | None, query_name: str):
    """Handles formatting and saving/printing the results."""
    output_formatter = OutputFormatter()

    if output_file:
        print(f"\nSaving results for '{query_name}' to {output_file}...")
        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                print(f"Creating directory: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)

            # Format data as JSON
            json_output = output_formatter.format_as_json(results)

            # Write to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_output)
            print(f"Successfully saved results to {output_file}")

        except IOError as e:
            print(f"Error: Could not write to file {output_file}: {e}", file=sys.stderr)
        except Exception as e: # Catch potential JSON or other errors
            print(f"Error: An unexpected error occurred during output formatting or saving: {e}", file=sys.stderr)
    elif results: # Only print to console if there are results
        print("\n--- Query Results ---")
        output_formatter.format_as_console_table(results)
    # else: # results is an empty list, message handled earlier
        # print("Query returned no results.")


def main():
    """Main execution function: parses args, gets query, connects, executes, handles output."""
    parser = setup_arg_parser()
    args = parser.parse_args()

    query_manager = QueryManager(SQL_TEMPLATES_DIR)
    sql: str = ""
    params: tuple = ()
    query_info: tuple | None = None # Holds (sql, params) from query_manager
    query_display_name = args.action # Default display name

    # --- 1. Determine SQL Query and Parameters based on Action/Arguments ---
    try:
        if args.action == 'list-tables':
            query_info = query_manager.get_list_tables_query()
            query_display_name = 'List Tables'

        elif args.action == 'query':
            query_display_name = f"Query '{args.query_name}'" # More specific name

            # --- Parameter Validation and Query Retrieval for 'query' action ---
            if args.query_name == 'patient-details':
                if args.patient_id is None:
                    parser.error("Argument --patient-id/-i is REQUIRED for query 'patient-details'.")
                query_info = query_manager.get_patient_by_id_query(args.patient_id)
            elif args.query_name == 'patient-visits':
                if args.patient_id is None:
                    parser.error("Argument --patient-id/-i is REQUIRED for query 'patient-visits'.")
                query_info = query_manager.get_patient_visits_query(args.patient_id)
            # --- Add elif blocks for other query names ---
            # elif args.query_name == 'some-other-query':
            #     if args.some_param is None:
            #         parser.error("Argument --some-param is REQUIRED for query 'some-other-query'.")
            #     query_info = query_manager.get_some_other_query(args.some_param)
            else:
                # This case should be caught by argparse 'choices' if configured correctly,
                # but good to have a fallback.
                print(f"Error: Query name '{args.query_name}' is not recognized or implemented.", file=sys.stderr)
                sys.exit(1)

            # Check if query loading itself failed (e.g., template not found)
            if query_info is None:
                print(f"Error: Failed to load or prepare query '{args.query_name}'. "
                      f"Check template exists and parameters are correct.", file=sys.stderr)
                sys.exit(1)

        # If we reached here and query_info is still None, it's an unexpected state
        if query_info is None:
             print(f"Error: Could not determine query for action '{args.action}'.", file=sys.stderr)
             sys.exit(1)

        sql, params = query_info

    except QueryTemplateNotFoundError as e: # Catch specific exception from QueryManager
        print(f"Error: {e}", file=sys.stderr)
        print(f"Please ensure '{SQL_TEMPLATES_DIR}' directory exists and contains the required '.sql' file.", file=sys.stderr)
        sys.exit(1)
    except Exception as e: # Catch other unexpected errors during query preparation
        print(f"An unexpected error occurred during query preparation: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 2. Connect to DB, Execute Query, and Fetch Results ---
    results = None
    print(f"\nAttempting to execute: {query_display_name}")
    if params:
         print(f"With parameters: {params}")

    try:
        # Use the SQLInterface context manager for automatic connection closing
        with SQLInterface() as db:
            # Connection success is checked within __enter__/connect methods
            if not db.connection:
                print("Aborting: Database connection failed.", file=sys.stderr)
                sys.exit(1) # Exit if connection failed

            # Execute the query
            if db.execute_query(sql, params):
                # Fetch results only if execution seems successful
                # fetch_results handles non-SELECT cases returning []
                fetched_data = db.fetch_results()

                if fetched_data is not None: # Indicates fetch attempt didn't error
                    results = fetched_data
                    if not results:
                        # Provide specific feedback if a query known to need params returns nothing
                        if args.action == 'query' and args.patient_id is not None:
                            print(f"Query executed successfully, but no data found for Patient ID {args.patient_id}.")
                        else:
                            print("Query executed successfully, but returned no results.")
                else:
                    # Error message already printed by fetch_results
                    print("Error occurred while fetching results.", file=sys.stderr)
                    # Consider exiting or just proceeding without results

            else:
                # Error message already printed by execute_query
                print("Aborting: Query execution failed.", file=sys.stderr)
                # Often good to exit if execution fails
                sys.exit(1)

            # --- Explicit Commit/Rollback (Example for future DML) ---
            # if args.action == 'some-update-action':
            #     if execution_success: # If execute_query returned True
            #         if db.commit():
            #             print("Changes committed successfully.")
            #         else:
            #             print("Error: Failed to commit changes.", file=sys.stderr)
            #             # Rollback already attempted in execute_query on error,
            #             # but might be needed here if commit fails.
            #     # No else needed here, as rollback is handled on execution error

    except pyodbc.Error as db_err:
        # Catch potential pyodbc errors not handled deeply within SQLInterface methods
        # (e.g., during commit, context manager exit, etc.)
        print(f"\nA database error occurred: {db_err}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Catch any other unexpected errors during the database interaction block
        print(f"\nAn unexpected error occurred during database interaction: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc() # Print detailed traceback for unexpected errors
        sys.exit(1)

    # --- 3. Handle Output (if results were obtained) ---
    if results is not None:
        output_file_path = getattr(args, 'output', None) # Get output file if provided for the action
        handle_output(results, output_file_path, query_display_name)

    print(f"\n--- {query_display_name} finished ---")


if __name__ == "__main__":
    main()