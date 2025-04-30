import pyodbc
from dotenv import load_dotenv
import os # os module is already imported
import argparse # Import the argparse module
import json     # Import the json module for saving results
from datetime import datetime, date # Import datetime and date for type checking

# Load environment variables from .env file
load_dotenv()

class SQLInterface:
    def __init__(self):
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("DATABASE")
        self.username_sql = os.getenv("USERNAME_SQL")
        self.password = os.getenv("PASSWORD")
        self.connection = None
        self.cursor = None
        # Kept the driver as requested
        self.driver = "{SQL Server Native Client 10.0}" # Consider moving this to .env if it can change

    # Add __enter__ and __exit__ for context manager support (using 'with')
    def __enter__(self):
        self.connect()
        return self # Return the instance itself

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        # Returning False propagates exceptions, True suppresses them
        # You typically want to propagate exceptions for debugging
        return False

    def connect(self):
        """Establishes a database connection."""
        if self.connection is not None and not self.connection.closed:
            print("Connection already open.")
            return True

        if not all([self.server, self.database, self.username_sql, self.password, self.driver]):
            print("Database connection details incomplete. Check your .env file or driver setting.")
            return False

        try:
            self.connection_string = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username_sql};"
                f"PWD={self.password};"
            )
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            print("Successfully connected to the database.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error connecting to the database: SQLSTATE {sqlstate}")
            # Print full error for more details
            print(f"Error details: {ex.args[1]}")
            self.connection = None # Ensure connection is None on failure
            self.cursor = None
            return False

    def execute_query(self, query, *params):
        """Executes a SQL query. Use parameters to prevent SQL injection."""
        if self.connection is None or self.connection.closed or self.cursor is None:
            print("Not connected to the database. Cannot execute query.")
            return False

        try:
            # Execute the query with parameters. If no params, *params is an empty tuple.
            self.cursor.execute(query, params)

            # Note: For SELECT statements, you don't commit.
            # For INSERT, UPDATE, DELETE, you need to call self.connection.commit()
            # *after* calling execute_query.
            # The class structure could be refined further with separate methods
            # for DML (Data Manipulation Language) and DQL (Data Query Language)
            # if needed, to handle commits automatically for DML.
            # For now, the caller should handle committing DML.

            # print("Query execution attempted.") # Optional: Add print for success
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            error_message = ex.args[1]
            print(f"Error executing query: SQLSTATE {sqlstate} - {error_message}")
            # Rollback changes if this was a DML statement and it failed
            if self.connection:
                 try:
                     self.connection.rollback()
                     # print("Transaction rolled back.") # Optional: Add print
                 except pyodbc.Error as rollback_ex:
                     print(f"Error during rollback: {rollback_ex}")
            return False


    def fetch_results(self):
        """Fetches all results from the last executed SELECT query."""
        if self.cursor is None:
            print("No cursor available to fetch results.")
            return None

        try:
            # Check if there are results to fetch (e.g., after a SELECT)
            if self.cursor.description is None:
                 # This wasn't a SELECT query that returns results
                 # print("No results to fetch (query was likely not a SELECT).")
                 return [] # Return empty list if no results are expected

            columns = [column[0] for column in self.cursor.description]
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return results
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error fetching results: SQLSTATE {sqlstate}")
            return None

    def list_tables(self):
        """Lists all base tables in the current database."""
        table_list_query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME;
        """
        print("\nAttempting to list tables...")
        if self.execute_query(table_list_query):
            tables = self.fetch_results()
            if tables is not None: # fetch_results returns None on error
                print("Successfully retrieved table list.")
                return tables
            else:
                print("Failed to fetch table list results.")
                return None
        else:
            print("Failed to execute query to list tables.")
            return None


    def close_connection(self):
        """Closes the cursor and connection."""
        # Check if connection exists and is not already closed before trying to close
        if self.cursor:
            try:
                self.cursor.close()
                # print("Cursor closed.") # Optional: Add print
            except pyodbc.Error as ex:
                 print(f"Error closing cursor: {ex}") # Corrected line
            finally:
                 self.cursor = None # Ensure cursor is None after attempt

        if self.connection:
            try:
                if not self.connection.closed:
                    self.connection.close()
                    print("Connection closed.")
            except pyodbc.Error as ex:
                 print(f"Error closing connection: {ex}") # Corrected line
            finally:
                 self.connection = None # Ensure connection is None after attempt


# --- Main Execution Block ---

if __name__ == "__main__":
    # --- Set up command-line argument parsing ---
    parser = argparse.ArgumentParser(description="Connects to a SQL database and performs actions like querying patient information or listing tables.")

    # Create a mutually exclusive group for actions (either query OR list)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--patient-id', '-i', type=int,
                       help='The integer ID of the patient to query.')
    group.add_argument('--list-tables', '-l', action='store_true',
                       help='List all available tables in the database.')

    # Add the optional output file parameter (not part of the exclusive group)
    parser.add_argument('--output', '-o', type=str,
                        help='Optional path to a file to save the query results (JSON format). Only applies with --patient-id.')


    args = parser.parse_args()

    # Using the 'with' statement ensures the connection is closed automatically
    # even if errors occur.
    with SQLInterface() as sql_interface:
        # The __enter__ method attempts to connect. Check if it succeeded.
        if sql_interface.connection:

            # --- Perform action based on which argument was provided ---

            # Check if the list_tables argument was provided
            if args.list_tables:
                print("\n--- Listing Tables ---")
                # Note: Table listing output always goes to console
                tables = sql_interface.list_tables()
                if tables:
                    print("Available tables (schema.name):")
                    for table in tables:
                        # Check for keys before accessing in case of unexpected result format
                        schema = table.get('TABLE_SCHEMA', 'N/A')
                        name = table.get('TABLE_NAME', 'N/A')
                        print(f"  {schema}.{name}")
                    print("--- End Table List ---")
                elif tables is not None: # Means list was empty, not an error
                    print("No tables found in the database (or user lacks permissions to see them).")
                else: # Means there was an error listing tables
                    print("Could not retrieve table list due to an error.")

            # Check if the patient-id argument was provided (it won't be None if used)
            elif args.patient_id is not None:
                patient_id_to_find = args.patient_id

                print(f"\n--- Executing Specific Patient Query for PatientID {patient_id_to_find} ---")
                # Define your specific SQL query using parameters (?)
                # Selects all columns (*) for the specified patient ID
                your_query = """
                SELECT
                    T1.* -- Selecting all columns
                FROM
                    dbo.Patient AS T1 -- **Verify this name against the listed tables!**
                WHERE
                    T1.PatientID = ?
                ;
                """

                # Define the parameter value(s) - MUST be a tuple or list even for one parameter
                params = (patient_id_to_find,) # Note the comma for a single-element tuple

                # Execute the query using parameters (safer!)
                if sql_interface.execute_query(your_query, *params):
                    # If execution was attempted (doesn't mean rows returned for SELECT)
                    results = sql_interface.fetch_results()

                    if results:
                        # --- Handle Output ---
                        if args.output:
                            # Save to file if -o is used
                            try:
                                # Extract the directory path from the output file path
                                output_dir = os.path.dirname(args.output)
                                # Create the directory if it doesn't exist
                                if output_dir and not os.path.exists(output_dir):
                                    print(f"Creating directory: {output_dir}")
                                    os.makedirs(output_dir, exist_ok=True) # exist_ok=True prevents error if dir exists

                                # Prepare results for JSON serialization
                                serializable_results = []
                                for row in results:
                                    serializable_row = {}
                                    for key, value in row.items():
                                        # Convert datetime.datetime or datetime.date objects to ISO 8601 strings
                                        if isinstance(value, (datetime, date)):
                                            serializable_row[key] = value.isoformat()
                                        else:
                                            serializable_row[key] = value
                                    serializable_results.append(serializable_row)

                                with open(args.output, 'w') as f:
                                    json.dump(serializable_results, f, indent=4) # Use indent for pretty printing
                                print(f"Query results saved to {args.output}")
                            except IOError as e:
                                print(f"Error saving results to file {args.output}: {e}")
                            except Exception as e: # Catch other potential errors during serialization
                                print(f"An unexpected error occurred during JSON serialization: {e}")

                        else:
                            # Print to console if -o is not used
                            print("\nQuery Results:")
                            for row in results:
                                print(row)
                        # --- End Handle Output ---

                    else:
                        # If fetch_results returns an empty list after execute_query succeeded for a SELECT
                        # This message always goes to console as it's a status update
                        if sql_interface.cursor is not None and sql_interface.cursor.description is not None:
                             print(f"\nNo patient found with PatientID {patient_id_to_find}.")
                        # If description is None, execute_query failed or wasn't a SELECT,
                        # the error would have been printed by execute_query

                print("--- End Specific Patient Query ---")

            # If the connection failed, the above blocks won't execute anyway
        else:
            print("\nCould not connect to the database. Skipping actions.")

    # The connection is automatically closed when exiting the 'with' block

    print("\nScript finished.")