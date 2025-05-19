import pyodbc
import os
from typing import Optional, List, Dict, Any, Tuple

class SQLInterface:
    """Handles database connection, query execution, and result fetching."""

    def __init__(self, debug: bool = False):
        """Initializes connection parameters from environment variables."""
        self.server: Optional[str] = os.getenv("SQL_SERVER")
        self.database: Optional[str] = os.getenv("DATABASE")
        self.username_sql: Optional[str] = os.getenv("USERNAME_SQL")
        self.password: Optional[str] = os.getenv("PASSWORD")
        # Load driver from .env, provide a default if not set
        self.driver: str = os.getenv("SQL_DRIVER", "{SQL Server Native Client 10.0}")
        self.connection: Optional[pyodbc.Connection] = None
        self.cursor: Optional[pyodbc.Cursor] = None
        self.debug = debug

    def __enter__(self):
        """Context manager entry point: establishes connection."""
        self.connect()
        return self # Return the instance for use within the 'with' block

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point: closes connection."""
        self.close_connection()
        # Return False to propagate any exceptions that occurred within the 'with' block
        return False

    def connect(self) -> bool:
        """
        Establishes a database connection using parameters from environment variables.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        # Avoid reconnecting if already connected (simple check)
        if self.connection is not None:
            # Depending on desired behavior, you might want to check connection.closed
            # or simply log a warning/return True.
            print("Warning: Connection object already exists. Close before reconnecting if needed.")
            return True # Or consider returning False if strict single connection is desired

        # Check for required connection details
        if not all([self.server, self.database, self.username_sql, self.password, self.driver]):
            print("Error: Database connection details incomplete. Check your .env file "
                  "(SQL_SERVER, DATABASE, USERNAME_SQL, PASSWORD, SQL_DRIVER).")
            return False

        try:
            connection_string = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username_sql};"
                f"PWD={self.password};"
            )
            if self.debug:
                masked_pwd = '***' if self.password else None
                print(f"[DEBUG] Connection string: DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username_sql};PWD={masked_pwd}")
                print("[DEBUG] Connecting to database...")
            self.connection = pyodbc.connect(connection_string, autocommit=False)
            self.cursor = self.connection.cursor()
            if self.debug:
                print("[DEBUG] Database connection established.")
            print("Successfully connected to the database.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error connecting to the database: SQLSTATE {sqlstate}")
            print(f"Error details: {ex.args[1]}")
            self.connection = None # Ensure state reflects failure
            self.cursor = None
            return False

    def execute_query(self, query: str, params: Tuple = ()) -> bool:
        """
        Executes a SQL query using parameters to prevent SQL injection.

        Handles potential pyodbc errors during execution and attempts rollback.
        Requires explicit call to commit() for DML operations (INSERT, UPDATE, DELETE).

        Args:
            query (str): The SQL query string with '?' placeholders for parameters.
            params (Tuple): A tuple of parameter values corresponding to the placeholders.

        Returns:
            bool: True if execution was successful (from pyodbc's perspective), False on error.
        """
        if not self.connection or not self.cursor:
            print("Error: Not connected to the database. Cannot execute query.")
            return False
        if self.debug:
            print(f"[DEBUG] Executing query: {query}")
            print(f"[DEBUG] With parameters: {params}")
        try:
            self.cursor.execute(query, params)
            if self.debug:
                print("[DEBUG] Query executed.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            error_message = ex.args[1]
            print(f"Error executing query: SQLSTATE {sqlstate} - {error_message}")
            self._rollback() # Attempt to rollback on execution error
            return False

    def fetch_results(self) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches all results from the last executed query that returned rows (e.g., SELECT).

        Converts results into a list of dictionaries where keys are column names.

        Returns:
            Optional[List[Dict[str, Any]]]: A list of dictionaries representing the rows,
                                            an empty list if the query returned no rows,
                                            or None if an error occurred during fetch or
                                            if the cursor is invalid.
        """
        if not self.cursor:
            print("Error: No cursor available to fetch results.")
            return None

        try:
            # Check if the last execution produced a result set
            if self.cursor.description is None:
                # This typically means it wasn't a SELECT or the SELECT failed before producing results.
                # It could also be a successful INSERT/UPDATE/DELETE.
                # Returning [] is appropriate as no *rows* are available to fetch.
                return []

            columns = [column[0] for column in self.cursor.description]
            # Fetch all rows from the cursor
            rows = self.cursor.fetchall()
            if self.debug:
                print(f"[DEBUG] Rows fetched: {len(rows)}")
            # Convert rows (list of tuples) to list of dictionaries
            return [dict(zip(columns, row)) for row in rows]

        except pyodbc.Error as ex:
            # Catch errors specifically during fetch or description access
            sqlstate = ex.args[0]
            print(f"Error fetching results from cursor: SQLSTATE {sqlstate} - {ex.args[1]}")
            return None

    def commit(self) -> bool:
        """
        Commits the current transaction to the database.

        Should be called after successful DML operations (INSERT, UPDATE, DELETE).

        Returns:
            bool: True if commit was successful, False otherwise.
        """
        if not self.connection:
            print("Error: Cannot commit, no active connection.")
            return False
        try:
            self.connection.commit()
            # print("Transaction committed.") # Optional info message
            return True
        except pyodbc.Error as ex:
            print(f"Error committing transaction: {ex}")
            # Consider attempting rollback here as well if commit fails mid-transaction
            self._rollback()
            return False

    def _rollback(self) -> None:
        """Internal helper method to roll back the current transaction."""
        if self.connection:
            try:
                self.connection.rollback()
                print("Transaction rolled back due to error or explicit request.") # Optional info
            except pyodbc.Error as rollback_ex:
                # Log this prominently - failure during rollback is problematic
                print(f"!!! Critical: Error during transaction rollback: {rollback_ex}")

    def close_connection(self) -> None:
        """Closes the database cursor and connection if they are open."""
        if self.debug:
            print("[DEBUG] Closing database connection and cursor...")
        if self.cursor:
            try:
                self.cursor.close()
            except pyodbc.Error as ex:
                print(f"Warning: Error closing cursor: {ex}")
            finally:
                self.cursor = None # Ensure cursor is None regardless of close success

        if self.connection:
            try:
                # Rollback any pending changes before closing if not explicitly committed/rolled back
                # self._rollback() # Optional: Decide if implicit rollback on close is desired
                self.connection.close()
                print("Connection closed.")
            except pyodbc.Error as ex:
                print(f"Warning: Error closing connection: {ex}")
            finally:
                self.connection = None # Ensure connection is None regardless of close success
        if self.debug:
            print("[DEBUG] Database connection closed.")