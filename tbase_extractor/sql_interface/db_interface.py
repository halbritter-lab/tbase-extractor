import html
import re
from bs4 import BeautifulSoup
import pyodbc
import os
import logging
from typing import Optional, List, Dict, Any, Tuple

# Initialize logger
logger = logging.getLogger(__name__)

class SQLInterface:
    """Handles database connection, query execution, and result fetching."""

    @staticmethod
    def _clean_field_value(value: Any) -> Any:
        """
        Cleans a field value by removing HTML tags and standardizing newlines.

        Args:
            value (Any): The value to clean, typically a string from a database field.

        Returns:
            Any: The cleaned value if the input was a string, otherwise the original value.
        """
        if not isinstance(value, str):
            return value

        # First, unescape HTML entities (e.g., Ä -> Ä)
        text = html.unescape(value)

        # Replace <br> tags with newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)

        # Remove all other HTML tags using BeautifulSoup
        text = BeautifulSoup(text, "html.parser").get_text(separator='\n')

        # Normalize multiple consecutive newlines to a single newline
        text = re.sub(r'\n\s*\n+', '\n', text)

        # Remove leading and trailing whitespace
        return text.strip()

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
            logger.warning("Connection object already exists. Close before reconnecting if needed.")
            return True

        # Check for required connection details
        if not all([self.server, self.database, self.username_sql, self.password, self.driver]):
            logger.error("Database connection details incomplete. Check your .env file "
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
                logger.debug(f"Connection string: DRIVER={self.driver};SERVER={self.server};"
                          f"DATABASE={self.database};UID={self.username_sql};PWD={masked_pwd}")
                logger.debug("Connecting to database...")
            self.connection = pyodbc.connect(connection_string, autocommit=False)
            self.cursor = self.connection.cursor()
            if self.debug:
                logger.debug("Database connection established.")
            logger.info("Successfully connected to the database.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logger.error(f"Error connecting to the database: SQLSTATE {sqlstate}")
            logger.error(f"Error details: {ex.args[1]}")
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
            logger.error("Not connected to the database. Cannot execute query.")
            return False
        if self.debug:
            logger.debug(f"Executing query: {query}")
            logger.debug(f"With parameters: {params}")
        try:
            self.cursor.execute(query, params)
            if self.debug:
                logger.debug("Query executed successfully.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            error_message = ex.args[1]
            logger.error(f"Error executing query: SQLSTATE {sqlstate} - {error_message}")
            self._rollback() # Attempt to rollback on execution error
            return False

    def fetch_results(self) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches all results from the last executed query that returned rows (e.g., SELECT).

        Converts results into a list of dictionaries where keys are column names.

        Automatically cleans any string values to remove HTML and normalize newlines.

        Returns:
            Optional[List[Dict[str, Any]]]: A list of dictionaries representing the rows,
                                            an empty list if the query returned no rows,
                                            or None if an error occurred during fetch or
                                            if the cursor is invalid.
        """
        if not self.cursor:
            logger.error("No cursor available to fetch results.")
            return None

        try:
            # Check if the last execution produced a result set
            if self.cursor.description is None:
                # This typically means it wasn't a SELECT or the SELECT failed before producing results.
                # It could also be a successful INSERT/UPDATE/DELETE.
                return []

            columns = [column[0] for column in self.cursor.description]
            # Fetch all rows from the cursor
            rows = self.cursor.fetchall()
            if self.debug:
                logger.debug(f"Rows fetched: {len(rows)}")
            
            # Convert rows to list of dictionaries, cleaning any string values
            cleaned_results = []
            for row in rows:
                cleaned_row = {}
                for col, val in zip(columns, row):
                    cleaned_row[col] = self._clean_field_value(val)
                cleaned_results.append(cleaned_row)
            return cleaned_results

        except pyodbc.Error as ex:
            # Catch errors specifically during fetch or description access
            sqlstate = ex.args[0]
            logger.error(f"Error fetching results from cursor: SQLSTATE {sqlstate} - {ex.args[1]}")
            return None

    def commit(self) -> bool:
        """
        Commits the current transaction to the database.

        Should be called after successful DML operations (INSERT, UPDATE, DELETE).

        Returns:
            bool: True if commit was successful, False otherwise.
        """
        if not self.connection:
            logger.error("Cannot commit, no active connection.")
            return False
        try:
            self.connection.commit()
            return True
        except pyodbc.Error as ex:
            logger.error(f"Error committing transaction: {ex}")
            # Consider attempting rollback here as well if commit fails mid-transaction
            self._rollback()
            return False

    def _rollback(self) -> None:
        """Internal helper method to roll back the current transaction."""
        if self.connection:
            try:
                self.connection.rollback()
                logger.info("Transaction rolled back due to error or explicit request.")
            except pyodbc.Error as rollback_ex:
                # Log this prominently - failure during rollback is problematic
                logger.critical(f"Error during transaction rollback: {rollback_ex}")

    def close_connection(self) -> None:
        """Closes the database cursor and connection if they are open."""
        if self.debug:
            logger.debug("Closing database connection and cursor...")
        if self.cursor:
            try:
                self.cursor.close()
            except pyodbc.Error as ex:
                logger.warning(f"Error closing cursor: {ex}")
            finally:
                self.cursor = None # Ensure cursor is None regardless of close success

        if self.connection:
            try:
                # Rollback any pending changes before closing if not explicitly committed/rolled back
                # self._rollback() # Optional: Decide if implicit rollback on close is desired
                self.connection.close()
                logger.info("Connection closed.")
            except pyodbc.Error as ex:
                logger.warning(f"Error closing connection: {ex}")
            finally:
                self.connection = None # Ensure connection is None regardless of close success
