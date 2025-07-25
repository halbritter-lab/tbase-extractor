"""Database interface module for SQL Server connections."""

import html
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import pyodbc
except ImportError:
    # Allow module to be imported for testing without pyodbc
    pyodbc = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    # Fallback for environments without BeautifulSoup
    BeautifulSoup = None  # type: ignore

from ..secure_logging import get_secure_logger

# Initialize secure logger
logger = get_secure_logger(__name__)


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
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)

        # Remove all other HTML tags using BeautifulSoup if available
        if BeautifulSoup is not None:
            text = BeautifulSoup(text, "html.parser").get_text(separator="\n")
        else:
            # Fallback: simple HTML tag removal using regex
            text = re.sub(r"<[^>]+>", "", text)

        # Normalize multiple consecutive newlines to a single newline
        text = re.sub(r"\n\s*\n+", "\n", text)

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
        return self  # Return the instance for use within the 'with' block

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
        # Check if pyodbc is available
        if pyodbc is None:
            logger.error("pyodbc not available. Cannot establish database connection.")
            return False

        # Avoid reconnecting if already connected (simple check)
        if self.connection is not None:
            logger.warning("Connection object already exists. Close before reconnecting if needed.")
            return True

        # Check for required connection details
        if not all([self.server, self.database, self.username_sql, self.password, self.driver]):
            logger.error(
                "Database connection details incomplete. Check your .env file "
                "(SQL_SERVER, DATABASE, USERNAME_SQL, PASSWORD, SQL_DRIVER).",
            )
            return False

        start_time = time.time()
        try:
            connection_string = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username_sql};"
                f"PWD={self.password};"
            )
            
            # Secure logging: Never log actual connection string or credentials
            logger.debug(f"Attempting database connection to server: {self.server or 'Unknown'}")
            logger.debug(f"Target database: {self.database or 'Unknown'}")
            
            self.connection = pyodbc.connect(connection_string, autocommit=False)
            self.cursor = self.connection.cursor()
            
            duration_ms = (time.time() - start_time) * 1000
            logger.log_authentication_event("DB_CONNECT", self.username_sql, success=True)
            logger.log_database_operation("CONNECT", success=True, duration_ms=duration_ms)
            logger.info("Database connection established successfully")
            return True
        except Exception as ex:  # Catch all exceptions since pyodbc might not be available
            duration_ms = (time.time() - start_time) * 1000
            
            # Secure error logging - don't expose sensitive connection details
            if pyodbc and hasattr(ex, "args") and len(ex.args) >= 2:
                # This is a pyodbc.Error - log SQLSTATE but sanitize details
                sqlstate = ex.args[0]
                logger.log_authentication_event("DB_CONNECT", self.username_sql, success=False, 
                                               details=f"SQLSTATE {sqlstate}")
                logger.error(f"Database connection failed: SQLSTATE {sqlstate}")
                # Don't log detailed error message as it might contain sensitive info
                logger.debug("Connection error details available in debug mode (sanitized)")
            else:
                # Generic exception - sanitize the error message
                error_type = type(ex).__name__
                logger.log_authentication_event("DB_CONNECT", self.username_sql, success=False, 
                                               details=f"Exception: {error_type}")
                logger.error(f"Database connection failed: {error_type}")
                logger.debug(f"Connection error details: {str(ex)[:100]}...")  # Truncate for safety
            
            logger.log_database_operation("CONNECT", success=False, duration_ms=duration_ms)
            self.connection = None  # Ensure state reflects failure
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
        
        start_time = time.time()
        try:
            self.cursor.execute(query, params)
            duration_ms = (time.time() - start_time) * 1000
            logger.log_sql_execution(query, params, success=True, duration_ms=duration_ms)
            return True
        except Exception as ex:
            duration_ms = (time.time() - start_time) * 1000
            logger.log_sql_execution(query, params, success=False, duration_ms=duration_ms)
            
            # Secure error logging - don't expose query details in error messages
            if pyodbc and hasattr(ex, "args") and len(ex.args) >= 2:
                # This is a pyodbc.Error
                sqlstate = ex.args[0]
                logger.error(f"SQL execution failed: SQLSTATE {sqlstate}")
                logger.debug("Query execution error details available in debug mode")
            else:
                error_type = type(ex).__name__
                logger.error(f"SQL execution failed: {error_type}")
                logger.debug(f"Query execution error: {str(ex)[:100]}...")
            
            self._rollback()  # Attempt to rollback on execution error
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
            start_time = time.time()
            rows = self.cursor.fetchall()
            duration_ms = (time.time() - start_time) * 1000
            
            row_count = len(rows)
            logger.log_database_operation("FETCH", success=True, duration_ms=duration_ms, row_count=row_count)

            # Convert rows to list of dictionaries, cleaning any string values
            cleaned_results = []
            for row in rows:
                cleaned_row = {}
                for col, val in zip(columns, row):
                    cleaned_row[col] = self._clean_field_value(val)
                cleaned_results.append(cleaned_row)
            return cleaned_results

        except Exception as ex:
            # Catch errors specifically during fetch or description access
            if pyodbc and hasattr(ex, "args") and len(ex.args) >= 2:
                # This is a pyodbc.Error
                sqlstate = ex.args[0]
                logger.error(f"Error fetching results from cursor: SQLSTATE {sqlstate} - {ex.args[1]}")
            else:
                logger.error(f"Error fetching results from cursor: {ex}")
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
        except Exception as ex:
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
            except Exception as rollback_ex:
                # Log this prominently - failure during rollback is problematic
                logger.critical(f"Error during transaction rollback: {rollback_ex}")

    def close_connection(self) -> None:
        """Closes the database cursor and connection if they are open."""
        if self.debug:
            logger.debug("Closing database connection and cursor...")
        if self.cursor:
            try:
                self.cursor.close()
            except Exception as ex:
                logger.warning(f"Error closing cursor: {ex}")
            finally:
                self.cursor = None  # Ensure cursor is None regardless of close success

        if self.connection:
            try:
                # Rollback any pending changes before closing if not explicitly committed/rolled back
                # self._rollback() # Optional: Decide if implicit rollback on close is desired
                self.connection.close()
                logger.info("Connection closed.")
            except Exception as ex:
                logger.warning(f"Error closing connection: {ex}")
            finally:
                self.connection = None  # Ensure connection is None regardless of close success
