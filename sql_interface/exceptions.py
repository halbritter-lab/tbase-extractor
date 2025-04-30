# sql_interface/exceptions.py

class SQLInterfaceError(Exception):
    """Base exception for errors raised by the sql_interface package."""
    pass

class ConfigurationError(SQLInterfaceError):
    """Exception raised for configuration-related errors (e.g., missing .env variables)."""
    pass

class ConnectionError(SQLInterfaceError):
    """Exception raised for errors during database connection attempt."""
    pass

class QueryExecutionError(SQLInterfaceError):
    """Exception raised for errors during SQL query execution."""
    pass

class QueryTemplateError(SQLInterfaceError):
    """Base exception for errors related to SQL query templates."""
    pass

# This is the specific class being imported in query_manager.py
class QueryTemplateNotFoundError(QueryTemplateError, FileNotFoundError):
    """Exception raised when a specified SQL template file cannot be found."""
    # Inherits from FileNotFoundError so it can be caught as either type if needed
    pass

class OutputFormattingError(SQLInterfaceError):
    """Exception raised for errors during output formatting (e.g., JSON serialization)."""
    pass