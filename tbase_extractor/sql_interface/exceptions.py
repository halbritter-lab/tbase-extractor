"""Custom exceptions for the SQL interface."""

class QueryTemplateNotFoundError(Exception):
    """Raised when a SQL template file cannot be found."""
    pass

class DatabaseConnectionError(Exception):
    """Raised when unable to connect to the database."""
    pass

class QueryExecutionError(Exception):
    """Raised when a query fails to execute."""
    pass

class InvalidQueryParametersError(Exception):
    """Raised when invalid parameters are provided for a query."""
    pass
