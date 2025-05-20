"""tbase_extractor package"""
import logging

# Configure a null handler by default
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Expose public interface
from . import sql_interface
from .sql_interface import (
    SQLInterface,
    QueryManager,
    OutputFormatter,
    QueryTemplateNotFoundError,
    DatabaseConnectionError,
    QueryExecutionError,
    InvalidQueryParametersError
)
from .main import main

__version__ = "0.1.0"

__all__ = [
    'sql_interface',
    'SQLInterface',
    'QueryManager',
    'OutputFormatter',
    'QueryTemplateNotFoundError',
    'DatabaseConnectionError',
    'QueryExecutionError',
    'InvalidQueryParametersError',
    'main',
]

if __name__ == '__main__':
    import sys
    print("[DEBUG] Running package as script (__init__.py)", file=sys.stderr)
    main()
