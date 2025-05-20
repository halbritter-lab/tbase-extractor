"""tbase_extractor package"""
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
from .main import setup_arg_parser, process_args, main

__all__ = [
    'sql_interface',
    'SQLInterface',
    'QueryManager',
    'OutputFormatter',
    'QueryTemplateNotFoundError',
    'DatabaseConnectionError',
    'QueryExecutionError',
    'InvalidQueryParametersError',
    'main'
]

if __name__ == '__main__':
    main()
