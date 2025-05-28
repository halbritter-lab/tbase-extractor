"""SQL interface package for tbase-extractor"""
import logging

from .db_interface import SQLInterface
from .query_manager import QueryManager
from .dynamic_query_manager import DynamicQueryManager, HybridQueryManager
from .dynamic_query_builder import (
    DynamicQueryBuilder, 
    PatientQueryBuilder, 
    TableInfoQueryBuilder,
    JoinType,
    QueryType,
    TableConfig,
    ColumnConfig,
    JoinConfig
)
from .exceptions import (
    QueryTemplateNotFoundError,
    DatabaseConnectionError,
    QueryExecutionError,
    InvalidQueryParametersError
)
from .output_formatter import OutputFormatter

# Initialize package logger
logger = logging.getLogger(__name__)

__all__ = [
    'SQLInterface',
    'QueryManager',
    'DynamicQueryManager',
    'HybridQueryManager',
    'DynamicQueryBuilder',
    'PatientQueryBuilder',
    'TableInfoQueryBuilder',
    'JoinType',
    'QueryType',
    'TableConfig',
    'ColumnConfig',
    'JoinConfig',
    'QueryTemplateNotFoundError',
    'DatabaseConnectionError',
    'QueryExecutionError',
    'InvalidQueryParametersError',
    'OutputFormatter'
]

logger.debug("SQL interface package initialized")
