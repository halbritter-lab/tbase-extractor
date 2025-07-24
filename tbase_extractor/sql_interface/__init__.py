"""SQL interface package for tbase-extractor."""

import logging

from .db_interface import SQLInterface
from .dynamic_query_builder import (
    ColumnConfig,
    DynamicQueryBuilder,
    JoinConfig,
    JoinType,
    PatientQueryBuilder,
    QueryType,
    TableConfig,
    TableInfoQueryBuilder,
)
from .dynamic_query_manager import DynamicQueryManager, HybridQueryManager
from .exceptions import (
    DatabaseConnectionError,
    InvalidQueryParametersError,
    QueryExecutionError,
    QueryTemplateNotFoundError,
)
from .output_formatter import OutputFormatter
from .query_manager import QueryManager

# Initialize package logger
logger = logging.getLogger(__name__)

__all__ = [
    "SQLInterface",
    "QueryManager",
    "DynamicQueryManager",
    "HybridQueryManager",
    "DynamicQueryBuilder",
    "PatientQueryBuilder",
    "TableInfoQueryBuilder",
    "JoinType",
    "QueryType",
    "TableConfig",
    "ColumnConfig",
    "JoinConfig",
    "QueryTemplateNotFoundError",
    "DatabaseConnectionError",
    "QueryExecutionError",
    "InvalidQueryParametersError",
    "OutputFormatter",
]

logger.debug("SQL interface package initialized")
