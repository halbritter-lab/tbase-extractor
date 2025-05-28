"""Flexible SQL query builder for querying arbitrary tables with patient ID relationships."""
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import re


class JoinType(Enum):
    """Types of SQL joins."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


@dataclass
class TableSpec:
    """Specification for a table to include in the query."""
    name: str
    schema: str = "dbo"
    alias: str = None
    columns: List[str] = field(default_factory=list)  # Specific columns to select, empty = all
    patient_id_column: str = "PatientID"  # Name of the patient ID column in this table
    
    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.name}"
    
    @property
    def reference(self) -> str:
        """Get table reference for SQL (with alias if provided)."""
        if self.alias:
            return f"{self.full_name} {self.alias}"
        return self.full_name
    
    @property
    def effective_alias(self) -> str:
        """Get the alias to use in column references."""
        return self.alias or self.name


@dataclass
class FlexibleQueryConfig:
    """Configuration for a flexible patient-based query."""
    primary_table: TableSpec  # The main table (usually patient table)
    additional_tables: List[TableSpec] = field(default_factory=list)  # Additional tables to join
    join_type: JoinType = JoinType.LEFT
    where_conditions: List[str] = field(default_factory=list)
    order_by: List[str] = field(default_factory=list)
    limit: Optional[int] = None


class FlexibleQueryBuilder:
    """Builds SQL queries for arbitrary patient-related tables."""
    
    def __init__(self, debug: bool = False):
        """Initialize the flexible query builder."""
        self.debug = debug
        
    def build_patient_query(self, config: FlexibleQueryConfig, patient_id: int) -> Tuple[str, Tuple[Any, ...]]:
        """
        Build a query to get data for a specific patient from multiple tables.
        
        Args:
            config: Query configuration specifying tables and columns
            patient_id: Patient ID to query for
            
        Returns:
            Tuple of (SQL query string, parameters tuple)
        """
        if self.debug:
            print(f"[DEBUG FlexibleQueryBuilder] Building query for patient ID {patient_id}")
            print(f"[DEBUG FlexibleQueryBuilder] Primary table: {config.primary_table.full_name}")
            print(f"[DEBUG FlexibleQueryBuilder] Additional tables: {[t.full_name for t in config.additional_tables]}")
        
        # Build SELECT clause
        select_parts = []
        
        # Add columns from primary table
        primary_alias = config.primary_table.effective_alias
        if config.primary_table.columns:
            for col in config.primary_table.columns:
                select_parts.append(f"{primary_alias}.{col}")
        else:
            select_parts.append(f"{primary_alias}.*")
        
        # Add columns from additional tables
        for table in config.additional_tables:
            table_alias = table.effective_alias
            if table.columns:
                for col in table.columns:
                    # Use alias to avoid column name conflicts
                    column_alias = f"{table.name}_{col}" if col in [c.split('.')[-1] for c in select_parts] else None
                    if column_alias:
                        select_parts.append(f"{table_alias}.{col} AS {column_alias}")
                    else:
                        select_parts.append(f"{table_alias}.{col}")
            else:
                # Select all columns with table prefix to avoid conflicts
                select_parts.append(f"{table_alias}.*")
        
        sql_parts = ["SELECT", "    " + ",\n    ".join(select_parts)]
        
        # Build FROM clause
        sql_parts.extend(["FROM", f"    {config.primary_table.reference}"])
        
        # Build JOIN clauses
        for table in config.additional_tables:
            join_condition = f"{primary_alias}.{config.primary_table.patient_id_column} = {table.effective_alias}.{table.patient_id_column}"
            sql_parts.append(f"{config.join_type.value} {table.reference} ON {join_condition}")
        
        # Build WHERE clause
        where_conditions = [f"{primary_alias}.{config.primary_table.patient_id_column} = ?"]
        where_conditions.extend(config.where_conditions)
        
        sql_parts.extend(["WHERE", "    " + " AND ".join(where_conditions)])
        
        # Add ORDER BY if specified
        if config.order_by:
            sql_parts.extend(["ORDER BY", "    " + ", ".join(config.order_by)])
        
        # Add LIMIT if specified
        if config.limit:
            sql_parts.append(f"LIMIT {config.limit}")
        
        sql = "\n".join(sql_parts)
        params = (patient_id,)
        
        if self.debug:
            print(f"[DEBUG FlexibleQueryBuilder] Generated SQL:\n{sql}")
            print(f"[DEBUG FlexibleQueryBuilder] Parameters: {params}")
        
        return sql, params
    
    def build_discovery_query(self, schema: str = "dbo") -> Tuple[str, Tuple[Any, ...]]:
        """
        Build a query to discover tables that contain patient ID columns.
        
        Args:
            schema: Database schema to search in
            
        Returns:
            Tuple of (SQL query string, parameters tuple)
        """
        sql = """
SELECT 
    t.TABLE_NAME,
    t.TABLE_SCHEMA,
    c.COLUMN_NAME as PATIENT_ID_COLUMN,
    COUNT(c2.COLUMN_NAME) as TOTAL_COLUMNS
FROM 
    INFORMATION_SCHEMA.TABLES t
INNER JOIN 
    INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
    AND t.TABLE_NAME = c.TABLE_NAME
INNER JOIN 
    INFORMATION_SCHEMA.COLUMNS c2 ON t.TABLE_SCHEMA = c2.TABLE_SCHEMA 
    AND t.TABLE_NAME = c2.TABLE_NAME
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
    AND t.TABLE_SCHEMA = ?
    AND (
        c.COLUMN_NAME LIKE '%PatientID%' 
        OR c.COLUMN_NAME LIKE '%Patient_ID%'
        OR c.COLUMN_NAME LIKE '%patient_id%'
        OR c.COLUMN_NAME = 'PatientID'
    )
GROUP BY 
    t.TABLE_NAME, t.TABLE_SCHEMA, c.COLUMN_NAME
ORDER BY 
    t.TABLE_NAME
"""
        return sql, (schema,)
    
    def build_table_columns_query(self, table_name: str, schema: str = "dbo") -> Tuple[str, Tuple[Any, ...]]:
        """
        Build a query to get all columns for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema
            
        Returns:
            Tuple of (SQL query string, parameters tuple)
        """
        sql = """
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    ORDINAL_POSITION
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_NAME = ? 
    AND TABLE_SCHEMA = ?
ORDER BY 
    ORDINAL_POSITION
"""
        return sql, (table_name, schema)
    
    @staticmethod
    def parse_table_specification(table_spec: str) -> TableSpec:
        """
        Parse a table specification string into a TableSpec object.
        
        Format: "schema.table_name:alias[column1,column2,...]@patient_id_column"
        Examples:
            - "dbo.Patient:p"  # All columns from dbo.Patient with alias 'p'
            - "dbo.Diagnose:d[ICD10,Bezeichnung]"  # Specific columns
            - "hospital.Labs:l@PatID"  # Different patient ID column name
            - "dbo.Medications:m[Drug,Dose,Date]@PatientID"  # Full specification
        
        Args:
            table_spec: Table specification string
            
        Returns:
            TableSpec object
        """
        # Default values
        schema = "dbo"
        table_name = table_spec
        alias = None
        columns = []
        patient_id_column = "PatientID"
        
        # Parse patient ID column (@ symbol)
        if '@' in table_spec:
            table_part, patient_id_column = table_spec.rsplit('@', 1)
            table_spec = table_part
        
        # Parse columns (square brackets)
        if '[' in table_spec and ']' in table_spec:
            table_part, columns_part = table_spec.split('[', 1)
            columns_part = columns_part.rstrip(']')
            columns = [col.strip() for col in columns_part.split(',') if col.strip()]
            table_spec = table_part
        
        # Parse alias (colon)
        if ':' in table_spec:
            table_part, alias = table_spec.rsplit(':', 1)
            table_spec = table_part
        
        # Parse schema.table
        if '.' in table_spec:
            schema, table_name = table_spec.split('.', 1)
        else:
            table_name = table_spec
        
        return TableSpec(
            name=table_name,
            schema=schema,
            alias=alias,
            columns=columns,
            patient_id_column=patient_id_column
        )


class FlexibleQueryManager:
    """Manages flexible queries for arbitrary patient-related tables."""
    
    def __init__(self, debug: bool = False):
        """Initialize the flexible query manager."""
        self.builder = FlexibleQueryBuilder(debug)
        self.debug = debug
    
    def query_patient_tables(
        self, 
        patient_id: int, 
        table_specs: List[str],
        join_type: str = "LEFT",
        order_by: List[str] = None,
        limit: Optional[int] = None
    ) -> Tuple[str, Tuple[Any, ...]]:
        """
        Query multiple tables for a specific patient.
        
        Args:
            patient_id: Patient ID to query for
            table_specs: List of table specification strings
            join_type: Type of join to use (LEFT, INNER, RIGHT, FULL)
            order_by: List of columns to order by
            limit: Maximum number of rows to return
            
        Returns:
            Tuple of (SQL query string, parameters tuple)
        """
        if not table_specs:
            raise ValueError("At least one table specification is required")
        
        # Parse table specifications
        parsed_tables = [FlexibleQueryBuilder.parse_table_specification(spec) for spec in table_specs]
        
        # First table is the primary table
        primary_table = parsed_tables[0]
        additional_tables = parsed_tables[1:] if len(parsed_tables) > 1 else []
        
        # Convert join type string to enum
        try:
            join_enum = JoinType[join_type.upper()]
        except KeyError:
            join_enum = JoinType.LEFT
        
        # Create configuration
        config = FlexibleQueryConfig(
            primary_table=primary_table,
            additional_tables=additional_tables,
            join_type=join_enum,
            order_by=order_by or [],
            limit=limit
        )
        
        return self.builder.build_patient_query(config, patient_id)
    
    def discover_patient_tables(self, schema: str = "dbo") -> Tuple[str, Tuple[Any, ...]]:
        """
        Discover tables that contain patient ID columns.
        
        Args:
            schema: Database schema to search in
            
        Returns:
            Tuple of (SQL query string, parameters tuple)
        """
        return self.builder.build_discovery_query(schema)
    
    def get_table_columns(self, table_name: str, schema: str = "dbo") -> Tuple[str, Tuple[Any, ...]]:
        """
        Get column information for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema
            
        Returns:
            Tuple of (SQL query string, parameters tuple)
        """
        return self.builder.build_table_columns_query(table_name, schema)
