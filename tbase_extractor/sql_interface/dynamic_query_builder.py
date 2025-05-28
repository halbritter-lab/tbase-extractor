"""Dynamic SQL query builder for runtime query generation."""
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class JoinType(Enum):
    """Types of SQL joins."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class QueryType(Enum):
    """Types of queries that can be built."""
    SELECT_BY_ID = "select_by_id"
    SELECT_BY_NAME_DOB = "select_by_name_dob"
    SELECT_ALL = "select_all"
    SELECT_WITH_FILTERS = "select_with_filters"


@dataclass
class TableConfig:
    """Configuration for a database table."""
    name: str
    schema: str = "dbo"
    alias: str = None
    
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


@dataclass
class ColumnConfig:
    """Configuration for table columns."""
    name: str
    table_alias: str = None
    alias: str = None
    
    @property
    def reference(self) -> str:
        """Get column reference for SQL."""
        prefix = f"{self.table_alias}." if self.table_alias else ""
        suffix = f" AS {self.alias}" if self.alias else ""
        return f"{prefix}{self.name}{suffix}"


@dataclass
class JoinConfig:
    """Configuration for table joins."""
    table: TableConfig
    join_type: JoinType
    on_condition: str


class DynamicQueryBuilder:
    """Builds SQL queries dynamically based on configuration."""
    
    def __init__(self):
        """Initialize the query builder."""
        self.reset()
    
    def reset(self):
        """Reset the builder state."""
        self._select_columns: List[ColumnConfig] = []
        self._from_table: Optional[TableConfig] = None
        self._joins: List[JoinConfig] = []
        self._where_conditions: List[str] = []
        self._parameters: List[Any] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None
    
    def select(self, columns: List[ColumnConfig]) -> 'DynamicQueryBuilder':
        """Add SELECT columns."""
        self._select_columns.extend(columns)
        return self
    
    def select_all_from_table(self, table_alias: str) -> 'DynamicQueryBuilder':
        """Select all columns from a specific table."""
        self._select_columns.append(ColumnConfig(name="*", table_alias=table_alias))
        return self
    
    def from_table(self, table: TableConfig) -> 'DynamicQueryBuilder':
        """Set the FROM table."""
        self._from_table = table
        return self
    
    def join(self, join_config: JoinConfig) -> 'DynamicQueryBuilder':
        """Add a JOIN clause."""
        self._joins.append(join_config)
        return self
    
    def where(self, condition: str, *params) -> 'DynamicQueryBuilder':
        """Add WHERE condition with parameters."""
        self._where_conditions.append(condition)
        self._parameters.extend(params)
        return self
    
    def order_by(self, column: str, direction: str = "ASC") -> 'DynamicQueryBuilder':
        """Add ORDER BY clause."""
        self._order_by.append(f"{column} {direction}")
        return self
    
    def limit(self, count: int) -> 'DynamicQueryBuilder':
        """Add LIMIT clause (SQL Server uses TOP)."""
        self._limit = count
        return self
    
    def build(self) -> Tuple[str, Tuple[Any, ...]]:
        """Build the final SQL query and parameters."""
        if not self._from_table:
            raise ValueError("FROM table must be specified")
        
        # Build SELECT clause
        if not self._select_columns:
            select_clause = "SELECT *"
        else:
            columns_str = ", ".join(col.reference for col in self._select_columns)
            select_clause = f"SELECT {columns_str}"
        
        # Add TOP clause for SQL Server if limit is specified
        if self._limit:
            select_clause = f"SELECT TOP {self._limit} " + select_clause[7:]  # Remove "SELECT "
        
        # Build FROM clause
        from_clause = f"FROM {self._from_table.reference}"
        
        # Build JOIN clauses
        join_clauses = []
        for join in self._joins:
            join_clauses.append(f"{join.join_type.value} {join.table.reference} ON {join.on_condition}")
        
        # Build WHERE clause
        where_clause = ""
        if self._where_conditions:
            where_clause = "WHERE " + " AND ".join(self._where_conditions)
        
        # Build ORDER BY clause
        order_clause = ""
        if self._order_by:
            order_clause = "ORDER BY " + ", ".join(self._order_by)
        
        # Combine all parts
        query_parts = [select_clause, from_clause] + join_clauses
        if where_clause:
            query_parts.append(where_clause)
        if order_clause:
            query_parts.append(order_clause)
        
        sql = "\n".join(query_parts)
        return sql, tuple(self._parameters)


class PatientQueryBuilder:
    """Specialized query builder for patient-related queries."""
    
    def __init__(self, patient_table: str = "Patient", diagnose_table: str = "Diagnose", schema: str = "dbo"):
        """Initialize with table configurations."""
        self.patient_table = TableConfig(name=patient_table, schema=schema, alias="p")
        self.diagnose_table = TableConfig(name=diagnose_table, schema=schema, alias="d")
        self.builder = DynamicQueryBuilder()
    
    def get_patient_by_id_query(self, patient_id: int, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Build query to get patient by ID."""
        self.builder.reset()
        
        # Define patient columns
        patient_columns = [
            ColumnConfig("PatientID", "p"),
            ColumnConfig("Vorname", "p"),
            ColumnConfig("Name", "p"),
            ColumnConfig("Geburtsdatum", "p"),
            ColumnConfig("Grunderkrankung", "p"),
            ColumnConfig("ET_Grunderkrankung", "p"),
            ColumnConfig("Dauernotiz", "p"),
            ColumnConfig("Dauernotiz_Diagnose", "p")
        ]
        
        self.builder.select(patient_columns).from_table(self.patient_table)
        
        # Add diagnosis join if requested
        if include_diagnoses:
            diagnosis_columns = [
                ColumnConfig("ICD10", "d"),
                ColumnConfig("Bezeichnung", "d", "DiagnoseBezeichnung")
            ]
            self.builder.select(diagnosis_columns)
            
            join_config = JoinConfig(
                table=self.diagnose_table,
                join_type=JoinType.LEFT,
                on_condition="p.PatientID = d.PatientID"
            )
            self.builder.join(join_config)
        
        self.builder.where("p.PatientID = ?", patient_id)
        
        return self.builder.build()
    
    def get_patient_by_name_dob_query(self, first_name: str, last_name: str, dob, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Build query to get patient by name and DOB."""
        self.builder.reset()
        
        # Define patient columns
        patient_columns = [
            ColumnConfig("PatientID", "p"),
            ColumnConfig("Vorname", "p"),
            ColumnConfig("Name", "p"),
            ColumnConfig("Geburtsdatum", "p"),
            ColumnConfig("Grunderkrankung", "p"),
            ColumnConfig("ET_Grunderkrankung", "p"),
            ColumnConfig("Dauernotiz", "p"),
            ColumnConfig("Dauernotiz_Diagnose", "p")
        ]
        
        self.builder.select(patient_columns).from_table(self.patient_table)
        
        # Add diagnosis join if requested
        if include_diagnoses:
            diagnosis_columns = [
                ColumnConfig("ICD10", "d"),
                ColumnConfig("Bezeichnung", "d", "DiagnoseBezeichnung")
            ]
            self.builder.select(diagnosis_columns)
            
            join_config = JoinConfig(
                table=self.diagnose_table,
                join_type=JoinType.LEFT,
                on_condition="p.PatientID = d.PatientID"
            )
            self.builder.join(join_config)
        
        self.builder.where("p.Vorname = ? AND p.Name = ? AND p.Geburtsdatum = ?", first_name, last_name, dob)
        
        return self.builder.build()
    
    def get_all_patients_query(self, include_diagnoses: bool = False, limit: Optional[int] = None) -> Tuple[str, Tuple[Any, ...]]:
        """Build query to get all patients."""
        self.builder.reset()
        
        self.builder.select_all_from_table("p").from_table(self.patient_table)
        
        # Add diagnosis join if requested
        if include_diagnoses:
            diagnosis_columns = [
                ColumnConfig("ICD10", "d"),
                ColumnConfig("Bezeichnung", "d", "DiagnoseBezeichnung")
            ]
            self.builder.select(diagnosis_columns)
            
            join_config = JoinConfig(
                table=self.diagnose_table,
                join_type=JoinType.LEFT,
                on_condition="p.PatientID = d.PatientID"
            )
            self.builder.join(join_config)
        
        if limit:
            self.builder.limit(limit)
        
        return self.builder.build()
    
    def get_patients_by_lastname_like_query(self, lastname_pattern: str, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Build query to get patients by lastname pattern."""
        self.builder.reset()
        
        patient_columns = [
            ColumnConfig("PatientID", "p"),
            ColumnConfig("Vorname", "p"),
            ColumnConfig("Name", "p"),
            ColumnConfig("Geburtsdatum", "p"),
            ColumnConfig("Grunderkrankung", "p"),
            ColumnConfig("ET_Grunderkrankung", "p"),
            ColumnConfig("Dauernotiz", "p"),
            ColumnConfig("Dauernotiz_Diagnose", "p")
        ]
        
        self.builder.select(patient_columns).from_table(self.patient_table)
        
        # Add diagnosis join if requested
        if include_diagnoses:
            diagnosis_columns = [
                ColumnConfig("ICD10", "d"),
                ColumnConfig("Bezeichnung", "d", "DiagnoseBezeichnung")
            ]
            self.builder.select(diagnosis_columns)
            
            join_config = JoinConfig(
                table=self.diagnose_table,
                join_type=JoinType.LEFT,
                on_condition="p.PatientID = d.PatientID"
            )
            self.builder.join(join_config)
        
        # Add wildcard if not present
        if not any(c in lastname_pattern for c in ['%', '_']):
            lastname_pattern = f"{lastname_pattern}%"
        
        self.builder.where("p.Name LIKE ?", lastname_pattern)
        
        return self.builder.build()


class TableInfoQueryBuilder:
    """Builder for database metadata queries."""
    
    def __init__(self):
        """Initialize the table info query builder."""
        self.builder = DynamicQueryBuilder()
    
    def get_list_tables_query(self) -> Tuple[str, Tuple[Any, ...]]:
        """Build query to list all tables."""
        # This is complex due to aggregation, so we'll use a direct SQL approach
        sql = """
        SELECT 
            t.TABLE_NAME as [Table Name],
            COUNT(*) as [Column Count],
            CAST(STRING_AGG(CAST((c.COLUMN_NAME + ' (' + c.DATA_TYPE + ')') AS VARCHAR(MAX)), CHAR(13) + CHAR(10)) AS VARCHAR(MAX)) as [Columns]
        FROM 
            INFORMATION_SCHEMA.TABLES t
        INNER JOIN 
            INFORMATION_SCHEMA.COLUMNS c 
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
            AND t.TABLE_NAME = c.TABLE_NAME
        WHERE 
            t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY
            t.TABLE_NAME
        ORDER BY 
            t.TABLE_NAME
        """
        return sql, tuple()
    
    def get_table_columns_query(self, table_name: str, table_schema: str) -> Tuple[str, Tuple[Any, ...]]:
        """Build query to get columns for a specific table."""
        self.builder.reset()
        
        columns = [
            ColumnConfig("COLUMN_NAME"),
            ColumnConfig("DATA_TYPE")
        ]
        
        info_schema_table = TableConfig(name="COLUMNS", schema="INFORMATION_SCHEMA")
        
        self.builder.select(columns).from_table(info_schema_table)
        self.builder.where("TABLE_NAME = ? AND TABLE_SCHEMA = ?", table_name, table_schema)
        
        return self.builder.build()
