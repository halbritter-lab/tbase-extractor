"""Dynamic query manager that uses the query builder instead of templates."""
from typing import Tuple, Any, Optional
from .dynamic_query_builder import PatientQueryBuilder, TableInfoQueryBuilder
from .query_manager import QueryManager


class DynamicQueryManager:
    """Manages SQL queries using dynamic query building instead of templates."""
    
    def __init__(self, patient_table: str = "Patient", diagnose_table: str = "Diagnose", schema: str = "dbo", debug: bool = False):
        """
        Initialize dynamic query manager.
        
        Args:
            patient_table: Name of the patient table
            diagnose_table: Name of the diagnose table  
            schema: Database schema name
            debug: Whether to print debug information
        """
        self.patient_table = patient_table
        self.diagnose_table = diagnose_table
        self.schema = schema
        self.debug = debug
        
        # Initialize builders
        self.patient_builder = PatientQueryBuilder(patient_table, diagnose_table, schema)
        self.table_info_builder = TableInfoQueryBuilder()
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Initialized with patient_table='{patient_table}', diagnose_table='{diagnose_table}', schema='{schema}'")
    
    def get_patient_by_id_query(self, patient_id: int, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get a query to find a patient by ID."""
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Building patient_by_id query for ID {patient_id}, include_diagnoses={include_diagnoses}")
        
        sql, params = self.patient_builder.get_patient_by_id_query(patient_id, include_diagnoses)
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Generated SQL: {sql}")
            print(f"[DEBUG DynamicQueryManager] Parameters: {params}")
        
        return sql, params
    
    def get_patient_by_name_dob_query(self, first_name: str, last_name: str, dob_date, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get a query to find a patient by name and date of birth."""
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Building patient_by_name_dob query for {first_name} {last_name}, dob={dob_date}, include_diagnoses={include_diagnoses}")
        
        sql, params = self.patient_builder.get_patient_by_name_dob_query(first_name, last_name, dob_date, include_diagnoses)
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Generated SQL: {sql}")
            print(f"[DEBUG DynamicQueryManager] Parameters: {params}")
        
        return sql, params
    
    def get_all_patients_query(self, include_diagnoses: bool = False, limit: Optional[int] = None) -> Tuple[str, Tuple[Any, ...]]:
        """Get all patients from the database. Use with caution!"""
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Building get_all_patients query, include_diagnoses={include_diagnoses}, limit={limit}")
        
        sql, params = self.patient_builder.get_all_patients_query(include_diagnoses, limit)
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Generated SQL: {sql}")
            print(f"[DEBUG DynamicQueryManager] Parameters: {params}")
        
        return sql, params
    
    def get_patients_by_lastname_like_query(self, lastname_pattern: str, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get patients with last names matching a pattern."""
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Building lastname_like query for pattern '{lastname_pattern}', include_diagnoses={include_diagnoses}")
        
        sql, params = self.patient_builder.get_patients_by_lastname_like_query(lastname_pattern, include_diagnoses)
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Generated SQL: {sql}")
            print(f"[DEBUG DynamicQueryManager] Parameters: {params}")
        
        return sql, params
    
    def get_list_tables_query(self) -> Tuple[str, Tuple[Any, ...]]:
        """Get a query to list available tables."""
        if self.debug:
            print("[DEBUG DynamicQueryManager] Building list_tables query")
        
        sql, params = self.table_info_builder.get_list_tables_query()
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Generated SQL: {sql}")
        
        return sql, params
    
    def get_table_columns_query(self, table_name: str, table_schema: str) -> Tuple[str, Tuple[Any, ...]]:
        """Get a query to fetch column names and data types for a specific table."""
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Building table_columns query for {table_schema}.{table_name}")
        
        sql, params = self.table_info_builder.get_table_columns_query(table_name, table_schema)
        
        if self.debug:
            print(f"[DEBUG DynamicQueryManager] Generated SQL: {sql}")
            print(f"[DEBUG DynamicQueryManager] Parameters: {params}")
        
        return sql, params


class HybridQueryManager:
    """
    A hybrid query manager that can use either static templates or dynamic query building.
    This allows for gradual migration and comparison between approaches.
    """
    
    def __init__(self, templates_dir: str, patient_table: str = "Patient", diagnose_table: str = "Diagnose", schema: str = "dbo", debug: bool = False):
        """
        Initialize hybrid query manager.
        
        Args:
            templates_dir: Directory containing SQL templates
            patient_table: Name of the patient table
            diagnose_table: Name of the diagnose table  
            schema: Database schema name
            debug: Whether to print debug information
        """
        self.template_manager = QueryManager(templates_dir, debug)
        self.dynamic_manager = DynamicQueryManager(patient_table, diagnose_table, schema, debug)
        self.debug = debug
        
        if self.debug:
            print(f"[DEBUG HybridQueryManager] Initialized with both template and dynamic managers")
    
    def get_patient_by_id_query(self, patient_id: int, use_dynamic: bool = False, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get a query to find a patient by ID using either templates or dynamic building."""
        if use_dynamic:
            return self.dynamic_manager.get_patient_by_id_query(patient_id, include_diagnoses)
        else:
            return self.template_manager.get_patient_by_id_query(patient_id)
    
    def get_patient_by_name_dob_query(self, first_name: str, last_name: str, dob_date, use_dynamic: bool = False, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get a query to find a patient by name and DOB using either templates or dynamic building."""
        if use_dynamic:
            return self.dynamic_manager.get_patient_by_name_dob_query(first_name, last_name, dob_date, include_diagnoses)
        else:
            return self.template_manager.get_patient_by_name_dob_query(first_name, last_name, dob_date)
    
    def get_all_patients_query(self, use_dynamic: bool = False, include_diagnoses: bool = False, limit: Optional[int] = None) -> Tuple[str, Tuple[Any, ...]]:
        """Get all patients using either templates or dynamic building."""
        if use_dynamic:
            return self.dynamic_manager.get_all_patients_query(include_diagnoses, limit)
        else:
            return self.template_manager.get_all_patients_query()
    
    def get_patients_by_lastname_like_query(self, lastname_pattern: str, use_dynamic: bool = False, include_diagnoses: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get patients by lastname pattern using either templates or dynamic building."""
        if use_dynamic:
            return self.dynamic_manager.get_patients_by_lastname_like_query(lastname_pattern, include_diagnoses)
        else:
            return self.template_manager.get_patients_by_lastname_like_query(lastname_pattern)
    
    def get_list_tables_query(self, use_dynamic: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get list of tables using either templates or dynamic building."""
        if use_dynamic:
            return self.dynamic_manager.get_list_tables_query()
        else:
            return self.template_manager.get_list_tables_query()
    
    def get_table_columns_query(self, table_name: str, table_schema: str, use_dynamic: bool = False) -> Tuple[str, Tuple[Any, ...]]:
        """Get table columns using either templates or dynamic building."""
        if use_dynamic:
            return self.dynamic_manager.get_table_columns_query(table_name, table_schema)
        else:
            return self.template_manager.get_table_columns_query(table_name, table_schema)
