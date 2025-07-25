"""Query management utilities for SQL template loading and execution."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from ..secure_logging import get_secure_logger
from .db_interface import SQLInterface
from .exceptions import QueryTemplateNotFoundError


class QueryManager:
    """Manages SQL queries, including template loading and parameter substitution."""

    def __init__(self, templates_dir: Union[str, Path], debug: bool = False):
        """
        Initialize QueryManager with a templates directory and debug flag.

        Args:
            templates_dir (Union[str, Path]): Path to directory containing SQL templates.
            debug (bool): Whether to print debug information.

        Raises:
            ValueError: If templates_dir is None, not a string/Path, or not a valid directory.
        """
        if templates_dir is None:
            raise ValueError("templates_dir cannot be None")

        # Convert to string if it's a Path or similar object
        templates_dir_str = str(templates_dir)

        # Strict validation
        if not os.path.exists(templates_dir_str):
            raise ValueError(f"templates_dir path does not exist: {templates_dir_str}")
        if not os.path.isdir(templates_dir_str):
            raise ValueError(f"templates_dir is not a directory: {templates_dir_str}")

        self.templates_dir = templates_dir_str
        self.debug = debug
        self.logger = get_secure_logger(__name__, production_mode=not debug)

        if self.debug:
            template_files = [f for f in os.listdir(self.templates_dir) if f.endswith(".sql")]
            self.logger.debug("QueryManager initialized with templates directory")
            self.logger.debug(f"Available SQL templates: {len(template_files)} files")

    def load_query_template(self, template_name: str) -> str:
        """
        Load a SQL query template from file.

        Args:
            template_name (str): Name of template file without .sql extension

        Returns:
            str: The SQL query template string

        Raises:
            QueryTemplateNotFoundError: If template file doesn't exist
        """
        if not template_name.endswith(".sql"):
            template_name += ".sql"

        template_path = os.path.join(self.templates_dir, template_name)
        if not os.path.isfile(template_path):
            raise QueryTemplateNotFoundError(f"SQL template file not found: {template_path}")

        try:
            with open(template_path, encoding="utf-8") as f:
                template = f.read()

            if self.debug:
                self.logger.debug(f"Template '{template_name}' loaded successfully")
            return template
        except OSError as e:
            raise QueryTemplateNotFoundError(
                f"Error reading SQL template file '{template_path}': {e}",
            )

    def execute_template_query(
        self,
        db: SQLInterface,
        template_name: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a query from a template with parameters.

        Args:
            db (SQLInterface): Database connection wrapper
            template_name (str): Name of template file without .sql extension
            params (Dict[str, Any], optional): Parameters to substitute in query

        Returns:
            Optional[List[Dict[str, Any]]]: Query results as list of dictionaries,
                                          or None if query fails
        """
        try:
            self.logger.debug(f"Executing template '{template_name}'")
            if params and self.debug:
                self.logger.debug(f"Template parameters provided: {len(params)} parameters")

            query = self.load_query_template(template_name)
            param_values = tuple(params.values()) if params else ()

            if db.execute_query(query, param_values):
                results = db.fetch_results()
                return results

            self.logger.error("Template query execution failed")
            return None

        except QueryTemplateNotFoundError as e:
            self.logger.error(f"Template not found: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error executing template query: {type(e).__name__}")
            self.logger.debug(f"Template query error details: {str(e)[:100]}...")
            return None

    def get_list_tables_query(self) -> Tuple[str, tuple]:
        """Get a query to list available tables."""
        return self.load_query_template("list_tables"), ()

    def get_patient_by_id_query(self, patient_id: int, include_diagnoses: bool = True) -> Tuple[str, tuple]:
        """Get a query to find a patient by ID."""
        return self.load_query_template("get_patient_by_id"), (patient_id,)

    def get_patient_by_name_dob_query(
        self,
        first_name: str,
        last_name: str,
        dob_date: Any,
        include_diagnoses: bool = True,
    ) -> Tuple[str, tuple]:
        """Get a query to find a patient by name and date of birth."""
        return (
            self.load_query_template("get_patient_by_name_dob"),
            (first_name, last_name, dob_date),
        )

    def get_patients_by_dob_year_range_query(
        self,
        start_year: int,
        end_year: int,
        include_diagnoses: bool = True,
    ) -> Tuple[str, Tuple[int, int]]:
        """Get patients with DOB in a year range.

        Args:
            start_year (int): Start year (inclusive)
            end_year (int): End year (inclusive)

        Returns:
            Tuple[str, Tuple[int, int]]: SQL query and params tuple
        """
        sql = self.load_query_template("get_patients_by_dob_year_range")
        return sql, (start_year, end_year)

    def get_patients_by_lastname_like_query(
        self,
        lastname_pattern: str,
        include_diagnoses: bool = True,
    ) -> Tuple[str, Tuple[str]]:
        """Get patients with last names matching a pattern.

        Args:
            lastname_pattern (str): Lastname pattern for LIKE clause (% wildcards will be added if not present)

        Returns:
            Tuple[str, Tuple[str]]: SQL query and params tuple
        """
        if not any(c in lastname_pattern for c in ["%", "_"]):
            lastname_pattern = f"{lastname_pattern}%"
        sql = self.load_query_template("get_patients_by_lastname_like")
        return sql, (lastname_pattern,)

    def get_all_patients_query(self, include_diagnoses: bool = True) -> Tuple[str, Tuple[()]]:
        """Get all patients from the database. Use with caution!

        Returns:
            Tuple[str, Tuple[()]]: SQL query and empty params tuple
        """
        sql = self.load_query_template("get_all_patients")
        return sql, ()

    def get_table_columns_query(
        self,
        table_name: str,
        table_schema: str,
    ) -> Tuple[str, Tuple[str, str]]:
        """Get a query to fetch column names and data types for a specific table."""
        sql = self.load_query_template("get_table_columns")
        return sql, (table_name, table_schema)
