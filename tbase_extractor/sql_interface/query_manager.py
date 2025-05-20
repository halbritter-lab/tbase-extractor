import os
from typing import Dict, Any, Optional, List
from .db_interface import SQLInterface
from .exceptions import QueryTemplateNotFoundError

class QueryManager:
    """Manages SQL queries, including template loading and parameter substitution."""

    def __init__(self, templates_dir: Optional[str] = None, debug: bool = False):
        """
        Initialize QueryManager with a templates directory and debug flag.

        Args:
            templates_dir (Optional[str]): Path to directory containing SQL templates.
                If None, tries to find templates relative to this file.
            debug (bool): Whether to print debug information.
        """
        self.templates_dir = templates_dir or self._find_templates_dir()
        self.debug = debug
        if self.debug:
            print(f"[DEBUG] Templates directory: {self.templates_dir}")

    def _find_templates_dir(self) -> str:
        """Find the SQL templates directory relative to this file."""
        # This file is in sql_interface/, templates should be in parent/sql_templates/
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        templates_dir = os.path.join(parent_dir, "sql_templates")
        
        if not os.path.isdir(templates_dir):
            raise QueryTemplateNotFoundError(
                f"SQL templates directory not found at {templates_dir}"
            )
        return templates_dir

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
        if not template_name.endswith('.sql'):
            template_name += '.sql'

        template_path = os.path.join(self.templates_dir, template_name)
        if not os.path.isfile(template_path):
            raise QueryTemplateNotFoundError(
                f"SQL template file not found: {template_path}"
            )

        with open(template_path, 'r', encoding='utf-8') as f:
            template = f.read()

        if self.debug:
            print(f"[DEBUG] Loaded template '{template_name}'")
        return template

    def execute_template_query(
        self, 
        db: SQLInterface,
        template_name: str,
        params: Dict[str, Any] = None
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
            query = self.load_query_template(template_name)
            if params:
                # For pyodbc, convert dict params to tuple in correct order
                param_values = tuple(params.values())
            else:
                param_values = ()

            if db.execute_query(query, param_values):
                return db.fetch_results()
            return None
        except QueryTemplateNotFoundError as e:
            print(f"Error: {e}")
            return None
        except Exception as e:
            print(f"Error executing template query: {e}")
            return None
