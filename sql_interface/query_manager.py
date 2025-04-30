# sql_interface/query_manager.py
import os
from typing import Tuple, Optional, Any # Added Any for date object flexibility initially
from datetime import date # Import date for type hinting

# Import custom exception
from .exceptions import QueryTemplateNotFoundError

class QueryManager:
    """Loads and manages SQL queries from template files."""

    def __init__(self, templates_base_dir: str = 'sql_templates'):
        """
        Initializes the QueryManager.

        Args:
            templates_base_dir (str): Path to the directory containing .sql files.
                                      Defaults to 'sql_templates' relative to the
                                      current working directory where the script is run.
        """
        self.templates_dir = os.path.abspath(templates_base_dir)
        if not os.path.isdir(self.templates_dir):
            print(f"Warning: SQL template directory not found at expected location: {self.templates_dir}")

    def _load_template(self, template_name: str) -> str:
        """Loads the content of a specific SQL template file."""
        file_path = os.path.join(self.templates_dir, f"{template_name}.sql")
        if not os.path.isfile(file_path):
             raise QueryTemplateNotFoundError(f"Query template '{template_name}.sql' not found in {self.templates_dir}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except IOError as e:
            raise IOError(f"Error reading template '{template_name}.sql': {e}") from e
        except Exception as e:
             raise Exception(f"An unexpected error occurred while loading template '{template_name}.sql': {e}") from e

    def get_query(self, template_name: str, params_tuple: Tuple = ()) -> Optional[Tuple[str, Tuple]]:
        """Retrieves the SQL query string from a template and pairs it with parameters."""
        try:
            sql = self._load_template(template_name)
            return sql, params_tuple
        except (QueryTemplateNotFoundError, IOError, Exception) as e:
            print(f"Error getting query '{template_name}': {e}")
            return None

    # --- Convenience methods ---

    def get_list_tables_query(self) -> Optional[Tuple[str, Tuple]]:
        """Gets the query and parameters (empty tuple) to list tables."""
        return self.get_query('list_tables')

    def get_patient_by_id_query(self, patient_id: int) -> Optional[Tuple[str, Tuple]]:
        """Gets the query and parameters to find a patient by ID."""
        if not isinstance(patient_id, int):
             print("Error: patient_id must be an integer.")
             return None
        return self.get_query('get_patient_by_id', (patient_id,))

    # --- NEW METHOD ---
    def get_patient_by_name_dob_query(self, first_name: str, last_name: str, dob: date) -> Optional[Tuple[str, Tuple[Any, ...]]]:
        """
        Gets the query and parameters to find a patient by first name, last name, and date of birth.

        Args:
            first_name (str): Patient's first name.
            last_name (str): Patient's last name.
            dob (date): Patient's date of birth as a datetime.date object.

        Returns:
            Optional[Tuple[str, Tuple]]: SQL query string and parameters tuple, or None on error.
        """
        # Basic type checks (more robust validation often done in main.py)
        if not all(isinstance(arg, expected_type) for arg, expected_type in
                   [(first_name, str), (last_name, str), (dob, date)]):
            print("Error: Invalid parameter types for get_patient_by_name_dob_query. "
                  "Expected (str, str, date).")
            return None
        # Prepare parameters in the correct order for the SQL template
        params = (first_name, last_name, dob)
        return self.get_query('get_patient_by_name_dob', params)
