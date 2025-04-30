import os
from typing import Tuple, Optional, Dict, Any

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
        # Resolve the absolute path to handle relative paths correctly
        self.templates_dir = os.path.abspath(templates_base_dir)

        # Optional: Check if the directory exists during initialization
        if not os.path.isdir(self.templates_dir):
            # This could be a warning or raise a configuration error
            print(f"Warning: SQL template directory not found at expected location: {self.templates_dir}")
            # raise FileNotFoundError(f"SQL template directory not found: {self.templates_dir}")

    def _load_template(self, template_name: str) -> str:
        """
        Loads the content of a specific SQL template file.

        Args:
            template_name (str): The base name of the .sql file (without extension).

        Returns:
            str: The content of the SQL template file.

        Raises:
            QueryTemplateNotFoundError: If the specified template file does not exist.
            IOError: If there is an error reading the file.
        """
        file_path = os.path.join(self.templates_dir, f"{template_name}.sql")
        if not os.path.isfile(file_path):
             raise QueryTemplateNotFoundError(f"Query template '{template_name}.sql' not found in {self.templates_dir}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f: # Specify encoding
                return f.read()
        except IOError as e:
            # Catch potential read errors after confirming file exists
            raise IOError(f"Error reading template '{template_name}.sql': {e}") from e
        except Exception as e: # Catch unexpected errors
             raise Exception(f"An unexpected error occurred while loading template '{template_name}.sql': {e}") from e


    def get_query(self, template_name: str, params_tuple: Tuple = ()) -> Optional[Tuple[str, Tuple]]:
        """
        Retrieves the SQL query string from a template and pairs it with parameters.

        This is the core method for fetching queries. It handles loading the template
        and packaging it with the provided parameters.

        Args:
            template_name (str): The base name of the .sql file (without extension).
            params_tuple (Tuple): A tuple containing parameters for the query placeholders ('?').

        Returns:
            Optional[Tuple[str, Tuple]]: A tuple containing (sql_string, params_tuple),
                                         or None if the template loading fails.
        """
        try:
            sql = self._load_template(template_name)
            # Basic validation (optional): Check if param count matches '?' count
            # placeholder_count = sql.count('?')
            # if len(params_tuple) != placeholder_count:
            #     print(f"Warning: Parameter count mismatch for template '{template_name}'. "
            #           f"Expected {placeholder_count}, got {len(params_tuple)}.")
            #     # Decide whether to proceed or return None/raise error
            return sql, params_tuple
        except (QueryTemplateNotFoundError, IOError, Exception) as e:
            print(f"Error getting query '{template_name}': {e}") # Log the specific error
            return None # Return None to indicate failure

    # --- Convenience methods for specific, commonly used queries ---
    # These methods provide a clearer interface for specific actions in main.py

    def get_list_tables_query(self) -> Optional[Tuple[str, Tuple]]:
        """Gets the query and parameters (empty tuple) to list tables."""
        return self.get_query('list_tables') # No parameters needed

    def get_patient_by_id_query(self, patient_id: int) -> Optional[Tuple[str, Tuple]]:
        """Gets the query and parameters to find a patient by ID."""
        if not isinstance(patient_id, int):
             print("Error: patient_id must be an integer.")
             return None
        return self.get_query('get_patient_by_id', (patient_id,)) # Pass ID as a single-element tuple

    def get_patient_visits_query(self, patient_id: int) -> Optional[Tuple[str, Tuple]]:
        """Gets the query and parameters to find a patient and their visits."""
        if not isinstance(patient_id, int):
             print("Error: patient_id must be an integer.")
             return None
        return self.get_query('get_patient_with_visits_join', (patient_id,))

    # Add more convenience methods here as needed for other templates
    # def get_orders_by_date_range_query(self, start_date: date, end_date: date) -> Optional[Tuple[str, Tuple]]:
    #     return self.get_query('get_orders_by_date', (start_date, end_date))