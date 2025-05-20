# SQL Database Query Script (Refactored)

This Python script provides a command-line interface to connect to a Microsoft SQL Server database, execute predefined queries (like retrieving patient information or listing tables), and output results to the console or a JSON file. It uses environment variables for secure database credentials and SQL templates for maintainable queries.

## Project Idea

The goal of this project is to create a simple, reusable, and maintainable Python tool for interacting with a SQL Server database. It demonstrates secure credential management, modular code structure, the use of SQL templates, and a flexible command-line interface.

## Implementation Details

The application follows a modular structure:

*   **`main.py`**: The main entry point, handling argument parsing and orchestrating the workflow.
*   **`sql_interface/`**: A Python package containing the core logic:
    *   `db_interface.py`: Manages the database connection (`SQLInterface` class) using `pyodbc`.
    *   `query_manager.py`: Loads SQL queries from template files located in the `sql_templates/` directory.
    *   `output_formatter.py`: Formats query results for console (using `tabulate` if available) or JSON output.
    *   `exceptions.py`: Defines custom exception classes for specific errors.
*   **`sql_templates/`**: Directory holding `.sql` files containing the parameterized query text.

Key Libraries Used:

*   `pyodbc`: For connecting to and interacting with the SQL Server database.
*   `python-dotenv`: For loading database credentials from a `.env` file.
*   `argparse`: For handling command-line arguments via subparsers for different actions.
*   `tabulate`: (Optional but recommended) For well-formatted console table output.
*   `json`: For serializing query results into JSON format.
*   `os`, `sys`, `datetime`: Standard libraries for path manipulation, system interaction, and date handling.

## Features

*   **Secure Configuration:** Uses a `.env` file for database credentials and driver information.
*   **Modular Design:** Code is separated into logical modules for better readability, maintainability, and testability.
*   **SQL Templates:** SQL queries are stored in `.sql` files, keeping complex SQL separate from Python code. Parameterized queries (`?`) are used to prevent SQL injection.
*   **Multiple Query Types:**
    *   List available database tables.
    *   Query patient data by `PatientID`.
    *   Query patient data by `FirstName`, `LastName`, and `DateOfBirth`. (Easily extensible with more templates).
*   **Flexible Output:**
    *   Prints results to the console in a formatted table (`tabulate` preferred).
    *   Saves results to a specified file in **JSON**, **CSV**, or **TSV** format (see `--format`).
    *   Use `--format` to select output type: `json`, `csv`, `tsv`, or `stdout` (pretty table to console).
    *   If `--output` is not given, formatted output is printed to the console.
*   **User-Friendly CLI:** Uses `argparse` with subparsers (`list-tables`, `query`) for clear command structure. Now supports `--format` for output type selection.
*   **Date Handling:** Validates date input format and correctly serializes date/datetime objects for JSON output.
*   **Error Handling:** Includes basic error handling for connection issues, query execution, template loading, and file I/O.

## Prerequisites

*   Python 3.7 or higher (due to type hinting usage, though might work on 3.6)
*   Access to a Microsoft SQL Server database.
*   An **ODBC Driver for SQL Server** installed on your system (e.g., "ODBC Driver 17 for SQL Server", "SQL Server Native Client 11.0"). The exact name needs to match the `SQL_DRIVER` setting in your `.env` file.

## Installation

1.  Clone or download the project files.
2.  **(Recommended)** Create and activate a Python virtual environment:
    ```bash
    python -m venv venv
    # Windows: .\venv\Scripts\activate
    # macOS/Linux: source venv/bin/activate
    ```
3.  Install the required Python libraries from the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```
    *(This installs `pyodbc`, `python-dotenv`, and `tabulate`)*
4.  Install the package itself:
    ```bash
    pip install .
    ```
    Or for a development/editable install:
    ```bash
    pip install -e .
    ```

This will install the `tbase-extractor` command and all dependencies.

## Setup

1.  Create a file named `.env` in the project's root directory (alongside `main.py`).
2.  Add your database connection details to the `.env` file. **Crucially, ensure the `SQL_DRIVER` value exactly matches the name of the ODBC driver installed on your system.**
    ```dotenv
    # .env
    SQL_SERVER=<your_sql_server_name_or_ip>
    DATABASE=<your_database_name>
    USERNAME_SQL=<your_sql_username>
    PASSWORD=<your_sql_password>
    SQL_DRIVER="{ODBC Driver 17 for SQL Server}" # <-- Example: Replace with your actual driver name
    ```
3.  Verify that the table and column names used in the `.sql` files within the `sql_templates/` directory (`dbo.Patient`, `PatientID`, `Vorname`, `Name`, `Geburtsdatum`) match your actual database schema. Adjust the templates if necessary.

## Usage

You can now run the tool from any directory:

*   **Get Help:**
    ```bash
    tbase-extractor --help
    tbase-extractor list-tables --help
    tbase-extractor query --help
    ```

*   **List Available Tables:**
    ```bash
    tbase-extractor list-tables
    ```

*   **Query Patient by ID (Console Output):**
    ```bash
    tbase-extractor query --query-name patient-details --patient-id 12345
    ```
    *(Alias: `tbase-extractor query -q patient-details -i 12345`)*

*   **Query Patient by ID (JSON Output):**
    ```bash
    tbase-extractor query -q patient-details -i 12345 -o output/patient_12345.json
    ```

*   **Query Patient by ID (CSV Output):**
    ```bash
    tbase-extractor query -q patient-details -i 12345 -f csv -o output/patient_12345.csv
    ```

*   **Query Patient by Name and DOB (Console Output):**
    ```bash
    tbase-extractor query --query-name patient-by-name-dob --first-name John --last-name Doe --dob 1990-05-20
    ```
    *(Aliases: `tbase-extractor query -q patient-by-name-dob -fn John -ln Doe -d 1990-05-20`)*
    *(Note: Date format must be YYYY-MM-DD)*

*   **Query Patient by Name and DOB (JSON Output):**
    ```bash
    tbase-extractor query -q patient-by-name-dob -fn Jane -ln Smith -d 1988-11-01 -o output/jane_smith_data.json
    ```

*   **Query Patient by Name and DOB (TSV Output):**
    ```bash
    tbase-extractor query -q patient-by-name-dob -fn Jane -ln Smith -d 1988-11-01 -f tsv -o output/jane_smith_data.tsv
    ```

## Adding New Queries

1.  Create a new `.sql` file in the `sql_templates/` directory (e.g., `get_orders_by_customer.sql`). Use `?` for parameters.
2.  Add a corresponding convenience method in `sql_interface/query_manager.py` (e.g., `get_orders_by_customer_query(self, customer_id)`).
3.  Update the `choices` for `--query-name` in `main.py`'s `setup_arg_parser` function.
4.  Add necessary command-line arguments (e.g., `--customer-id`) to the `query` subparser in `main.py`.
5.  Add an `elif` block in `main.py` to handle the new `--query-name`, validate its arguments, and call the appropriate `query_manager` method.

## Error Handling

The script includes handling for:

*   Missing or incomplete `.env` configuration.
*   Database connection errors (reporting SQLSTATE).
*   Query execution errors (reporting SQLSTATE).
*   Failure to fetch results.
*   SQL template file not found (`QueryTemplateNotFoundError`).
*   Missing required command-line arguments for specific queries.
*   Incorrect date format for the `--dob` argument.
*   Errors during JSON serialization or file output.

## Recent Updates

### May 2025
- **Improved Help Functionality**: Enhanced the `--help` command to provide clearer and more detailed descriptions for all arguments and subcommands.
- **Bug Fixes**: Resolved an issue where `%` characters in help strings caused errors in the `argparse` module.
- **Output Enhancements**: Added support for saving query results in multiple formats (JSON, CSV, TSV) with improved formatting options.

### Usage Example Updates
- **Query Patient by Name and DOB**:
  ```powershell
  python -m tbase_extractor query --query-name patient-by-name-dob --first-name Jane --last-name Doe --dob 1990-05-20 -o output/patient_data.json
  ```
  *(Note: Ensure the date format is `YYYY-MM-DD`.)*

- **List Available Tables**:
  ```powershell
  python -m tbase_extractor list-tables
  ```

## License

MIT