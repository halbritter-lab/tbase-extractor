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
    *   Saves results to a specified JSON file.
*   **User-Friendly CLI:** Uses `argparse` with subparsers (`list-tables`, `query`) for clear command structure.
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

Run the script from your terminal within the project's root directory. Use subparsers `list-tables` or `query` to specify the action.

*   **Get Help:**
    ```bash
    python main.py -h
    python main.py list-tables -h
    python main.py query -h
    ```

*   **List Available Tables:**
    ```bash
    python main.py list-tables
    ```

*   **Query Patient by ID (Console Output):**
    ```bash
    python main.py query --query-name patient-details --patient-id 12345
    ```
    *(Alias: `python main.py query -q patient-details -i 12345`)*

*   **Query Patient by ID (JSON Output):**
    ```bash
    python main.py query -q patient-details -i 12345 -o output/patient_12345.json
    ```

*   **Query Patient by Name and DOB (Console Output):**
    ```bash
    python main.py query --query-name patient-by-name-dob --first-name John --last-name Doe --dob 1990-05-20
    ```
    *(Aliases: `python main.py query -q patient-by-name-dob -fn John -ln Doe -d 1990-05-20`)*
    *(Note: Date format must be YYYY-MM-DD)*

*   **Query Patient by Name and DOB (JSON Output):**
    ```bash
    python main.py query -q patient-by-name-dob -fn Jane -ln Smith -d 1988-11-01 -o output/jane_smith_data.json
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

## License

This project is intended for educational purposes. You can adapt the license as needed. Consider adding a `LICENSE` file (e.g., with the MIT License text).