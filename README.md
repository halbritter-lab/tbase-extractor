# SQL Database Query Script

This Python script provides a command-line interface to connect to a Microsoft SQL Server database, retrieve patient information by ID, or list available tables. It uses environment variables for secure database credentials and supports saving query results to a JSON file.

## Project Idea

The goal of this project is to create a simple, reusable Python tool for interacting with a SQL Server database. It demonstrates secure credential management using `.env` files and provides flexible command-line options for performing common database tasks like querying specific records and inspecting database structure.

## Implementation Details

The script is implemented in Python and uses the following libraries:

* `pyodbc`: For connecting to and interacting with the SQL Server database.
* `python-dotenv`: For loading database credentials from a `.env` file, keeping sensitive information out of the main script.
* `argparse`: For handling command-line arguments, providing a user-friendly interface for specifying actions (query patient or list tables) and options (output file).
* `json`: For serializing query results into JSON format when saving to a file.
* `os`: Used for checking and creating output directories.
* `datetime`, `date`: Used for handling and serializing date/datetime objects from the database results into a JSON-compatible format.

The script defines a `SQLInterface` class to encapsulate the database connection and query logic, making the main execution block cleaner and leveraging context managers (`with`) for reliable connection handling.

## Features

* Secure database connection using environment variables (`.env`).
* Connects to Microsoft SQL Server using `pyodbc`.
* Query patient data by providing a patient ID via command-line argument (`-i` or `--patient-id`).
* List all available tables in the database via a command-line argument (`-l` or `--list-tables`).
* Mutually exclusive arguments: You must specify either a patient ID to query or request the table list.
* Optional output to a JSON file (`-o` or `--output`) when querying a patient.
* Automatically creates output directories if they don't exist.
* Handles JSON serialization of date/datetime objects.
* Basic error handling for database connection and query execution.

## Prerequisites

* Python 3.6 or higher
* `pyodbc` library
* `python-dotenv` library
* An ODBC driver for SQL Server installed on your system (e.g., "SQL Server Native Client 10.0" as specified in the script, or configure your `.env` to specify a different one).
* Access to a Microsoft SQL Server database.

## Installation

1.  Clone or download the script file.
2.  Install the required Python libraries:
    ```bash
    pip install pyodbc python-dotenv
    ```

## Setup

1.  Create a file named `.env` in the same directory as the script.
2.  Add your database connection details to the `.env` file in the following format:
    ```dotenv
    SQL_SERVER=<your_sql_server_name_or_ip>
    DATABASE=<your_database_name>
    USERNAME_SQL=<your_sql_server_username>
    PASSWORD=<your_sql_server_password>
    # Optional: Specify your ODBC driver if different from the default in the script
    # DRIVER={Your ODBC Driver Name}
    ```
    Replace the placeholder values with your actual database credentials.

## Usage

Run the script from your terminal. You must provide either the patient ID (`-i`) or the list tables flag (`-l`).

* **Query a patient and print results to the console:**
    ```bash
    python sql_interface.py -i 12345
    ```
    or
    ```bash
    python sql_interface.py --patient-id 67890
    ```

* **Query a patient and save results to a JSON file:**
    ```bash
    python sql_interface.py -i 12345 -o patient_data.json
    ```
    or save to a specific directory (directory will be created if it doesn't exist):
    ```bash
    python sql_interface.py --patient-id 67890 --output output/patient_details.json
    ```

* **List available tables:**
    ```bash
    python sql_interface.py -l
    ```
    or
    ```bash
    python sql_interface.py --list-tables
    ```

* **Get help (view available options):**
    ```bash
    python sql_interface.py -h
    ```
    or
    ```bash
    python sql_interface.py --help
    ```

## Error Handling

The script includes basic error handling for database connection issues and query execution errors. It also handles the case where a specified patient ID is not found. Errors during file saving are also caught and reported.

## License

This project is licensed under the MIT License - see the LICENSE file for details (Note: A LICENSE file is not included in this README, you may want to add one to your project).