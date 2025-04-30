import pyodbc
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class SQLInterface:
    def __init__(self):
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("DATABASE")
        self.username_sql = os.getenv("USERNAME_SQL")  # Renamed variable
        self.password = os.getenv("PASSWORD")
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection_string = (
                f"DRIVER={{SQL Server Native Client 10.0}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username_sql};"  # Using the renamed variable
                f"PWD={self.password};"
            )
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            print("Successfully connected to the database.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error connecting to the database: {sqlstate}")
            return False

    def execute_query(self, query):
        if self.connection and self.cursor:
            try:
                self.cursor.execute(query)
                self.connection.commit()  # For INSERT, UPDATE, DELETE statements
                print("Query executed successfully.")
                return True
            except pyodbc.Error as ex:
                sqlstate = ex.args[0]
                print(f"Error executing query: {sqlstate}")
                self.connection.rollback() # Rollback changes on error
                return False
        else:
            print("Not connected to the database.")
            return False

    def fetch_results(self):
        if self.cursor:
            columns = [column[0] for column in self.cursor.description]
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return results
        else:
            print("No cursor available.")
            return None

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Connection closed.")

# Example Usage
if __name__ == "__main__":
    sql_interface = SQLInterface()

    # Ensure your .env file in the same directory looks like this:
    # SQL_SERVER=<your_sql_server_ip_address>
    # DATABASE=<your_database_name>
    # USERNAME_SQL=<your_sql_server_username>
    # PASSWORD=<your_sql_server_password>

    if sql_interface.connect():
        # Example: Execute a SELECT query
        query = "SELECT TOP 10 * FROM YourTableName"  # Replace 'YourTableName'
        if sql_interface.execute_query(query):
            results = sql_interface.fetch_results()
            if results:
                print("\nQuery Results:")
                for row in results:
                    print(row)

        # Example: Execute an INSERT query
        # insert_query = "INSERT INTO YourTableName (Column1, Column2) VALUES (?, ?)"
        # values = ('value1', 'value2')
        # try:
        #     sql_interface.cursor.execute(insert_query, values)
        #     sql_interface.connection.commit()
        #     print("Row inserted successfully.")
        # except pyodbc.Error as ex:
        #     sqlstate = ex.args[0]
        #     print(f"Error inserting data: {sqlstate}")
        #     sql_interface.connection.rollback()

        sql_interface.close_connection()