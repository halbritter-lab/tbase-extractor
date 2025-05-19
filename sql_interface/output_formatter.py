import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import sys
import io # For potential string buffering
import csv

# Optional: Use tabulate for nicer console tables
# Run: pip install tabulate
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False
    print("Note: 'tabulate' library not found. Console table formatting will be basic.")
    print("      Install using: pip install tabulate")


class OutputFormatter:
    """Formats query results (list of dictionaries) for display or saving."""

    @staticmethod
    def _datetime_serializer(obj: Any) -> str:
        """
        Custom serializer for converting datetime.datetime and datetime.date
        objects into ISO 8601 string format for JSON compatibility.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Let the default JSON encoder handle other types or raise TypeError
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    @staticmethod
    def format_as_json(data: List[Dict[str, Any]], indent: Optional[int] = 4) -> str:
        """
        Formats the data into a JSON string.

        Handles date/datetime objects using the custom serializer.

        Args:
            data (List[Dict[str, Any]]): The query result data.
            indent (Optional[int]): The indentation level for pretty-printing JSON.
                                    Set to None for compact output. Defaults to 4.

        Returns:
            str: The JSON formatted string representation of the data.

        Raises:
            TypeError: If the data contains non-serializable types not handled
                       by the _datetime_serializer.
            ValueError: If there are issues during JSON encoding.
        """
        try:
            return json.dumps(data, default=OutputFormatter._datetime_serializer, indent=indent)
        except (TypeError, ValueError) as e:
            print(f"Error during JSON serialization: {e}", file=sys.stderr)
            # Re-raise or handle as appropriate for the application
            raise

    @staticmethod
    def format_as_console_table(data: List[Dict[str, Any]], stream=sys.stdout) -> None:
        """
        Prints the data as a formatted table to the specified stream (default: stdout).

        Uses the 'tabulate' library if available for better formatting, otherwise
        falls back to a basic, manually aligned format.

        Args:
            data (List[Dict[str, Any]]): The query result data.
            stream: The output stream (e.g., sys.stdout, io.StringIO). Defaults to sys.stdout.
        """
        if not data:
            print("No results to display.", file=stream)
            return

        # Assume all dictionaries have the same keys based on the first row
        # This is generally true for SQL query results.
        headers = list(data[0].keys())

        if HAS_TABULATE:
            # tabulate expects a list of lists (rows) + headers
            # It handles various data types including None reasonably well.
            rows = [[row.get(header) for header in headers] for row in data]
            try:
                # 'grid' format looks nice; other options: 'psql', 'simple', 'rst'
                print(tabulate(rows, headers=headers, tablefmt="grid", missingval="NULL"), file=stream)
            except Exception as e:
                # Catch potential errors within tabulate itself
                print(f"\nError using tabulate: {e}. Falling back to basic format.", file=stream)
                OutputFormatter._print_basic_table(data, headers, stream)

        else:
            # Fallback to basic formatting if tabulate is not installed
            OutputFormatter._print_basic_table(data, headers, stream)

    @staticmethod
    def _print_basic_table(data: List[Dict[str, Any]], headers: List[str], stream=sys.stdout) -> None:
        """Internal helper for basic manual table printing."""
        print("\n--- Basic Table Output ---", file=stream)

        # Calculate column widths (simple approach)
        col_widths = {header: len(header) for header in headers}
        for row in data:
            for header in headers:
                col_widths[header] = max(col_widths[header], len(str(row.get(header, 'NULL'))))

        # Create separator line
        separator = "+-" + "-+-".join("-" * col_widths[h] for h in headers) + "-+"

        # Print header
        header_line = "| " + " | ".join(h.ljust(col_widths[h]) for h in headers) + " |"
        print(separator, file=stream)
        print(header_line, file=stream)
        print(separator, file=stream)

        # Print rows
        for row in data:
            row_line = "| " + " | ".join(str(row.get(h, 'NULL')).ljust(col_widths[h]) for h in headers) + " |"
            print(row_line, file=stream)

        print(separator, file=stream)

    @staticmethod
    def format_as_csv(data: List[Dict[str, Any]], delimiter: str = ',', lineterminator: str = '\n') -> str:
        """
        Formats the data as CSV or TSV (by changing delimiter).
        Returns the CSV/TSV as a string (including header row).
        Handles date/datetime objects by converting to ISO strings.

        Args:
            data (List[Dict[str, Any]]): The query result data.
            delimiter (str): The delimiter for separating values (default: ',').
                             Use '\t' for tab-separated values (TSV).
            lineterminator (str): The string used to terminate lines (default: '\n').

        Returns:
            str: The CSV/TSV formatted string representation of the data.
        """
        if not data:
            return ''
        headers = list(data[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(
            output, fieldnames=headers, delimiter=delimiter, lineterminator=lineterminator, quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        for row in data:
            # Convert date/datetime to ISO string for CSV/TSV
            safe_row = {k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in row.items()}
            writer.writerow(safe_row)
        return output.getvalue()

    @staticmethod
    def format_as_tsv(data: List[Dict[str, Any]]) -> str:
        """
        Formats the data as TSV (tab-separated values).
        Returns the TSV as a string (including header row).

        Args:
            data (List[Dict[str, Any]]): The query result data.

        Returns:
            str: The TSV formatted string representation of the data.
        """
        return OutputFormatter.format_as_csv(data, delimiter='\t')