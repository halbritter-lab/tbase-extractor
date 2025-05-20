import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import sys
import io  # For potential string buffering
import csv
from ..matching.models import MatchCandidate, MatchInfo

# Optional: Use tabulate for nicer console tables
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
    def _match_candidate_to_dict(candidate: MatchCandidate) -> Dict[str, Any]:
        """Convert a MatchCandidate to a dictionary suitable for output."""
        result = {
            'overall_score': candidate.overall_score,
            'primary_match_type': candidate.primary_match_type,
            **candidate.db_record
        }
        
        # Add match details for each field
        for info in candidate.match_fields_info:
            field_prefix = info.field_name
            result[f"{field_prefix}_input_value"] = info.input_value
            result[f"{field_prefix}_db_value"] = info.db_value
            result[f"{field_prefix}_match_type"] = info.match_type
            result[f"{field_prefix}_similarity"] = info.similarity_score
            if info.details:
                result[f"{field_prefix}_details"] = info.details

        return result

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
    def format_as_json(data: List[Any], indent: Optional[int] = 4) -> str:
        """
        Formats the data into a JSON string.

        Handles date/datetime objects using the custom serializer.

        Args:
            data (List[Any]): The query result data.
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
            # Convert MatchCandidate objects to dictionaries if present
            if data and isinstance(data[0], MatchCandidate):
                data = [OutputFormatter._match_candidate_to_dict(candidate) for candidate in data]

            return json.dumps(data, default=OutputFormatter._datetime_serializer, indent=indent)
        except (TypeError, ValueError) as e:
            print(f"Error during JSON serialization: {e}", file=sys.stderr)
            # Re-raise or handle as appropriate for the application
            raise

    @staticmethod
    def format_as_csv(data: List[Dict[str, Any]]) -> str:
        """Formats the data into a CSV string."""
        if not data:
            return ""

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    @staticmethod
    def format_as_tsv(data: List[Dict[str, Any]]) -> str:
        """Formats the data into a TSV string."""
        if not data:
            return ""

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys(), delimiter='\t')
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    @staticmethod
    def format_as_console_table(data: List[Any], stream=sys.stdout) -> None:
        """Formats data as a console table and writes to the given stream."""
        if not data:
            print("No data to display.", file=stream)
            return

        # Check if data contains MatchCandidate objects
        if isinstance(data[0], MatchCandidate):
            headers = ["Name", "DOB", "Score", "Match Type"]
            rows = [
                [
                    candidate.db_record.get("Name"),
                    candidate.db_record.get("Geburtsdatum"),
                    candidate.overall_score,
                    candidate.primary_match_type
                ]
                for candidate in data
            ]
        else:
            headers = list(data[0].keys())
            rows = [list(row.values()) for row in data]

        if HAS_TABULATE:
            from tabulate import tabulate
            table = tabulate(rows, headers=headers, tablefmt="grid")
            print(table, file=stream)
        else:
            # Fallback to basic formatting
            print("\t".join(headers), file=stream)
            for row in data:
                print("\t".join(str(row.get(h, "")) for h in headers), file=stream)
