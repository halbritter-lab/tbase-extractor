import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import sys
import io  # For potential string buffering
import csv
import logging
from ..matching.models import MatchCandidate, MatchInfo

# Initialize logger
logger = logging.getLogger(__name__)

# Optional: Use tabulate for nicer console tables
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False
    logger.warning("'tabulate' library not found. Console table formatting will be basic.")
    logger.info("To install tabulate, run: pip install tabulate")

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
    def format_as_json(data_payload: List[Any], metadata: Dict[str, Any] = None, indent: Optional[int] = 4) -> str:
        """
        Formats the data payload and metadata into a structured JSON string.

        The output JSON will have two top-level keys: ""metadata"" and ""data"".
        Handles date/datetime objects using a custom serializer.

        Args:
            data_payload (List[Any]): The query result data.
            metadata (Dict[str, Any]): The metadata dictionary for the query.
                                     If None, no metadata will be included.
            indent (Optional[int]): The indentation level for pretty-printing JSON.
                                  Set to None for compact output. Defaults to 4.

        Returns:
            str: The JSON formatted string representation of the structured data.

        Raises:
            TypeError: If the data contains non-serializable types not handled
                     by the _datetime_serializer.
            ValueError: If there are issues during JSON encoding.
        """
        try:
            # Ensure indent is an integer or None
            if indent is not None and not isinstance(indent, int):
                indent = 4  # Default to 4 spaces if indent is provided but not an integer
                
            # Create the structured output
            structured_output = {
                "metadata": metadata or {},
                "data": data_payload
            }

            # Convert MatchCandidate objects in data_payload to dictionaries if present
            if isinstance(data_payload, list) and data_payload and hasattr(data_payload[0], 'match_fields_info') and hasattr(data_payload[0], 'overall_score'):
                processed_payload = []
                for candidate in data_payload:
                    processed_payload.append(OutputFormatter._match_candidate_to_dict(candidate))
                structured_output["data"] = processed_payload

            return json.dumps(structured_output, default=OutputFormatter._datetime_serializer, indent=indent)
        except (TypeError, ValueError) as e:
            logger.error(f"Error during JSON serialization: {e}")
            # Re-raise for proper error handling by caller
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
    def format_as_txt(data: List[Dict[str, Any]]) -> str:
        """
        Formats the data as a simple text file with one cell value per line.
        No metadata or headers are included.
        
        Args:
            data (List[Dict[str, Any]]): The data to format
            
        Returns:
            str: Text with one cell value per line
        """
        if not data:
            return ""
            
        cell_values = []
        # Extract all values from all records and add each complete value as a line
        for record in data:
            for value in record.values():
                if value is not None:
                    # Convert to string if needed
                    if isinstance(value, (int, float, bool)):
                        value = str(value)
                    elif not isinstance(value, str):
                        continue
                        
                    # Add the complete cell value as a single line (strip whitespace)
                    cell_value = str(value).strip()
                    if cell_value:  # Skip empty values
                        cell_values.append(cell_value)
        
        # Join all cell values with newlines
        return '\n'.join(cell_values)

    @staticmethod
    def format_as_txt_optimized(data: List[Dict[str, Any]]) -> str:
        """
        Formats the data as an optimized text file that groups patient information.
        Patient data (Name, Vorname, etc.) is shown once, followed by varying data.
        
        Args:
            data (List[Dict[str, Any]]): The data to format
            
        Returns:
            str: Optimized text with patient info shown once, then varying data
        """
        if not data:
            return ""
        
        # Identify patient-related fields that typically remain constant
        patient_fields = ['Name', 'Vorname', 'PatientID', 'FirstName', 'LastName', 'Geburtsdatum', 'DOB']
        # Identify diagnosis/varying fields
        varying_fields = ['ICD10', 'Bezeichnung', 'Diagnosis', 'Code', 'Description']
        
        # Separate patient info and varying info
        patient_info = {}
        varying_data = []
        
        first_record = data[0]
        
        # Extract patient information from first record
        for field in patient_fields:
            if field in first_record and first_record[field] is not None:
                patient_info[field] = first_record[field]
        
        # Extract varying information from all records
        for record in data:
            varying_record = {}
            for key, value in record.items():
                # Include varying fields or fields not in patient_fields
                if key in varying_fields or (key not in patient_fields and value is not None):
                    varying_record[key] = value
            if varying_record:  # Only add if there's varying data
                varying_data.append(varying_record)
        
        # Build output
        cell_values = []
        
        # Add patient information first
        for value in patient_info.values():
            if value is not None:
                if isinstance(value, (int, float, bool)):
                    value = str(value)
                elif not isinstance(value, str):
                    continue
                    
                # Add the complete cell value as a single line (strip whitespace)
                cell_value = str(value).strip()
                if cell_value:  # Skip empty values
                    cell_values.append(cell_value)
        
        # Add separator if we have both patient info and varying data
        if patient_info and varying_data:
            cell_values.append("---")  # Separator between patient info and diagnoses
        
        # Add varying information
        for record in varying_data:
            for value in record.values():
                if value is not None:
                    if isinstance(value, (int, float, bool)):
                        value = str(value)
                    elif not isinstance(value, str):
                        continue
                        
                    # Add the complete cell value as a single line (strip whitespace)
                    cell_value = str(value).strip()
                    if cell_value:  # Skip empty values
                        cell_values.append(cell_value)
        
        return '\n'.join(cell_values)

    @staticmethod
    def format_as_json_optimized(data: List[Dict[str, Any]], metadata: Dict[str, Any] = None, indent: Optional[int] = 4) -> str:
        """
        Formats the data as an optimized JSON that groups patient information.
        Patient data (Name, Vorname, etc.) is shown once, followed by varying data.
        
        Args:
            data (List[Dict[str, Any]]): The data to format
            metadata (Dict[str, Any]): Optional metadata to include
            indent (Optional[int]): JSON indentation level
            
        Returns:
            str: Optimized JSON with patient info grouped
        """
        if not data:
            structured_output = {
                "metadata": metadata or {},
                "data": []
            }
            return json.dumps(structured_output, default=OutputFormatter._datetime_serializer, indent=indent)
        
        # Identify patient-related fields that typically remain constant
        patient_fields = ['Name', 'Vorname', 'PatientID', 'FirstName', 'LastName', 'Geburtsdatum', 'DOB']
        # Identify diagnosis/varying fields
        varying_fields = ['ICD10', 'Bezeichnung', 'Diagnosis', 'Code', 'Description']
        
        # Separate patient info and varying info
        patient_info = {}
        varying_data = []
        
        first_record = data[0]
        
        # Extract patient information from first record
        for field in patient_fields:
            if field in first_record and first_record[field] is not None:
                patient_info[field] = first_record[field]
        
        # Extract varying information from all records
        for record in data:
            varying_record = {}
            for key, value in record.items():
                # Include varying fields or fields not in patient_fields
                if key in varying_fields or (key not in patient_fields and value is not None):
                    varying_record[key] = value
            if varying_record:  # Only add if there's varying data
                varying_data.append(varying_record)
        
        # Build optimized output structure
        optimized_data = {
            "patient_info": patient_info,
            "diagnoses": varying_data
        }
        
        structured_output = {
            "metadata": metadata or {},
            "data": optimized_data
        }
        
        return json.dumps(structured_output, default=OutputFormatter._datetime_serializer, indent=indent)

    @staticmethod
    def format_as_csv_optimized(data: List[Dict[str, Any]]) -> str:
        """
        Formats the data as an optimized CSV that groups patient information.
        Patient data is shown in the first rows, followed by varying data rows.
        
        Args:
            data (List[Dict[str, Any]]): The data to format
            
        Returns:
            str: Optimized CSV with patient info grouped
        """
        if not data:
            return ""
        
        # Identify patient-related fields that typically remain constant
        patient_fields = ['Name', 'Vorname', 'PatientID', 'FirstName', 'LastName', 'Geburtsdatum', 'DOB']
        # Identify diagnosis/varying fields
        varying_fields = ['ICD10', 'Bezeichnung', 'Diagnosis', 'Code', 'Description']
        
        # Separate patient info and varying info
        patient_info = {}
        varying_data = []
        
        first_record = data[0]
        
        # Extract patient information from first record
        for field in patient_fields:
            if field in first_record and first_record[field] is not None:
                patient_info[field] = first_record[field]
        
        # Extract varying information from all records
        for record in data:
            varying_record = {}
            for key, value in record.items():
                # Include varying fields or fields not in patient_fields
                if key in varying_fields or (key not in patient_fields and value is not None):
                    varying_record[key] = value
            if varying_record:  # Only add if there's varying data
                varying_data.append(varying_record)
        
        output = io.StringIO()
        
        # Write patient information section
        if patient_info:
            writer = csv.writer(output)
            writer.writerow(["Section", "Field", "Value"])
            for field, value in patient_info.items():
                writer.writerow(["Patient", field, value])
            writer.writerow([])  # Empty row separator
        
        # Write varying data section
        if varying_data:
            if patient_info:
                writer.writerow(["Diagnoses Section"])
            
            # Get all possible field names from varying data
            all_varying_fields = set()
            for record in varying_data:
                all_varying_fields.update(record.keys())
            all_varying_fields = sorted(list(all_varying_fields))
            
            dict_writer = csv.DictWriter(output, fieldnames=all_varying_fields)
            dict_writer.writeheader()
            dict_writer.writerows(varying_data)
        
        return output.getvalue()

    @staticmethod
    def format_as_console_table(data: List[Any], stream=sys.stdout) -> None:
        """Formats data as a console table and writes to the given stream."""
        if not data:
            logger.info("No data to display.")
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
            table = tabulate(rows, headers=headers, tablefmt="grid")
            print(table, file=stream)
        else:
            # Fallback to basic formatting
            logger.debug("Using basic table formatting (tabulate not available)")
            print("\t".join(headers), file=stream)
            for row in data:
                print("\t".join(str(row.get(h, "")) for h in headers), file=stream)
