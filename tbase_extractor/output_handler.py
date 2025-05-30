"""Output handling utilities for formatting and writing results."""
import os
import sys
import logging
from typing import Any, Dict, List, Optional
from .sql_interface.output_formatter import OutputFormatter
from .config import VALID_OUTPUT_FORMATS, FILE_EXTENSION_MAP, DEFAULT_FILE_ENCODING, sanitize_filename

logger = logging.getLogger(__name__)


def determine_output_format(user_format: Optional[str], output_file_path: Optional[str]) -> str:
    """Determines the effective output format based on user input and file extension."""
    if user_format:
        return user_format
    
    if output_file_path:
        _, ext = os.path.splitext(output_file_path)
        ext = ext.lower()
        
        if ext in FILE_EXTENSION_MAP:
            return FILE_EXTENSION_MAP[ext]
        else:
            if ext:
                logger.warning(
                    f"Output file extension '{ext}' for '{output_file_path}' is not recognized. "
                    f"Defaulting to 'json' format."
                )
            else:
                logger.warning(
                    f"No file extension for '{output_file_path}'. Defaulting to 'json' format."
                )
            return 'json'
    
    return 'stdout'


def process_match_candidates_for_tabular(results_envelope: List[Any]) -> List[Dict[str, Any]]:
    """Process MatchCandidate objects for CSV/TSV formats."""
    if not (isinstance(results_envelope, list) and results_envelope and 
            hasattr(results_envelope[0], 'match_fields_info') and 
            hasattr(results_envelope[0], 'overall_score')):
        return results_envelope
    
    processed_results = []
    for candidate in results_envelope:
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
                
        processed_results.append(result)
    
    return processed_results


def format_metadata_summary(metadata_dict: Optional[Dict[str, Any]]) -> str:
    """Format metadata dictionary as comment lines."""
    if not metadata_dict:
        return ''
    
    metadata_lines = [f"# {k}: {v}" for k, v in metadata_dict.items()]
    return '\n'.join(metadata_lines)


def write_output_to_file(
    file_path: str,
    results_envelope: List[Any],
    processed_results: List[Dict[str, Any]],
    effective_format: str,
    metadata_dict: Optional[Dict[str, Any]],
    output_formatter: OutputFormatter,
    optimize_txt: bool = False
) -> None:
    """Write formatted results to a file."""
    metadata_summary = format_metadata_summary(metadata_dict)
    
    # Check if we should use optimized formatting for patient-diagnosis data
    use_optimized = should_use_optimized_format(processed_results)
    
    with open(file_path, 'w', encoding=DEFAULT_FILE_ENCODING, newline='') as f:
        if effective_format == 'json':
            if use_optimized:
                logger.debug("Using optimized JSON format for patient-diagnosis data")
                f.write(output_formatter.format_as_json_optimized(processed_results, metadata_dict))
            else:
                f.write(output_formatter.format_as_json(results_envelope, metadata_dict))
        elif effective_format == 'csv':
            if metadata_summary:
                f.write(metadata_summary + '\n')
            if use_optimized:
                logger.debug("Using optimized CSV format for patient-diagnosis data")
                f.write(output_formatter.format_as_csv_optimized(processed_results))
            else:
                f.write(output_formatter.format_as_csv(processed_results))
        elif effective_format == 'tsv':
            if metadata_summary:
                f.write(metadata_summary + '\n')
            f.write(output_formatter.format_as_tsv(processed_results))
        elif effective_format == 'txt':
            # For txt format, no metadata or headers - use optimized format if requested or auto-detected
            if optimize_txt or use_optimized:
                if use_optimized:
                    logger.debug("Using optimized TXT format for patient-diagnosis data")
                f.write(output_formatter.format_as_txt_optimized(processed_results))
            else:
                f.write(output_formatter.format_as_txt(processed_results))
        elif effective_format == 'stdout':
            if metadata_summary:
                f.write(metadata_summary + '\n')
            import io
            buf = io.StringIO()
            output_formatter.format_as_console_table(results_envelope, stream=buf)
            f.write(buf.getvalue())
        else:
            raise ValueError(f"Unknown output format: {effective_format}")


def write_output_to_stdout(
    results_envelope: List[Any],
    processed_results: List[Dict[str, Any]],
    effective_format: str,
    metadata_dict: Optional[Dict[str, Any]],
    output_formatter: OutputFormatter,
    optimize_txt: bool = False
) -> None:
    """Write formatted results to stdout."""
    metadata_summary = format_metadata_summary(metadata_dict)
    
    # Check if we should use optimized formatting for patient-diagnosis data
    use_optimized = should_use_optimized_format(processed_results)
    
    if effective_format == 'json':
        if use_optimized:
            logger.debug("Using optimized JSON format for patient-diagnosis data")
            print(output_formatter.format_as_json_optimized(processed_results, metadata_dict))
        else:
            print(output_formatter.format_as_json(results_envelope, metadata_dict))
    elif effective_format == 'csv':
        if metadata_summary:
            print(metadata_summary)
        if use_optimized:
            logger.debug("Using optimized CSV format for patient-diagnosis data")
            print(output_formatter.format_as_csv_optimized(processed_results))
        else:
            print(output_formatter.format_as_csv(processed_results))
    elif effective_format == 'tsv':
        if metadata_summary:
            print(metadata_summary)
        print(output_formatter.format_as_tsv(processed_results))
    elif effective_format == 'txt':
        # For txt format, no metadata or headers - use optimized format if requested or auto-detected
        if optimize_txt or use_optimized:
            if use_optimized:
                logger.debug("Using optimized TXT format for patient-diagnosis data")
            print(output_formatter.format_as_txt_optimized(processed_results))
        else:
            print(output_formatter.format_as_txt(processed_results))
    elif effective_format == 'stdout':
        if metadata_summary:
            print(metadata_summary)
        output_formatter.format_as_console_table(results_envelope, stream=sys.stdout)
    else:
        raise ValueError(f"Unknown output format: {effective_format}")


def generate_split_filename(
    row_data: Dict[str, Any],
    filename_template: str,
    file_index: int
) -> str:
    """Generate filename for split output using template."""
    try:
        row_filename = filename_template.format(**row_data)
    except KeyError as e:
        logger.warning(f"Field {e} not found in row data. Using a generic filename.")
        row_filename = f"output_{file_index}"
    
    # Sanitize the filename
    row_filename = sanitize_filename(row_filename)
    if not row_filename:
        row_filename = f"output_{file_index}"
    
    return row_filename


def group_results_by_patient(
    processed_results: List[Dict[str, Any]],
    filename_template: str
) -> Dict[str, List[Dict[str, Any]]]:
    """Group results by patient based on the filename template fields."""
    grouped_data = {}
    
    for i, row_data in enumerate(processed_results, 1):
        try:
            # Generate the same key that would be used for filename
            group_key = generate_split_filename(row_data, filename_template, i)
        except KeyError as e:
            logger.warning(f"Field {e} not found in row data for grouping. Using generic key.")
            group_key = f"patient_{i}"
        
        if group_key not in grouped_data:
            grouped_data[group_key] = []
        grouped_data[group_key].append(row_data)
    
    return grouped_data


def handle_split_output(
    output_file_path: str,
    processed_results: List[Dict[str, Any]],
    effective_format: str,
    metadata_dict: Optional[Dict[str, Any]],
    filename_template: str,
    output_formatter: OutputFormatter,
    optimize_txt: bool = False
) -> int:
    """Handle split output - save data grouped by patient as separate files."""
    if not processed_results:
        return 0
    
    output_dir = os.path.dirname(output_file_path) or os.getcwd()
    _, file_ext = os.path.splitext(output_file_path)
    if not file_ext:
        file_ext = '.' + effective_format if effective_format != 'stdout' else '.txt'
    
    # Group results by patient
    grouped_data = group_results_by_patient(processed_results, filename_template)
    
    files_saved = 0
    for group_key, patient_rows in grouped_data.items():
        try:
            row_file_path = os.path.join(output_dir, f"{group_key}{file_ext}")
            
            # Prepare metadata for this patient group
            group_metadata = metadata_dict.copy() if metadata_dict else {}
            group_metadata['row_count_fetched'] = len(patient_rows)
            
            write_output_to_file(
                row_file_path, patient_rows, patient_rows, 
                effective_format, group_metadata, output_formatter, optimize_txt
            )
            
            files_saved += 1
            logger.debug(f"Saved {len(patient_rows)} rows for patient group '{group_key}' to {row_file_path}")
            
        except Exception as e:
            logger.error(f"Error saving split output file for patient group '{group_key}': {e}")
    
    logger.info(f"Created {files_saved} patient files from {len(processed_results)} total rows")
    return files_saved


def should_use_optimized_format(processed_results: List[Dict[str, Any]]) -> bool:
    """
    Detect if the data contains patient-diagnosis joins that would benefit from optimization.
    
    Args:
        processed_results: List of result dictionaries
        
    Returns:
        bool: True if optimization should be applied
    """
    if not processed_results or len(processed_results) < 2:
        return False
    
    # Check if we have both patient fields and diagnosis fields
    first_record = processed_results[0]
    
    # Patient-related fields that typically remain constant
    patient_fields = PATIENT_FIELDS
    # Diagnosis/varying fields
    varying_fields = VARYING_FIELDS
    
    # Check if we have both types of fields
    has_patient_fields = any(field in first_record for field in patient_fields)
    has_varying_fields = any(field in first_record for field in varying_fields)
    
    if not (has_patient_fields and has_varying_fields):
        return False
    
    # Check if patient data is repeated across multiple rows
    # Extract patient data from first two records
    patient_data_first = {}
    patient_data_second = {}
    
    for field in patient_fields:
        if field in first_record:
            patient_data_first[field] = first_record[field]
        if field in processed_results[1]:
            patient_data_second[field] = processed_results[1][field]
    
    # If patient data is identical between records, this indicates redundancy
    return patient_data_first == patient_data_second and len(patient_data_first) > 0


def handle_output(
    results_envelope: List[Any],
    output_file_path: Optional[str],
    query_display_name: str,
    effective_format: str,
    metadata_dict: Optional[Dict[str, Any]] = None,
    split_output: bool = False,
    filename_template: str = "{PatientID}",
    optimize_txt: bool = False
) -> None:
    """
    Format and output query results based on the specified format and destination.
    
    Args:
        results_envelope: Query results to format
        output_file_path: Path to save results to (None for stdout)
        query_display_name: Display name of the query for logging
        effective_format: Output format ('json', 'csv', 'tsv', 'stdout')
        metadata_dict: Optional metadata dictionary to include
        split_output: Whether to save each row as a separate file
        filename_template: Template for naming individual output files when split_output is True
        optimize_txt: Whether to use optimized TXT format that groups patient data
    """
    output_formatter = OutputFormatter()
    processed_results = process_match_candidates_for_tabular(results_envelope)
    
    try:
        if output_file_path:
            if split_output and processed_results:
                files_saved = handle_split_output(
                    output_file_path, processed_results, effective_format,
                    metadata_dict, filename_template, output_formatter, optimize_txt
                )
                output_dir = os.path.dirname(output_file_path) or os.getcwd()
                logger.info(f"Saved {files_saved} individual files for '{query_display_name}' in {output_dir}")
            else:
                write_output_to_file(
                    output_file_path, results_envelope, processed_results,
                    effective_format, metadata_dict, output_formatter, optimize_txt
                )
                logger.info(f"Saved results for '{query_display_name}' to {output_file_path}")
        else:
            write_output_to_stdout(
                results_envelope, processed_results, effective_format,
                metadata_dict, output_formatter, optimize_txt
            )
    
    except ValueError as e:
        logger.error(str(e))
        print(str(e), file=sys.stderr)
    except Exception as e:
        logger.error(f"Error during output handling: {e}")
        print(f"Error during output handling: {e}", file=sys.stderr)
