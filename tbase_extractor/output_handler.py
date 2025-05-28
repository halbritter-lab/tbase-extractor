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
    
    with open(file_path, 'w', encoding=DEFAULT_FILE_ENCODING, newline='') as f:
        if effective_format == 'json':
            f.write(output_formatter.format_as_json(results_envelope, metadata_dict))
        elif effective_format == 'csv':
            if metadata_summary:
                f.write(metadata_summary + '\n')
            f.write(output_formatter.format_as_csv(processed_results))
        elif effective_format == 'tsv':
            if metadata_summary:
                f.write(metadata_summary + '\n')
            f.write(output_formatter.format_as_tsv(processed_results))
        elif effective_format == 'txt':
            # For txt format, no metadata or headers - use optimized format if requested
            if optimize_txt:
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
    
    if effective_format == 'json':
        print(output_formatter.format_as_json(results_envelope, metadata_dict))
    elif effective_format == 'csv':
        if metadata_summary:
            print(metadata_summary)
        print(output_formatter.format_as_csv(processed_results))
    elif effective_format == 'tsv':
        if metadata_summary:
            print(metadata_summary)
        print(output_formatter.format_as_tsv(processed_results))
    elif effective_format == 'txt':
        # For txt format, no metadata or headers - use optimized format if requested
        if optimize_txt:
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


def handle_split_output(
    output_file_path: str,
    processed_results: List[Dict[str, Any]],
    effective_format: str,
    metadata_dict: Optional[Dict[str, Any]],
    filename_template: str,
    output_formatter: OutputFormatter,
    optimize_txt: bool = False
) -> int:
    """Handle split output - save each row as a separate file."""
    if not processed_results:
        return 0
    
    output_dir = os.path.dirname(output_file_path) or os.getcwd()
    _, file_ext = os.path.splitext(output_file_path)
    if not file_ext:
        file_ext = '.' + effective_format if effective_format != 'stdout' else '.txt'
    
    files_saved = 0
    for i, row_data in enumerate(processed_results, 1):
        try:
            row_filename = generate_split_filename(row_data, filename_template, i)
            row_file_path = os.path.join(output_dir, f"{row_filename}{file_ext}")
            
            # Prepare single-row metadata
            row_metadata = metadata_dict.copy() if metadata_dict else {}
            row_metadata['row_count_fetched'] = 1
            
            write_output_to_file(
                row_file_path, [row_data], [row_data], 
                effective_format, row_metadata, output_formatter, optimize_txt
            )
            
            files_saved += 1
            logger.debug(f"Saved row data to {row_file_path}")
            
        except Exception as e:
            logger.error(f"Error saving split output file for row {i}: {e}")
    
    return files_saved


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
