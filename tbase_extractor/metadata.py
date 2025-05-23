"""Metadata generation utilities for tbase_extractor."""
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from .config import APP_VERSION, METADATA_PARAM_KEYS, STATUS_SUCCESS, STATUS_SUCCESS_NO_DATA
from .config import STATUS_BATCH_SUCCESS_ALL, STATUS_BATCH_PARTIAL_SUCCESS, STATUS_BATCH_NO_DATA, STATUS_BATCH_INPUT_ERROR

logger = logging.getLogger(__name__)


def create_base_metadata(
    query_start_time: datetime,
    execution_duration_ms: int,
    args: Any,
    query_display_name: str,
    results_count: int
) -> Dict[str, Any]:
    """Create base metadata dictionary for all queries."""
    return {
        'query_timestamp_utc': query_start_time.isoformat(),
        'query_name': args.query_name if hasattr(args, 'query_name') and args.action == 'query' else args.action,
        'query_display_name': query_display_name,
        'tool_version': APP_VERSION,
        'execution_duration_ms': execution_duration_ms,
        'row_count_fetched': results_count,
    }


def extract_query_parameters(args: Any) -> Dict[str, str]:
    """Extract relevant query parameters from args for metadata."""
    return {
        k: str(v) for k, v in vars(args).items() 
        if k in METADATA_PARAM_KEYS and v is not None
    }


def determine_query_status(args: Any, results: Any) -> str:
    """Determine the appropriate status based on results and batch info."""
    # Check for batch processing
    if hasattr(args, 'batch_info') and args.batch_info:
        batch_info = args.batch_info
        
        if results:
            if 'ids_processed_successfully' in batch_info and 'total_ids_in_csv' in batch_info:
                if batch_info['ids_processed_successfully'] == batch_info['total_ids_in_csv']:
                    return STATUS_BATCH_SUCCESS_ALL
                elif batch_info['ids_processed_successfully'] > 0:
                    return STATUS_BATCH_PARTIAL_SUCCESS
                else:
                    return STATUS_BATCH_NO_DATA
            else:
                # For other batch types (like demographics search)
                return STATUS_BATCH_SUCCESS_ALL if results else STATUS_BATCH_NO_DATA
        elif batch_info.get('total_ids_in_csv', 0) > 0:
            return STATUS_BATCH_NO_DATA
        else:
            return STATUS_BATCH_INPUT_ERROR
    
    # Single query status
    return STATUS_SUCCESS if results else STATUS_SUCCESS_NO_DATA


def create_metadata_dict(
    query_start_time: datetime,
    execution_duration_ms: int,
    args: Any,
    query_display_name: str,
    results: Any
) -> Dict[str, Any]:
    """Create complete metadata dictionary for query results."""
    results_count = len(results) if results else 0
    
    metadata_dict = create_base_metadata(
        query_start_time, execution_duration_ms, args, 
        query_display_name, results_count
    )
    
    # Add query parameters
    metadata_dict['parameters'] = extract_query_parameters(args)
    
    # Add batch-specific metadata if available
    if hasattr(args, 'batch_info') and args.batch_info:
        metadata_dict['batch_processing_summary'] = args.batch_info
    
    # Determine and set status
    metadata_dict['status'] = determine_query_status(args, results)
    
    return metadata_dict
