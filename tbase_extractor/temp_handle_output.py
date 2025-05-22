def handle_output(results_envelope, output_file_path, query_display_name, effective_format, metadata_dict=None):
    """
    Format and output query results based on the specified format and destination.
    
    Args:
        results_envelope: Query results to format
        output_file_path: Path to save results to (None for stdout)
        query_display_name: Display name of the query for logging
        effective_format: Output format ('json', 'csv', 'tsv', 'stdout')
        metadata_dict: Optional metadata dictionary to include
    """
    logger = logging.getLogger("main")
    output_formatter = OutputFormatter()
    
    # Process metadata for non-JSON formats (as comments)
    metadata_summary = ''
    if metadata_dict:
        metadata_lines = [f"# {k}: {v}" for k, v in metadata_dict.items()]
        metadata_summary = '\n'.join(metadata_lines)
    
    # Check if we need to process MatchCandidate objects for CSV/TSV formats
    processed_results_for_tabular = results_envelope
    if isinstance(results_envelope, list) and results_envelope and hasattr(results_envelope[0], 'match_fields_info') and hasattr(results_envelope[0], 'overall_score'):
        processed_results_for_tabular = []
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
                    
            processed_results_for_tabular.append(result)
    
    # Output to file if specified
    if output_file_path:
        with open(output_file_path, 'w', encoding='utf-8', newline='') as f:
            if effective_format == 'json':
                f.write(output_formatter.format_as_json(results_envelope, metadata_dict))
            elif effective_format == 'csv':
                if metadata_summary:
                    f.write(metadata_summary + '\n')
                f.write(output_formatter.format_as_csv(processed_results_for_tabular))
            elif effective_format == 'tsv':
                if metadata_summary:
                    f.write(metadata_summary + '\n')
                f.write(output_formatter.format_as_tsv(processed_results_for_tabular))
            elif effective_format == 'stdout':
                if metadata_summary:
                    f.write(metadata_summary + '\n')
                import io
                buf = io.StringIO()
                output_formatter.format_as_console_table(results_envelope, stream=buf)
                f.write(buf.getvalue())
            else:
                logger.error(f"Unknown output format: {effective_format}")
                print(f"Unknown output format: {effective_format}", file=sys.stderr)
                return
        logger.info(f"Saved results for '{query_display_name}' to {output_file_path}")
    # Output to stdout
    else:
        if effective_format == 'json':
            print(output_formatter.format_as_json(results_envelope, metadata_dict))
        elif effective_format == 'csv':
            if metadata_summary:
                print(metadata_summary)
            print(output_formatter.format_as_csv(processed_results_for_tabular))
        elif effective_format == 'tsv':
            if metadata_summary:
                print(metadata_summary)
            print(output_formatter.format_as_tsv(processed_results_for_tabular))
        elif effective_format == 'stdout':
            if metadata_summary:
                print(metadata_summary)
            output_formatter.format_as_console_table(results_envelope, stream=sys.stdout)
        else:
            logger.error(f"Unknown output format: {effective_format}")
            print(f"Unknown output format: {effective_format}", file=sys.stderr)
            return
