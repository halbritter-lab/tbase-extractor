#!/usr/bin/env python3
"""
Test edge cases for the non-optimized format fixes
"""

import sys
sys.path.append('tbase_extractor')

from tbase_extractor.sql_interface.output_formatter import OutputFormatter

def test_edge_cases():
    formatter = OutputFormatter()
    
    print("=== Testing Edge Case: Mixed Complete/Incomplete Patient Data ===")
    
    # Test data with some patients having missing names
    mixed_data = {
        "metadata": {"columns": ["Name", "Vorname", "ICD10", "Bezeichnung"]},
        "data": [
            {"Name": "Smith", "Vorname": "John", "ICD10": "I10", "Bezeichnung": "Hypertension"},
            {"Name": "", "Vorname": "", "ICD10": "E11", "Bezeichnung": "Diabetes"},  # Missing patient info
            {"Name": "Mueller", "Vorname": "", "ICD10": "J44", "Bezeichnung": "COPD"},  # Partial patient info
            {"Name": "", "Vorname": "Anna", "ICD10": "I25", "Bezeichnung": "CAD"},  # Partial patient info
        ]
    }
    
    txt_result = formatter.format_as_txt(mixed_data)
    print("TXT Result:")
    print(txt_result)
    print()
    
    json_result = formatter.format_as_json(mixed_data)
    print("JSON Result:")
    print(json_result[:500] + "..." if len(json_result) > 500 else json_result)
    print()
    
    print("=== Testing Edge Case: Empty Data ===")
    
    empty_data = {
        "metadata": {"columns": []},
        "data": []
    }
    
    txt_empty = formatter.format_as_txt(empty_data)
    print("Empty TXT Result:")
    print(repr(txt_empty))
    print()
    
    json_empty = formatter.format_as_json(empty_data)
    print("Empty JSON Result:")
    print(json_empty)
    print()
    
    print("=== Testing Edge Case: Single Record ===")
    
    single_data = {
        "metadata": {"columns": ["Name", "Vorname", "ICD10"]},
        "data": [
            {"Name": "Test", "Vorname": "Patient", "ICD10": "I10"}
        ]
    }
    
    txt_single = formatter.format_as_txt(single_data)
    print("Single Record TXT:")
    print(txt_single)
    print()
    
    json_single = formatter.format_as_json(single_data)
    print("Single Record JSON:")
    print(json_single)
    print()

if __name__ == "__main__":
    test_edge_cases()
