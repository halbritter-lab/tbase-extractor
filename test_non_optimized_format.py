#!/usr/bin/env python3
"""
Test script to verify the non-optimized format fixes work correctly
for multi-patient data in both TXT and JSON formats.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'tbase_extractor'))

from tbase_extractor.sql_interface.output_formatter import OutputFormatter

def test_multi_patient_non_optimized_txt():
    """Test the non-optimized TXT format with multiple patients"""
    print("=== Testing Multi-Patient Non-Optimized TXT Format ===")
    
    # Mock data representing multiple patients with diagnoses (typical join result)
    mock_data = [
        {'Name': 'Smith', 'Vorname': 'John', 'ICD10': 'I10', 'Bezeichnung': 'Hypertension'},
        {'Name': 'Smith', 'Vorname': 'John', 'ICD10': 'E11', 'Bezeichnung': 'Diabetes'},
        {'Name': 'Smith', 'Vorname': 'John', 'ICD10': 'K21', 'Bezeichnung': 'GERD'},
        {'Name': 'Mueller', 'Vorname': 'Anna', 'ICD10': 'J44', 'Bezeichnung': 'COPD'},
        {'Name': 'Mueller', 'Vorname': 'Anna', 'ICD10': 'I25', 'Bezeichnung': 'CAD'},
        {'Name': 'Brown', 'Vorname': 'Bob', 'ICD10': 'N18', 'Bezeichnung': 'CKD'},
    ]
    
    result = OutputFormatter.format_as_txt(mock_data)
    print("Result:")
    print(result)
    print()
    
    # Check if patient names appear multiple times (should be grouped)
    lines = result.split('\n')
    smith_name_count = sum(1 for line in lines if line.strip() == 'Smith')
    john_name_count = sum(1 for line in lines if line.strip() == 'John')
    
    print(f"Occurrences of 'Smith': {smith_name_count}")
    print(f"Occurrences of 'John': {john_name_count}")
    print(f"Should see patient separators: {'===' in result}")
    print()

def test_multi_patient_non_optimized_json():
    """Test the non-optimized JSON format with multiple patients"""
    print("=== Testing Multi-Patient Non-Optimized JSON Format ===")
    
    # Mock data representing multiple patients with diagnoses
    mock_data = [
        {'Name': 'Smith', 'Vorname': 'John', 'ICD10': 'I10', 'Bezeichnung': 'Hypertension'},
        {'Name': 'Smith', 'Vorname': 'John', 'ICD10': 'E11', 'Bezeichnung': 'Diabetes'},
        {'Name': 'Mueller', 'Vorname': 'Anna', 'ICD10': 'J44', 'Bezeichnung': 'COPD'},
        {'Name': 'Mueller', 'Vorname': 'Anna', 'ICD10': 'I25', 'Bezeichnung': 'CAD'},
    ]
    
    result = OutputFormatter.format_as_json(mock_data)
    print("Result:")
    print(result)
    print()
    
    # Check if the result shows patient grouping
    print(f"Contains patient grouping: {'patient_info' in result}")
    print(f"Contains records separation: {'records' in result}")
    print()

def test_single_patient_compatibility():
    """Test that single patient data still works as expected"""
    print("=== Testing Single Patient Compatibility ===")
    
    # Single patient data
    single_patient_data = [
        {'Name': 'Doe', 'Vorname': 'Jane', 'ICD10': 'I10', 'Bezeichnung': 'Hypertension'},
        {'Name': 'Doe', 'Vorname': 'Jane', 'ICD10': 'E11', 'Bezeichnung': 'Diabetes'},
    ]
    
    txt_result = OutputFormatter.format_as_txt(single_patient_data)
    json_result = OutputFormatter.format_as_json(single_patient_data)
    
    print("TXT Result:")
    print(txt_result)
    print()
    
    print("JSON Result:")
    print(json_result[:200] + "..." if len(json_result) > 200 else json_result)
    print()

def test_no_patient_fields():
    """Test data without patient fields (should work normally)"""
    print("=== Testing Data Without Patient Fields ===")
    
    # Data without patient fields
    no_patient_data = [
        {'Code': 'ABC123', 'Description': 'Test Item 1', 'Value': 100},
        {'Code': 'DEF456', 'Description': 'Test Item 2', 'Value': 200},
    ]
    
    txt_result = OutputFormatter.format_as_txt(no_patient_data)
    json_result = OutputFormatter.format_as_json(no_patient_data)
    
    print("TXT Result:")
    print(txt_result)
    print()
    
    print("JSON Result:")
    print(json_result[:200] + "..." if len(json_result) > 200 else json_result)
    print()

if __name__ == "__main__":
    test_multi_patient_non_optimized_txt()
    test_multi_patient_non_optimized_json()
    test_single_patient_compatibility()
    test_no_patient_fields()
    
    print("=== Test Complete ===")
    print("Check the results above to verify that:")
    print("1. Multi-patient TXT format groups patients with separators")
    print("2. Multi-patient JSON format groups patients properly")
    print("3. Single-patient data still works correctly") 
    print("4. Non-patient data works normally")
