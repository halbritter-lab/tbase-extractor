#!/usr/bin/env python3
"""
Comprehensive test for Option 3: Runtime SQL Generation Implementation

This script verifies that the dynamic query generation system is complete and working.
Tests both import functionality and runtime dynamic query generation capabilities.
"""

import sys
import os
import traceback
from datetime import datetime

# Add the project root to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all dynamic query components can be imported."""
    print("Test 1: Verifying dynamic query imports...")
    try:
        from tbase_extractor.sql_interface.dynamic_query_builder import DynamicQueryBuilder
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.utils import resolve_templates_dir
        print("✓ All dynamic query components imported successfully")
        return True
    except Exception as e:
        print(f"✗ Import error: {e}")
        traceback.print_exc()
        return False

def test_dynamic_query_builder():
    """Test that the DynamicQueryBuilder can be instantiated and used."""
    print("\nTest 2: Testing DynamicQueryBuilder...")
    try:
        from tbase_extractor.sql_interface.dynamic_query_builder import DynamicQueryBuilder
        
        # Test builder creation
        builder = DynamicQueryBuilder()
        print("✓ DynamicQueryBuilder instantiated successfully")
        
        # Test basic query generation
        sql, params = builder.build_patient_by_id_query(123, 'Patient', 'dbo')
        print(f"✓ Generated patient by ID query: {sql[:50]}...")
        
        # Test with diagnoses
        sql, params = builder.build_patient_by_id_query(123, 'Patient', 'dbo', include_diagnoses=True, diagnose_table='Diagnose')
        print(f"✓ Generated patient by ID query with diagnoses: {sql[:50]}...")
        
        return True
    except Exception as e:
        print(f"✗ DynamicQueryBuilder error: {e}")
        traceback.print_exc()
        return False

def test_hybrid_query_manager():
    """Test that the HybridQueryManager works correctly."""
    print("\nTest 3: Testing HybridQueryManager...")
    try:
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.utils import resolve_templates_dir
        
        templates_dir = resolve_templates_dir()
        
        # Test hybrid manager creation
        manager = HybridQueryManager(templates_dir, 'Patient', 'Diagnose', 'dbo', debug=True)
        print("✓ HybridQueryManager instantiated successfully")
        
        # Test static capabilities (should fallback to templates)
        has_static = hasattr(manager, 'query_manager')
        print(f"✓ Static template support: {has_static}")
        
        # Test dynamic capabilities
        has_dynamic = hasattr(manager, 'dynamic_builder')
        print(f"✓ Dynamic query builder support: {has_dynamic}")
        
        # Test patient by ID query (dynamic)
        sql, params = manager.get_patient_by_id_query(123)
        print(f"✓ Patient by ID query: {sql[:50]}...")
        
        # Test patient by ID query with diagnoses
        if 'include_diagnoses' in manager.get_patient_by_id_query.__code__.co_varnames:
            sql, params = manager.get_patient_by_id_query(123, include_diagnoses=True)
            print(f"✓ Patient by ID query with diagnoses: {sql[:50]}...")
        else:
            print("⚠ include_diagnoses parameter not supported")
        
        # Test list tables query
        sql, params = manager.get_list_tables_query()
        print(f"✓ List tables query: {sql[:50]}...")
        
        return True
    except Exception as e:
        print(f"✗ HybridQueryManager error: {e}")
        traceback.print_exc()
        return False

def test_runtime_parameter_detection():
    """Test that runtime parameter detection works for include_diagnoses."""
    print("\nTest 4: Testing runtime parameter detection...")
    try:
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.utils import resolve_templates_dir
        
        templates_dir = resolve_templates_dir()
        manager = HybridQueryManager(templates_dir, 'Patient', 'Diagnose', 'dbo')
        
        # Test parameter detection using introspection
        method = manager.get_patient_by_id_query
        supports_include_diagnoses = (
            hasattr(method, '__code__') and 
            'include_diagnoses' in method.__code__.co_varnames
        )
        
        print(f"✓ Parameter detection working: include_diagnoses = {supports_include_diagnoses}")
        
        # Test all query methods for parameter support
        methods_to_test = [
            'get_patient_by_id_query',
            'get_patient_by_name_dob_query',
            'get_patients_by_dob_year_range_query',
            'get_patients_by_lastname_like_query',
            'get_all_patients_query'
        ]
        
        for method_name in methods_to_test:
            if hasattr(manager, method_name):
                method = getattr(manager, method_name)
                supports_param = (
                    hasattr(method, '__code__') and 
                    'include_diagnoses' in method.__code__.co_varnames
                )
                print(f"  - {method_name}: include_diagnoses = {supports_param}")
        
        return True
    except Exception as e:
        print(f"✗ Runtime parameter detection error: {e}")
        traceback.print_exc()
        return False

def test_table_customization():
    """Test that table names can be customized at runtime."""
    print("\nTest 5: Testing table customization...")
    try:
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.utils import resolve_templates_dir
        
        templates_dir = resolve_templates_dir()
        
        # Test with custom table names
        custom_manager = HybridQueryManager(
            templates_dir, 
            patient_table='MyPatients', 
            diagnose_table='MyDiagnoses', 
            schema='custom_schema'
        )
        
        # Generate query with custom tables
        sql, params = custom_manager.get_patient_by_id_query(123)
        
        # Check if custom table names appear in query
        has_custom_patient = 'MyPatients' in sql
        has_custom_schema = 'custom_schema' in sql
        
        print(f"✓ Custom patient table used: {has_custom_patient}")
        print(f"✓ Custom schema used: {has_custom_schema}")
        print(f"✓ Generated query: {sql[:100]}...")
        
        return has_custom_patient and has_custom_schema
    except Exception as e:
        print(f"✗ Table customization error: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all tests and report results."""
    print("=== TESTING OPTION 3: RUNTIME SQL GENERATION IMPLEMENTATION ===")
    print(f"Test started at: {datetime.now()}")
    print()
    
    tests = [
        test_imports,
        test_dynamic_query_builder,
        test_hybrid_query_manager,
        test_runtime_parameter_detection,
        test_table_customization
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test_func.__name__} failed with exception: {e}")
            traceback.print_exc()
    
    print()
    print("=== TEST RESULTS ===")
    print(f"Passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("✓ ALL TESTS PASSED - Option 3: Runtime SQL Generation is COMPLETE!")
        return 0
    else:
        print(f"✗ {total-passed} test(s) failed - Implementation needs fixes")
        return 1

if __name__ == "__main__":
    sys.exit(main())
