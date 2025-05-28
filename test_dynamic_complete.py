#!/usr/bin/env python3
"""
Test script to verify the complete implementation of Option 3: Runtime SQL Generation.
This tests the dynamic query generation system with configurable table names and parameters.
"""

import sys
import os
import subprocess
import json
from pathlib import Path

# Add the project root to the Python path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def run_command(cmd, expect_failure=False):
    """Run a command and return the result."""
    print(f"\n🔧 Running: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            cwd=project_root,
            timeout=30  # 30 second timeout
        )
        
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        print(f"Return code: {result.returncode}")
        
        if not expect_failure and result.returncode != 0:
            print(f"❌ Command failed with return code {result.returncode}")
            return None
        elif expect_failure and result.returncode == 0:
            print(f"❌ Expected failure but command succeeded")
            return None
        else:
            print("✅ Command completed as expected")
            return result
            
    except subprocess.TimeoutExpired:
        print("❌ Command timed out after 30 seconds")
        return None
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return None

def test_help_commands():
    """Test that help commands work and show dynamic options."""
    print("\n" + "="*80)
    print("🧪 TEST 1: Help Commands - Verify Dynamic Options Available")
    print("="*80)
    
    # Test main help
    result1 = run_command([sys.executable, "-m", "tbase_extractor", "--help"])
    
    # Test list-tables help
    result2 = run_command([sys.executable, "-m", "tbase_extractor", "list-tables", "--help"])
    
    # Test query help
    result3 = run_command([sys.executable, "-m", "tbase_extractor", "query", "--help"])
    
    if result1 and result2 and result3:
        # Check if dynamic options are present in query help
        if "--use-dynamic-builder" in result3.stdout and "--patient-table" in result3.stdout:
            print("✅ Dynamic query options are available in help text")
            return True
        else:
            print("❌ Dynamic query options not found in help text")
            return False
    return False

def test_syntax_validation():
    """Test that the Python syntax is valid by attempting imports."""
    print("\n" + "="*80)
    print("🧪 TEST 2: Syntax Validation - Import Tests")
    print("="*80)
    
    try:
        # Test importing main components
        from tbase_extractor.main import main, setup_arg_parser
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.sql_interface.dynamic_query_builder import DynamicQueryBuilder
        
        print("✅ All imports successful - syntax is valid")
        
        # Test argument parser setup
        parser = setup_arg_parser()
        print("✅ Argument parser setup successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Import/syntax error: {e}")
        return False

def test_dynamic_query_generation():
    """Test the dynamic query generation without database connection."""
    print("\n" + "="*80)
    print("🧪 TEST 3: Dynamic Query Generation - SQL Generation Test")
    print("="*80)
    
    try:
        from tbase_extractor.sql_interface.dynamic_query_builder import DynamicQueryBuilder
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.utils import resolve_templates_dir
        
        # Test custom table configuration
        custom_patient_table = "CustomPatient"
        custom_diagnose_table = "CustomDiagnosis"
        custom_schema = "healthcare"
        
        # Create dynamic query builder
        builder = DynamicQueryBuilder(custom_patient_table, custom_diagnose_table, custom_schema)
        
        # Test different query generation methods
        tests = [
            {
                "name": "get_patient_by_id_query with diagnoses",
                "method": "get_patient_by_id_query",
                "args": [123],
                "kwargs": {"include_diagnoses": True}
            },
            {
                "name": "get_patient_by_id_query without diagnoses", 
                "method": "get_patient_by_id_query",
                "args": [456],
                "kwargs": {"include_diagnoses": False}
            },
            {
                "name": "get_all_patients_query with diagnoses",
                "method": "get_all_patients_query", 
                "args": [],
                "kwargs": {"include_diagnoses": True}
            }
        ]
        
        all_passed = True
        
        for test in tests:
            print(f"\n🔍 Testing: {test['name']}")
            try:
                method = getattr(builder, test['method'])
                sql, params = method(*test['args'], **test['kwargs'])
                
                print(f"Generated SQL: {sql[:100]}...")
                print(f"Parameters: {params}")
                
                # Verify custom table names are used
                if custom_patient_table in sql and custom_schema in sql:
                    print(f"✅ Custom table names correctly used in SQL")
                else:
                    print(f"❌ Custom table names not found in SQL")
                    all_passed = False
                    
                # Verify diagnoses join when requested
                if test['kwargs'].get('include_diagnoses'):
                    if custom_diagnose_table in sql and "LEFT JOIN" in sql:
                        print(f"✅ Diagnoses join correctly included")
                    else:
                        print(f"❌ Diagnoses join not found when requested")
                        all_passed = False
                        
            except Exception as e:
                print(f"❌ Error in {test['name']}: {e}")
                all_passed = False
        
        if all_passed:
            print("\n✅ All dynamic query generation tests passed")
            return True
        else:
            print("\n❌ Some dynamic query generation tests failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing dynamic query generation: {e}")
        return False

def test_hybrid_manager():
    """Test the HybridQueryManager functionality."""
    print("\n" + "="*80)
    print("🧪 TEST 4: HybridQueryManager - Fallback Test")
    print("="*80)
    
    try:
        from tbase_extractor.sql_interface.dynamic_query_manager import HybridQueryManager
        from tbase_extractor.utils import resolve_templates_dir
        
        templates_dir = resolve_templates_dir()
        
        # Test with custom configuration
        hybrid_manager = HybridQueryManager(
            templates_dir=templates_dir,
            patient_table="TestPatient",
            diagnose_table="TestDiagnosis", 
            schema="test_schema",
            debug=True
        )
        
        # Test methods that should use dynamic builder
        print("🔍 Testing dynamic methods...")
        try:
            sql, params = hybrid_manager.get_patient_by_id_query(123, include_diagnoses=True)
            if "TestPatient" in sql and "test_schema" in sql:
                print("✅ Dynamic query with custom tables working")
            else:
                print("❌ Dynamic query not using custom tables")
                return False
        except Exception as e:
            print(f"❌ Error with dynamic method: {e}")
            return False
            
        # Test fallback to template methods
        print("🔍 Testing template fallback methods...")
        try:
            sql, params = hybrid_manager.get_table_columns_query("TestTable", "test_schema")
            if sql and "INFORMATION_SCHEMA" in sql:
                print("✅ Template fallback working")
            else:
                print("❌ Template fallback not working")
                return False
        except Exception as e:
            print(f"❌ Error with template fallback: {e}")
            return False
            
        print("✅ HybridQueryManager tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Error testing HybridQueryManager: {e}")
        return False

def test_argument_parsing():
    """Test that argument parsing works with dynamic options."""
    print("\n" + "="*80)
    print("🧪 TEST 5: Argument Parsing - Dynamic Options Test")
    print("="*80)
    
    try:
        from tbase_extractor.main import setup_arg_parser
        
        parser = setup_arg_parser()
        
        # Test parsing dynamic arguments
        test_args = [
            "query",
            "--query-name", "get_patient_by_id",
            "--patient-id", "123",
            "--use-dynamic-builder",
            "--patient-table", "CustomPatient", 
            "--diagnose-table", "CustomDiagnosis",
            "--schema", "custom_schema",
            "--include-diagnoses",
            "--format", "json"
        ]
        
        args = parser.parse_args(test_args)
        
        # Verify all dynamic options are parsed correctly
        checks = [
            (args.use_dynamic_builder, True, "use_dynamic_builder"),
            (args.patient_table, "CustomPatient", "patient_table"),
            (args.diagnose_table, "CustomDiagnosis", "diagnose_table"),
            (args.schema, "custom_schema", "schema"),
            (args.include_diagnoses, True, "include_diagnoses"),
            (args.query_name, "get_patient_by_id", "query_name"),
            (args.patient_id, 123, "patient_id")
        ]
        
        all_passed = True
        for actual, expected, name in checks:
            if actual == expected:
                print(f"✅ {name}: {actual}")
            else:
                print(f"❌ {name}: expected {expected}, got {actual}")
                all_passed = False
                
        if all_passed:
            print("✅ All argument parsing tests passed")
            return True
        else:
            print("❌ Some argument parsing tests failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing argument parsing: {e}")
        return False

def main():
    """Run all tests for the dynamic query system."""
    print("🚀 TESTING OPTION 3: Runtime SQL Generation Implementation")
    print("=" * 80)
    print("This test suite verifies the complete dynamic query generation system")
    print("=" * 80)
    
    tests = [
        ("Help Commands", test_help_commands),
        ("Syntax Validation", test_syntax_validation), 
        ("Dynamic Query Generation", test_dynamic_query_generation),
        ("HybridQueryManager", test_hybrid_manager),
        ("Argument Parsing", test_argument_parsing)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*80)
    print("📋 TEST SUMMARY")
    print("="*80)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
        if result:
            passed += 1
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! Option 3: Runtime SQL Generation is fully implemented!")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
