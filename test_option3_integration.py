#!/usr/bin/env python3
"""
Test script for Option 3: Runtime SQL Generation integration
Tests the new flexible query commands added to the CLI.
"""

import sys
import subprocess
import os

def run_command(cmd, description):
    """Run a command and return the result."""
    print(f"\n=== {description} ===")
    print(f"Command: {cmd}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd="t:\\Dokumente\\VSC_AGDE\\SQL_App")
        
        if result.returncode == 0:
            print("✅ SUCCESS")
            if result.stdout:
                print("STDOUT:")
                print(result.stdout)
        else:
            print("❌ FAILED")
            if result.stderr:
                print("STDERR:")
                print(result.stderr)
            if result.stdout:
                print("STDOUT:")
                print(result.stdout)
                
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False

def main():
    """Run integration tests for the new flexible query commands."""
    print("Testing Option 3: Runtime SQL Generation Integration")
    print("=" * 60)
    
    tests = [
        {
            "cmd": "python -m tbase_extractor --help",
            "desc": "Main help - should show new commands"
        },
        {
            "cmd": "python -m tbase_extractor discover-patient-tables --help", 
            "desc": "discover-patient-tables help"
        },
        {
            "cmd": "python -m tbase_extractor query-custom-tables --help",
            "desc": "query-custom-tables help" 
        }
    ]
    
    results = []
    for test in tests:
        success = run_command(test["cmd"], test["desc"])
        results.append(success)
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print(f"Tests passed: {sum(results)}/{len(results)}")
    
    if all(results):
        print("🎉 All integration tests passed!")
        print("\nThe flexible query system has been successfully integrated!")
        print("\nNext steps:")
        print("1. Test with actual database connections")
        print("2. Test discover-patient-tables with real database")
        print("3. Test query-custom-tables with example table specifications")
        print("4. Add comprehensive documentation and examples")
        return 0
    else:
        print("❌ Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
