"""Utility functions for tbase-extractor"""
import os
import sys
import importlib.resources as resources
from pathlib import Path
from typing import Optional

def resolve_templates_dir() -> str:
    """
    Resolves the path to the SQL templates directory robustly.
    Handles both development and installed package scenarios.
    
    Returns:
        str: Absolute path to the sql_templates directory
        
    Raises:
        RuntimeError: If the sql_templates directory cannot be found
    """
    print("[DEBUG utils] Attempting to resolve templates directory...", file=sys.stderr)
    
    # Strategy 1: Try importlib.resources (works for installed package)
    try:
        print("[DEBUG utils] Strategy 1: Using importlib.resources...", file=sys.stderr)
        templates = resources.files('tbase_extractor.sql_templates')
        if templates and hasattr(templates, 'is_dir') and templates.is_dir():
            # Convert to string path that can be used with os.path functions
            templates_str = str(templates)
            # Verify the path actually exists and is a directory
            if os.path.isdir(templates_str):
                print(f"[DEBUG utils] Found templates via resources: {templates_str}", file=sys.stderr)
                return templates_str
            else:
                print(f"[DEBUG utils] Resources path exists but is not a directory: {templates_str}", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG utils] resources.files() failed: {e}", file=sys.stderr)

    # Strategy 2: Try relative to this file (development mode)
    try:
        print("[DEBUG utils] Strategy 2: Checking relative to utils.py...", file=sys.stderr)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        dev_path = os.path.join(current_dir, "sql_templates")
        if os.path.isdir(dev_path):
            print(f"[DEBUG utils] Found templates dir: {dev_path}", file=sys.stderr)
            return dev_path
        else:
            print(f"[DEBUG utils] Development path not found: {dev_path}", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG utils] Development path check failed: {e}", file=sys.stderr)

    # Strategy 3: Try relative to project root (if running from repo root)
    try:
        print("[DEBUG utils] Strategy 3: Checking project root...", file=sys.stderr)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        root_path = os.path.join(project_root, "sql_templates")
        if os.path.isdir(root_path):
            print(f"[DEBUG utils] Found templates in project root: {root_path}", file=sys.stderr)
            return root_path
        else:
            print(f"[DEBUG utils] Project root path not found: {root_path}", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG utils] Project root check failed: {e}", file=sys.stderr)

    error_msg = "Could not locate sql_templates directory after trying:\n"
    error_msg += "1. Package resources (installed package)\n"
    error_msg += "2. Development path (relative to utils.py)\n"
    error_msg += "3. Project root path\n\n"
    error_msg += "Please ensure sql_templates directory exists and is readable."
    raise RuntimeError(error_msg)
