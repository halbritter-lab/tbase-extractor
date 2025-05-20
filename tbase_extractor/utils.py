import os
import sys
import importlib.util
import importlib

# Utility to resolve the sql_templates directory robustly

def resolve_templates_dir():
    """
    Returns the absolute path to the sql_templates directory within the installed package.
    Falls back to development path if running from source.
    """
    try:
        import importlib.resources as resources
        if hasattr(resources, "files"):
            # Always convert to string path
            return str(resources.files("tbase_extractor.sql_templates"))
        else:
            with resources.path("tbase_extractor.sql_templates", "") as p:
                return str(p)
    except Exception:
        # Fallback: try relative to this file (development mode)
        here = os.path.dirname(os.path.abspath(__file__))
        dev_path = os.path.join(here, "sql_templates")
        if os.path.isdir(dev_path):
            return dev_path
        up_path = os.path.abspath(os.path.join(here, "..", "sql_templates"))
        if os.path.isdir(up_path):
            return up_path
        raise RuntimeError("Could not locate sql_templates directory.")
