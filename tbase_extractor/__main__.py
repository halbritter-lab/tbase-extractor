# tbase_extractor/__main__.py

"""
This file allows the tbase_extractor package to be executed as a script
using `python -m tbase_extractor`.
"""

import sys
from .main import main # Use a relative import to get the main function

if __name__ == "__main__":
    # Optionally, you could add sys.exit(main()) if your main function returns an exit code.
    # For now, just calling main() is fine.
    main()