# TBase Extractor

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

A powerful Python CLI tool for extracting patient data from SQL Server databases with configurable SQL templates, fuzzy matching capabilities, and flexible output formats.

## 🚀 Features

- **🔒 Secure Configuration**: Environment-based credential management
- **📊 Multiple Output Formats**: JSON, CSV, TSV, TXT, and formatted console output
- **🔍 Fuzzy Matching**: Advanced patient search with configurable similarity thresholds
- **⚡ Batch Processing**: Process multiple patients from CSV input files
- **🎯 Dynamic Queries**: Runtime SQL generation with flexible table joins
- **🧹 Data Cleaning**: Automatic HTML tag removal and field normalization
- **📁 Split Output**: Individual files per patient for batch operations
- **🔧 Modular Architecture**: Clean separation of concerns for maintainability

## 📋 Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Architecture](#architecture)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Prerequisites

- **Python 3.8+**
- **Microsoft SQL Server database access**
- **ODBC Driver for SQL Server** (e.g., "ODBC Driver 17 for SQL Server")

## 🛠️ Installation

### Standard Installation

```bash
# Clone the repository
git clone https://github.com/halbritter-lab/tbase-extractor.git
cd tbase-extractor

# Create and activate virtual environment
python -m venv venv
# Windows:
.\venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install the package
pip install .
```

### Development Installation

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Set up pre-commit hooks
make pre-commit
```

## ⚙️ Configuration

1. Create a `.env` file in the project root:

```env
SQL_SERVER=<your_sql_server_name_or_ip>
DATABASE=<your_database_name>
USERNAME_SQL=<your_sql_username>
PASSWORD=<your_sql_password>
SQL_DRIVER="{ODBC Driver 17 for SQL Server}"
```

2. Verify database schema compatibility:
   - Primary patient table: `dbo.Patient`
   - Required columns: `PatientID`, `Vorname` (first name), `Name` (last name), `Geburtsdatum` (DOB)
   - Diagnosis table: `dbo.Diagnose` with `ICD10`, `Bezeichnung` columns

## 🎯 Usage

### Basic Commands

```bash
# Get help
tbase-extractor --help

# List available tables
tbase-extractor list-tables

# Discover patient tables
tbase-extractor discover-patient-tables
```

### Query Examples

#### Single Patient Query

```bash
# Query by Patient ID (console output)
tbase-extractor query --query-name patient-details --patient-id 12345

# Query by Patient ID (JSON output)
tbase-extractor query -q patient-details -i 12345 -o output/patient_12345.json

# Query by Patient ID (CSV output)
tbase-extractor query -q patient-details -i 12345 -f csv -o output/patient_12345.csv
```

#### Patient Search by Demographics

```bash
# Query by name and date of birth
tbase-extractor query -q patient-by-name-dob -fn John -ln Doe -d 1990-05-20

# With JSON output
tbase-extractor query -q patient-by-name-dob -fn Jane -ln Smith -d 1988-11-01 -o output/jane_smith.json
```

#### Batch Processing

```bash
# Batch query from CSV file
tbase-extractor query -q get_patient_by_id --input-csv patients.csv -o batch_results.json

# Batch with custom ID column
tbase-extractor query -q get_patient_by_id -ic patients.csv --id-column ID -o results.json

# Split output (one file per patient)
tbase-extractor query -q get_patient_by_id -ic patients.csv -o output/ --split-output

# Custom filename template
tbase-extractor query -q get_patient_by_id -ic patients.csv -o output/ --split-output --filename-template "{Vorname}_{Name}"
```

#### Custom Table Queries

```bash
# Query custom tables with flexible specifications
tbase-extractor query-custom-tables --table-specs "TableName:Schema:JoinColumn" -i 12345
```

### Output Formats

| Format | Description | Usage |
|--------|-------------|-------|
| `json` | JSON format with metadata | `-f json` |
| `csv` | Comma-separated values | `-f csv` |
| `tsv` | Tab-separated values | `-f tsv` |
| `txt` | Plain text (one value per line) | `-f txt` |
| `stdout` | Formatted console table (default) | `-f stdout` |

## 🏗️ Architecture

### Core Components

```
tbase_extractor/
├── main.py                    # CLI entry point and argument parsing
├── config.py                  # Configuration management
├── metadata.py                # Query metadata handling
├── output_handler.py          # Multi-format output processing
├── utils.py                   # Utility functions
├── sql_interface/             # Database interaction layer
│   ├── db_interface.py        # Core database connection (pyodbc)
│   ├── query_manager.py       # SQL template management
│   ├── dynamic_query_manager.py # Runtime query generation
│   ├── flexible_query_builder.py # Custom table query construction
│   └── output_formatter.py   # Result formatting
├── matching/                  # Fuzzy matching system
│   ├── fuzzy_matchers.py     # String similarity matching (rapidfuzz)
│   ├── search_strategy.py    # Patient search logic
│   └── models.py             # Data models
└── sql_templates/            # Parameterized SQL queries
    └── *.sql                 # Query templates
```

### Key Technologies

- **Database**: `pyodbc` for SQL Server connectivity
- **CLI**: `argparse` with subcommands
- **Fuzzy Matching**: `rapidfuzz` for similarity scoring
- **Output**: `tabulate` for console formatting
- **Configuration**: `python-dotenv` for environment management
- **Data Cleaning**: `beautifulsoup4` for HTML tag removal

## 🧪 Development

### Available Make Commands

```bash
make help          # Show all available commands
make install-dev   # Install with development dependencies
make format        # Format code with black and ruff
make lint          # Run all linting tools (ruff, black, flake8, mypy)
make type-check    # Run mypy type checking
make test          # Run tests with pytest
make test-cov      # Run tests with coverage
make pre-commit    # Install and run pre-commit hooks
make clean         # Clean up build artifacts
```

### Manual Development Commands

```bash
# Format code
black .
ruff --fix .

# Check code quality
ruff check .        # Style and import linting
black --check .     # Code formatting check  
flake8 .           # Additional style and docstring checks
mypy .             # Type checking (has known issues)

# Run tests
pytest
pytest --cov=tbase_extractor
```

### Adding New Queries

1. Create a `.sql` file in `sql_templates/` with parameterized queries (`?` placeholders)
2. Add a method in `sql_interface/query_manager.py`
3. Update command-line arguments in `main.py`
4. Add handling logic for the new query type

## 🔐 Security Features

- **Parameterized Queries**: All SQL uses `?` placeholders to prevent injection
- **Environment Variables**: Sensitive credentials stored in `.env` files
- **Input Validation**: Date format validation and parameter checking
- **Error Handling**: Comprehensive SQLSTATE error reporting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following the code style guidelines
4. Run tests and linting (`make lint test`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Developed by the Halbritter Lab team
- Built with modern Python best practices
- Designed for clinical research data extraction workflows

---

For questions, issues, or contributions, please visit our [GitHub repository](https://github.com/halbritter-lab/tbase-extractor).