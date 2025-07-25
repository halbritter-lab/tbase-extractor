"""Shared pytest configuration and fixtures for tbase-extractor tests."""

import csv
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, Mock

import pytest

from tbase_extractor.matching import FuzzyMatcher
from tbase_extractor.matching.models import MatchCandidate, MatchInfo
from tbase_extractor.sql_interface import SQLInterface
from tbase_extractor.sql_interface.query_manager import QueryManager


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_patient_data():
    """Sample patient data for testing."""
    return [
        {
            "PatientID": 1001,
            "Name": "Müller",
            "Vorname": "Hans",
            "Geburtsdatum": date(1980, 5, 15),
            "Address": "Hauptstraße 123",
        },
        {
            "PatientID": 1002,
            "Name": "Schmidt",
            "Vorname": "Anna",
            "Geburtsdatum": date(1975, 12, 3),
            "Address": "Nebenstraße 45",
        },
        {
            "PatientID": 1003,
            "Name": "Johnson",
            "Vorname": "John",
            "Geburtsdatum": date(1990, 8, 20),
            "Address": "Oak Street 789",
        },
    ]


@pytest.fixture
def sample_diagnosis_data():
    """Sample diagnosis data for testing."""
    return [
        {
            "PatientID": 1001,
            "ICD10": "E11.9",
            "Bezeichnung": "Diabetes mellitus, Type 2",
            "Date": date(2023, 1, 15),
        },
        {
            "PatientID": 1001,
            "ICD10": "I10",
            "Bezeichnung": "Essential hypertension",
            "Date": date(2023, 2, 20),
        },
        {
            "PatientID": 1002,
            "ICD10": "M79.1",
            "Bezeichnung": "Myalgia",
            "Date": date(2023, 3, 10),
        },
    ]


@pytest.fixture
def sample_csv_data(temp_dir):
    """Create sample CSV files for testing."""
    # Patient IDs CSV
    patient_ids_csv = temp_dir / "patient_ids.csv"
    with open(patient_ids_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["PatientID"])
        writer.writerows([["1001"], ["1002"], ["1003"], ["invalid"], [""]])

    # Demographics CSV
    demographics_csv = temp_dir / "demographics.csv"
    with open(demographics_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["FirstName", "LastName", "DOB"])
        writer.writerows(
            [
                ["Hans", "Müller", "1980-05-15"],
                ["Anna", "Schmidt", "1975-12-03"],
                ["John", "Johnson", "1990-08-20"],
                ["", "Invalid", "1985-01-01"],  # Missing first name
                ["Jane", "", "1990-01-01"],  # Missing last name
                ["Bob", "Test", "invalid-date"],  # Invalid date
            ],
        )

    return {
        "patient_ids": patient_ids_csv,
        "demographics": demographics_csv,
    }


@pytest.fixture
def mock_sql_interface():
    """Mock SQLInterface for testing without database connection."""
    mock = Mock(spec=SQLInterface)
    mock.connect.return_value = True
    mock.connection = MagicMock()
    mock.cursor = MagicMock()
    mock.execute_query.return_value = True
    mock.fetch_results.return_value = []
    mock.commit.return_value = True
    mock.close_connection.return_value = None
    return mock


@pytest.fixture
def mock_query_manager():
    """Mock QueryManager for testing without SQL templates."""
    mock = Mock(spec=QueryManager)
    mock.load_query_template.return_value = "SELECT * FROM test"
    mock.get_list_tables_query.return_value = ("SELECT * FROM tables", ())
    mock.get_patient_by_id_query.return_value = ("SELECT * FROM patients WHERE id = ?", (1,))
    mock.get_patient_by_name_dob_query.return_value = (
        "SELECT * FROM patients WHERE name = ? AND dob = ?",
        ("Test", date(1980, 1, 1)),
    )
    return mock


@pytest.fixture
def fuzzy_matcher():
    """Create a FuzzyMatcher instance for testing."""
    return FuzzyMatcher(string_similarity_threshold=0.85, date_year_tolerance=1)


@pytest.fixture
def sample_match_info():
    """Sample MatchInfo objects for testing."""
    return [
        MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
        MatchInfo("LastName", "Müller", "Mueller", "Fuzzy", 0.9),
        MatchInfo("DOB", date(1980, 5, 15), date(1980, 5, 15), "Exact", 1.0),
    ]


@pytest.fixture
def sample_match_candidate(sample_patient_data, sample_match_info):
    """Sample MatchCandidate for testing."""
    candidate = MatchCandidate(
        db_record=sample_patient_data[0],
        match_fields_info=sample_match_info,
    )
    return candidate


@pytest.fixture
def mock_db_results():
    """Mock database results for different query types."""
    return {
        "patient_by_id": [
            {
                "PatientID": 1001,
                "Name": "Müller",
                "Vorname": "Hans",
                "Geburtsdatum": datetime(1980, 5, 15),
            },
        ],
        "patient_with_diagnoses": [
            {
                "PatientID": 1001,
                "Name": "Müller",
                "Vorname": "Hans",
                "Geburtsdatum": datetime(1980, 5, 15),
                "ICD10": "E11.9",
                "Bezeichnung": "Diabetes mellitus, Type 2",
            },
        ],
        "list_tables": [
            {"Table Name": "Patient", "Column Count": 5, "Columns": "PatientID (int)\nName (varchar)"},
            {"Table Name": "Diagnose", "Column Count": 3, "Columns": "PatientID (int)\nICD10 (varchar)"},
        ],
        "table_columns": [
            {"COLUMN_NAME": "PatientID", "DATA_TYPE": "int"},
            {"COLUMN_NAME": "Name", "DATA_TYPE": "varchar"},
            {"COLUMN_NAME": "Vorname", "DATA_TYPE": "varchar"},
        ],
        "empty_result": [],
    }


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    return Mock()


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Set up test environment variables and cleanup."""
    # Mock environment variables to avoid requiring .env file
    test_env = {
        "SQL_SERVER": "test_server",
        "DATABASE": "test_db",
        "USERNAME_SQL": "test_user",
        "PASSWORD": "test_pass",
        "SQL_DRIVER": "{SQL Server Native Client 10.0}",
    }

    for key, value in test_env.items():
        monkeypatch.setenv(key, value)

    yield

    # Cleanup is automatic with monkeypatch


# Test markers for different test categories
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "database: mark test as requiring database")
    config.addinivalue_line("markers", "performance: mark test as a performance test")


# Custom assertions for testing
class CustomAssertions:
    """Custom assertion helpers for domain-specific testing."""

    @staticmethod
    def assert_valid_patient_data(data: Dict) -> None:
        """Assert that data contains valid patient fields."""
        required_fields = ["PatientID", "Name", "Vorname"]
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"
            assert data[field] is not None, f"Field {field} should not be None"

    @staticmethod
    def assert_valid_match_info(match_info: MatchInfo) -> None:
        """Assert that MatchInfo object is properly constructed."""
        assert match_info.field_name is not None
        assert match_info.match_type in ["Exact", "Fuzzy", "Mismatch", "NotCompared", "MissingDBValue", "YearMismatch"]
        if match_info.similarity_score is not None:
            assert 0.0 <= match_info.similarity_score <= 1.0

    @staticmethod
    def assert_sql_query_format(query: str) -> None:
        """Assert that SQL query is properly formatted."""
        assert isinstance(query, str)
        assert len(query.strip()) > 0
        assert query.strip().upper().startswith(("SELECT", "INSERT", "UPDATE", "DELETE"))


@pytest.fixture
def custom_assertions():
    """Provide custom assertion helpers."""
    return CustomAssertions()
