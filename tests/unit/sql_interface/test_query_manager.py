"""Unit tests for tbase_extractor.sql_interface.query_manager module."""

from unittest.mock import Mock, patch

import pytest

from tbase_extractor.sql_interface.exceptions import QueryTemplateNotFoundError
from tbase_extractor.sql_interface.query_manager import QueryManager


class TestQueryManagerInit:
    """Test QueryManager initialization."""

    def test_init_with_valid_directory(self, temp_dir):
        """Test initialization with valid templates directory."""
        # Create some test SQL files
        (temp_dir / "test_query.sql").write_text("SELECT * FROM test;")
        (temp_dir / "another_query.sql").write_text("SELECT * FROM another;")

        query_manager = QueryManager(temp_dir)

        assert query_manager.templates_dir == str(temp_dir)
        assert query_manager.debug is False

    def test_init_with_path_object(self, temp_dir):
        """Test initialization with Path object."""
        query_manager = QueryManager(temp_dir)

        assert query_manager.templates_dir == str(temp_dir)

    def test_init_with_debug_mode(self, temp_dir):
        """Test initialization with debug mode enabled."""
        (temp_dir / "test.sql").write_text("SELECT 1;")

        query_manager = QueryManager(temp_dir, debug=True)

        assert query_manager.debug is True

    def test_init_with_none_directory(self):
        """Test initialization with None directory raises ValueError."""
        with pytest.raises(ValueError, match="templates_dir cannot be None"):
            QueryManager(None)

    def test_init_with_nonexistent_directory(self):
        """Test initialization with nonexistent directory raises ValueError."""
        with pytest.raises(ValueError, match="templates_dir path does not exist"):
            QueryManager("/nonexistent/path")

    def test_init_with_file_instead_of_directory(self, temp_dir):
        """Test initialization with file instead of directory raises ValueError."""
        test_file = temp_dir / "not_a_directory.txt"
        test_file.write_text("content")

        with pytest.raises(ValueError, match="templates_dir is not a directory"):
            QueryManager(str(test_file))


class TestLoadQueryTemplate:
    """Test load_query_template method."""

    def test_load_existing_template(self, temp_dir):
        """Test loading an existing SQL template."""
        sql_content = "SELECT * FROM patients WHERE id = ?;"
        template_file = temp_dir / "get_patient.sql"
        template_file.write_text(sql_content, encoding="utf-8")

        query_manager = QueryManager(temp_dir)
        result = query_manager.load_query_template("get_patient")

        assert result == sql_content

    def test_load_template_with_sql_extension(self, temp_dir):
        """Test loading template when .sql extension is provided."""
        sql_content = "SELECT * FROM diagnoses;"
        template_file = temp_dir / "get_diagnoses.sql"
        template_file.write_text(sql_content, encoding="utf-8")

        query_manager = QueryManager(temp_dir)
        result = query_manager.load_query_template("get_diagnoses.sql")

        assert result == sql_content

    def test_load_nonexistent_template(self, temp_dir):
        """Test loading nonexistent template raises exception."""
        query_manager = QueryManager(temp_dir)

        with pytest.raises(QueryTemplateNotFoundError, match="SQL template file not found"):
            query_manager.load_query_template("nonexistent")

    def test_load_template_with_unicode(self, temp_dir):
        """Test loading template with unicode characters."""
        sql_content = "SELECT * FROM patients WHERE name LIKE '%Müller%';"
        template_file = temp_dir / "unicode_query.sql"
        template_file.write_text(sql_content, encoding="utf-8")

        query_manager = QueryManager(temp_dir)
        result = query_manager.load_query_template("unicode_query")

        assert result == sql_content
        assert "Müller" in result

    def test_load_template_with_multiline_sql(self, temp_dir):
        """Test loading multiline SQL template."""
        sql_content = """SELECT
    p.PatientID,
    p.Name,
    p.Vorname
FROM Patient p
WHERE p.PatientID = ?;"""

        template_file = temp_dir / "multiline.sql"
        template_file.write_text(sql_content, encoding="utf-8")

        query_manager = QueryManager(temp_dir)
        result = query_manager.load_query_template("multiline")

        assert result == sql_content
        assert "PatientID" in result
        assert "FROM Patient p" in result

    def test_load_template_file_read_error(self, temp_dir):
        """Test handling of file read errors."""
        template_file = temp_dir / "protected.sql"
        template_file.write_text("SELECT 1;")

        query_manager = QueryManager(temp_dir)

        # Mock open to raise OSError
        with patch("builtins.open", side_effect=OSError("Permission denied")), pytest.raises(
            QueryTemplateNotFoundError,
            match="Error reading SQL template file",
        ):
            query_manager.load_query_template("protected")


class TestPrebuiltQueryMethods:
    """Test prebuilt query helper methods."""

    def test_get_list_tables_query(self, temp_dir):
        """Test get_list_tables_query method."""
        sql_content = "SELECT name FROM sys.tables;"
        template_file = temp_dir / "list_tables.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)
        query, params = query_manager.get_list_tables_query()

        assert query == sql_content
        assert params == ()

    def test_get_patient_by_id_query(self, temp_dir):
        """Test get_patient_by_id_query method."""
        sql_content = "SELECT * FROM Patient WHERE PatientID = ?;"
        template_file = temp_dir / "get_patient_by_id.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)
        query, params = query_manager.get_patient_by_id_query(1001)

        assert query == sql_content
        assert params == (1001,)

    def test_get_patient_by_name_dob_query(self, temp_dir):
        """Test get_patient_by_name_dob_query method."""
        sql_content = "SELECT * FROM Patient WHERE Vorname = ? AND Name = ? AND Geburtsdatum = ?;"
        template_file = temp_dir / "get_patient_by_name_dob.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)
        from datetime import date

        test_date = date(1980, 5, 15)

        query, params = query_manager.get_patient_by_name_dob_query("Hans", "Müller", test_date)

        assert query == sql_content
        assert params == ("Hans", "Müller", test_date)

    def test_get_patients_by_dob_year_range_query(self, temp_dir):
        """Test get_patients_by_dob_year_range_query method."""
        sql_content = "SELECT * FROM Patient WHERE YEAR(Geburtsdatum) BETWEEN ? AND ?;"
        template_file = temp_dir / "get_patients_by_dob_year_range.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)
        query, params = query_manager.get_patients_by_dob_year_range_query(1980, 1990)

        assert query == sql_content
        assert params == (1980, 1990)

    def test_get_patients_by_lastname_like_query(self, temp_dir):
        """Test get_patients_by_lastname_like_query method."""
        sql_content = "SELECT * FROM Patient WHERE Name LIKE ?;"
        template_file = temp_dir / "get_patients_by_lastname_like.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)

        # Test without wildcards (should add %)
        query, params = query_manager.get_patients_by_lastname_like_query("Mueller")
        assert query == sql_content
        assert params == ("Mueller%",)

        # Test with existing wildcards (should not modify)
        query, params = query_manager.get_patients_by_lastname_like_query("Mue%ler")
        assert params == ("Mue%ler",)

        # Test with underscore wildcard
        query, params = query_manager.get_patients_by_lastname_like_query("M_eller")
        assert params == ("M_eller",)

    def test_get_all_patients_query(self, temp_dir):
        """Test get_all_patients_query method."""
        sql_content = "SELECT * FROM Patient;"
        template_file = temp_dir / "get_all_patients.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)
        query, params = query_manager.get_all_patients_query()

        assert query == sql_content
        assert params == ()

    def test_get_table_columns_query(self, temp_dir):
        """Test get_table_columns_query method."""
        sql_content = """SELECT COLUMN_NAME, DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?;"""
        template_file = temp_dir / "get_table_columns.sql"
        template_file.write_text(sql_content)

        query_manager = QueryManager(temp_dir)
        query, params = query_manager.get_table_columns_query("Patient", "dbo")

        assert query == sql_content
        assert params == ("Patient", "dbo")


class TestExecuteTemplateQuery:
    """Test execute_template_query method."""

    def test_execute_template_query_success(self, temp_dir):
        """Test successful template query execution."""
        sql_content = "SELECT * FROM patients WHERE id = ?;"
        template_file = temp_dir / "get_patient.sql"
        template_file.write_text(sql_content)

        # Mock database interface
        mock_db = Mock()
        mock_db.execute_query.return_value = True
        mock_db.fetch_results.return_value = [{"id": 1, "name": "Test Patient"}]

        query_manager = QueryManager(temp_dir)
        result = query_manager.execute_template_query(mock_db, "get_patient", {"id": 1})

        assert result == [{"id": 1, "name": "Test Patient"}]
        mock_db.execute_query.assert_called_once_with(sql_content, (1,))
        mock_db.fetch_results.assert_called_once()

    def test_execute_template_query_no_params(self, temp_dir):
        """Test template query execution without parameters."""
        sql_content = "SELECT COUNT(*) FROM patients;"
        template_file = temp_dir / "count_patients.sql"
        template_file.write_text(sql_content)

        mock_db = Mock()
        mock_db.execute_query.return_value = True
        mock_db.fetch_results.return_value = [{"count": 5}]

        query_manager = QueryManager(temp_dir)
        result = query_manager.execute_template_query(mock_db, "count_patients")

        assert result == [{"count": 5}]
        mock_db.execute_query.assert_called_once_with(sql_content, ())

    def test_execute_template_query_execution_failure(self, temp_dir):
        """Test template query execution failure."""
        sql_content = "INVALID SQL SYNTAX;"
        template_file = temp_dir / "invalid.sql"
        template_file.write_text(sql_content)

        mock_db = Mock()
        mock_db.execute_query.return_value = False  # Execution failed

        query_manager = QueryManager(temp_dir)
        result = query_manager.execute_template_query(mock_db, "invalid")

        assert result is None
        mock_db.fetch_results.assert_not_called()

    def test_execute_template_query_template_not_found(self, temp_dir):
        """Test template query execution with missing template."""
        mock_db = Mock()
        query_manager = QueryManager(temp_dir)

        result = query_manager.execute_template_query(mock_db, "nonexistent")

        assert result is None
        mock_db.execute_query.assert_not_called()

    def test_execute_template_query_fetch_failure(self, temp_dir):
        """Test template query execution with fetch failure."""
        sql_content = "SELECT * FROM patients;"
        template_file = temp_dir / "patients.sql"
        template_file.write_text(sql_content)

        mock_db = Mock()
        mock_db.execute_query.return_value = True
        mock_db.fetch_results.return_value = None  # Fetch failed

        query_manager = QueryManager(temp_dir)
        result = query_manager.execute_template_query(mock_db, "patients")

        assert result is None

    def test_execute_template_query_with_debug(self, temp_dir, caplog):
        """Test template query execution in debug mode."""
        sql_content = "SELECT * FROM patients WHERE id = ?;"
        template_file = temp_dir / "debug_query.sql"
        template_file.write_text(sql_content)

        mock_db = Mock()
        mock_db.execute_query.return_value = True
        mock_db.fetch_results.return_value = [{"id": 1}]

        with caplog.at_level('DEBUG'):
            query_manager = QueryManager(temp_dir, debug=True)
            query_manager.execute_template_query(mock_db, "debug_query", {"patient_id": 123})

        log_messages = caplog.text
        # The SecureLogger outputs different debug messages than the original print statements
        assert "Executing template 'debug_query'" in log_messages
        assert "Template parameters provided: 1 parameters" in log_messages

    def test_execute_template_query_exception_handling(self, temp_dir):
        """Test exception handling in template query execution."""
        sql_content = "SELECT * FROM patients;"
        template_file = temp_dir / "exception_query.sql"
        template_file.write_text(sql_content)

        mock_db = Mock()
        mock_db.execute_query.side_effect = Exception("Database error")

        query_manager = QueryManager(temp_dir)
        result = query_manager.execute_template_query(mock_db, "exception_query")

        assert result is None


@pytest.mark.unit
class TestQueryManagerIntegration:
    """Integration tests for QueryManager functionality."""

    def test_complete_query_workflow(self, temp_dir):
        """Test complete workflow from template loading to execution."""
        # Create realistic SQL templates
        patient_query = """
        SELECT
            PatientID,
            Name,
            Vorname,
            Geburtsdatum
        FROM Patient
        WHERE PatientID = ?;
        """
        template_file = temp_dir / "get_patient_by_id.sql"
        template_file.write_text(patient_query)

        # Mock successful database execution
        mock_db = Mock()
        mock_db.execute_query.return_value = True
        mock_db.fetch_results.return_value = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "Geburtsdatum": "1980-05-15"},
        ]

        query_manager = QueryManager(temp_dir)

        # Test direct query loading
        loaded_query = query_manager.load_query_template("get_patient_by_id")
        assert "PatientID" in loaded_query
        assert "Patient" in loaded_query

        # Test helper method
        query, params = query_manager.get_patient_by_id_query(1001)
        assert query == loaded_query
        assert params == (1001,)

        # Test execution
        result = query_manager.execute_template_query(mock_db, "get_patient_by_id", {"id": 1001})
        assert len(result) == 1
        assert result[0]["PatientID"] == 1001
        assert result[0]["Name"] == "Müller"

    def test_multiple_templates_management(self, temp_dir):
        """Test managing multiple SQL templates."""
        # Create multiple templates
        templates = {
            "list_tables.sql": "SELECT name FROM sys.tables;",
            "get_patient_by_id.sql": "SELECT * FROM Patient WHERE PatientID = ?;",
            "get_patient_by_name_dob.sql": "SELECT * FROM Patient WHERE Vorname = ? AND Name = ? AND Geburtsdatum = ?;",
            "count_patients.sql": "SELECT COUNT(*) as total FROM Patient;",
        }

        for filename, content in templates.items():
            (temp_dir / filename).write_text(content)

        query_manager = QueryManager(temp_dir)

        # Test loading each template
        for template_name, expected_content in templates.items():
            name_without_ext = template_name.replace(".sql", "")
            loaded_content = query_manager.load_query_template(name_without_ext)
            assert loaded_content == expected_content

    def test_template_with_complex_sql(self, temp_dir):
        """Test template with complex SQL including JOINs and conditions."""
        complex_sql = """
        SELECT
            p.PatientID,
            p.Name,
            p.Vorname,
            p.Geburtsdatum,
            d.ICD10,
            d.Bezeichnung,
            d.Date as DiagnosisDate
        FROM Patient p
        LEFT JOIN Diagnose d ON p.PatientID = d.PatientID
        WHERE p.PatientID = ?
            AND d.Date >= ?
            AND d.Date <= ?
        ORDER BY d.Date DESC;
        """

        template_file = temp_dir / "patient_with_diagnoses.sql"
        template_file.write_text(complex_sql)

        query_manager = QueryManager(temp_dir)
        loaded_query = query_manager.load_query_template("patient_with_diagnoses")

        assert "LEFT JOIN" in loaded_query
        assert "ORDER BY" in loaded_query
        assert loaded_query.count("?") == 3  # Three parameters

    def test_debug_mode_output(self, temp_dir, caplog):
        """Test debug mode produces correct output."""
        template_file = temp_dir / "debug_test.sql"
        template_file.write_text("SELECT 1;")
        another_file = temp_dir / "another_debug.sql"
        another_file.write_text("SELECT 2;")

        with caplog.at_level('DEBUG'):
            QueryManager(temp_dir, debug=True)

        log_messages = caplog.text
        # The SecureLogger outputs different debug messages than the original print statements
        assert "QueryManager initialized with templates directory" in log_messages
        assert "Available SQL templates: 2 files" in log_messages

    def test_realistic_parameter_handling(self, temp_dir):
        """Test realistic parameter handling scenarios."""
        # Template with multiple parameter types
        sql_template = """
        SELECT * FROM Patient
        WHERE (Vorname LIKE ? OR Vorname IS NULL)
          AND (Name LIKE ? OR Name IS NULL)
          AND Geburtsdatum BETWEEN ? AND ?
          AND PatientID IN (?, ?, ?);
        """

        template_file = temp_dir / "complex_search.sql"
        template_file.write_text(sql_template)

        query_manager = QueryManager(temp_dir)

        # Test with various parameter types
        from datetime import date

        params = {
            "first_name_pattern": "Hans%",
            "last_name_pattern": "Mül%",
            "start_date": date(1980, 1, 1),
            "end_date": date(1990, 12, 31),
            "id1": 1001,
            "id2": 1002,
            "id3": 1003,
        }

        mock_db = Mock()
        mock_db.execute_query.return_value = True
        mock_db.fetch_results.return_value = []

        query_manager.execute_template_query(mock_db, "complex_search", params)

        # Verify parameters were passed correctly
        expected_params = tuple(params.values())
        mock_db.execute_query.assert_called_once_with(sql_template, expected_params)
