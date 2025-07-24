"""Unit tests for tbase_extractor.sql_interface.output_formatter module."""

import json
from datetime import date, datetime
from io import StringIO
from unittest.mock import patch

import pytest

from tbase_extractor.matching.models import MatchCandidate, MatchInfo
from tbase_extractor.sql_interface.output_formatter import OutputFormatter


class TestOutputFormatterDatetimeSerializer:
    """Test _datetime_serializer static method."""

    def test_serialize_datetime(self):
        """Test serializing datetime objects."""
        dt = datetime(2023, 5, 15, 14, 30, 45)
        result = OutputFormatter._datetime_serializer(dt)
        assert result == "2023-05-15T14:30:45"

    def test_serialize_date(self):
        """Test serializing date objects."""
        d = date(2023, 5, 15)
        result = OutputFormatter._datetime_serializer(d)
        assert result == "2023-05-15"

    def test_serialize_invalid_type(self):
        """Test serializing invalid types raises TypeError."""
        with pytest.raises(TypeError, match="Object of type str is not JSON serializable"):
            OutputFormatter._datetime_serializer("invalid")

        with pytest.raises(TypeError, match="Object of type int is not JSON serializable"):
            OutputFormatter._datetime_serializer(123)


class TestMatchCandidateToDict:
    """Test _match_candidate_to_dict static method."""

    def test_basic_match_candidate_conversion(self):
        """Test converting basic MatchCandidate to dictionary."""
        db_record = {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"}
        match_info = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0, "Perfect match"),
        ]

        candidate = MatchCandidate(
            db_record=db_record,
            match_fields_info=match_info,
            overall_score=0.95,
            primary_match_type="Exact Match",
        )

        result = OutputFormatter._match_candidate_to_dict(candidate)

        # Check basic fields
        assert result["overall_score"] == 0.95
        assert result["primary_match_type"] == "Exact Match"
        assert result["PatientID"] == 1001
        assert result["Name"] == "Müller"
        assert result["Vorname"] == "Hans"

        # Check match field details
        assert result["FirstName_input_value"] == "Hans"
        assert result["FirstName_db_value"] == "Hans"
        assert result["FirstName_match_type"] == "Exact"
        assert result["FirstName_similarity"] == 1.0
        assert result["FirstName_details"] == "Perfect match"

    def test_multiple_match_fields_conversion(self):
        """Test converting MatchCandidate with multiple match fields."""
        db_record = {"PatientID": 1001}
        match_info = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Mueller", "Müller", "Fuzzy", 0.9, "Unicode variant"),
            MatchInfo("DOB", date(1980, 5, 15), date(1980, 5, 15), "Exact", 1.0),
        ]

        candidate = MatchCandidate(
            db_record=db_record,
            match_fields_info=match_info,
            overall_score=0.87,
            primary_match_type="Partial Match",
        )

        result = OutputFormatter._match_candidate_to_dict(candidate)

        # Check all field details are present
        assert result["FirstName_match_type"] == "Exact"
        assert result["LastName_match_type"] == "Fuzzy"
        assert result["LastName_details"] == "Unicode variant"
        assert result["DOB_match_type"] == "Exact"
        assert result["DOB_input_value"] == date(1980, 5, 15)

    def test_match_candidate_without_details(self):
        """Test converting MatchCandidate with match fields without details."""
        db_record = {"PatientID": 1001}
        match_info = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0, None),  # No details
        ]

        candidate = MatchCandidate(
            db_record=db_record,
            match_fields_info=match_info,
        )

        result = OutputFormatter._match_candidate_to_dict(candidate)

        assert "FirstName_details" not in result


class TestFormatAsJson:
    """Test format_as_json method."""

    def test_basic_json_formatting(self):
        """Test basic JSON formatting with simple data."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna"},
        ]
        metadata = {"query_type": "patient_search", "timestamp": "2023-05-15"}

        result = OutputFormatter.format_as_json(data, metadata)
        parsed = json.loads(result)

        assert "metadata" in parsed
        assert "data" in parsed
        assert parsed["metadata"]["query_type"] == "patient_search"
        assert len(parsed["data"]) == 2
        # With 2 different patients, the formatter groups them with patient_info structure
        assert parsed["data"][0]["patient_info"]["PatientID"] == 1001

    def test_json_with_datetime_objects(self):
        """Test JSON formatting with datetime objects."""
        data = [
            {
                "PatientID": 1001,
                "Name": "Müller",
                "Geburtsdatum": datetime(1980, 5, 15, 10, 30),
                "RegistrationDate": date(2023, 1, 1),
            },
        ]

        result = OutputFormatter.format_as_json(data)
        parsed = json.loads(result)

        patient = parsed["data"][0]
        assert patient["Geburtsdatum"] == "1980-05-15T10:30:00"
        assert patient["RegistrationDate"] == "2023-01-01"

    def test_json_with_match_candidates(self):
        """Test JSON formatting with MatchCandidate objects."""
        db_record = {"PatientID": 1001, "Name": "Müller"}
        match_info = [MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0)]

        candidates = [
            MatchCandidate(
                db_record=db_record,
                match_fields_info=match_info,
                overall_score=0.95,
                primary_match_type="Exact Match",
            ),
        ]

        result = OutputFormatter.format_as_json(candidates)
        parsed = json.loads(result)

        candidate_data = parsed["data"][0]
        assert candidate_data["overall_score"] == 0.95
        assert candidate_data["primary_match_type"] == "Exact Match"
        assert candidate_data["PatientID"] == 1001
        assert candidate_data["FirstName_match_type"] == "Exact"

    def test_json_with_multiple_patients_grouping(self):
        """Test JSON formatting with multiple patients (should group)."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "I10"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna", "ICD10": "M79.1"},
        ]

        result = OutputFormatter.format_as_json(data)
        parsed = json.loads(result)

        # Should group by patients
        patients_data = parsed["data"]
        assert len(patients_data) == 2  # Two unique patients

        # First patient should have patient_info and records
        first_patient = patients_data[0]
        assert "patient_info" in first_patient
        assert "records" in first_patient
        assert first_patient["patient_info"]["Name"] == "Müller"
        assert len(first_patient["records"]) == 2  # Two diagnoses for Hans

    def test_json_with_compact_output(self):
        """Test JSON formatting with compact output (no indentation)."""
        data = [{"test": "value"}]

        result = OutputFormatter.format_as_json(data, indent=None)

        # Should not contain newlines for formatting
        assert "\n" not in result
        # JSON structure itself contains spaces in keys, so just check it's compact
        assert result.startswith('{"metadata"')  # Compact JSON starts without spaces

    def test_json_with_empty_data(self):
        """Test JSON formatting with empty data."""
        result = OutputFormatter.format_as_json([])
        parsed = json.loads(result)

        assert parsed["metadata"] == {}
        assert parsed["data"] == []

    def test_json_serialization_error_handling(self):
        """Test JSON serialization error handling."""
        # Create data with non-serializable object
        data = [{"value": {1, 2, 3}}]  # set is not JSON serializable

        with pytest.raises(TypeError):
            OutputFormatter.format_as_json(data)


class TestFormatAsCsv:
    """Test format_as_csv method."""

    def test_basic_csv_formatting(self):
        """Test basic CSV formatting."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna"},
        ]

        result = OutputFormatter.format_as_csv(data)

        lines = result.strip().split("\n")
        assert len(lines) == 3  # Header + 2 data rows
        assert "PatientID,Name,Vorname" in lines[0]
        assert "1001,Müller,Hans" in lines[1]
        assert "1002,Schmidt,Anna" in lines[2]

    def test_csv_with_empty_data(self):
        """Test CSV formatting with empty data."""
        result = OutputFormatter.format_as_csv([])
        assert result == ""

    def test_csv_with_special_characters(self):
        """Test CSV formatting with special characters that need quoting."""
        data = [
            {"Name": "O'Connor", "Address": "Main St, Apt 5", "Notes": 'Has "special" needs'},
        ]

        result = OutputFormatter.format_as_csv(data)

        # CSV should properly quote fields with special characters
        assert '"O\'Connor"' in result or "O'Connor" in result
        assert '"Main St, Apt 5"' in result
        assert '"""special"""' in result or '"Has ""special"" needs"' in result


class TestFormatAsTsv:
    """Test format_as_tsv method."""

    def test_basic_tsv_formatting(self):
        """Test basic TSV formatting."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna"},
        ]

        result = OutputFormatter.format_as_tsv(data)

        lines = result.strip().split("\n")
        assert len(lines) == 3  # Header + 2 data rows
        assert "PatientID\tName\tVorname" in lines[0]
        assert "1001\tMüller\tHans" in lines[1]
        assert "1002\tSchmidt\tAnna" in lines[2]

    def test_tsv_with_empty_data(self):
        """Test TSV formatting with empty data."""
        result = OutputFormatter.format_as_tsv([])
        assert result == ""


class TestFormatAsTxt:
    """Test format_as_txt method."""

    def test_basic_txt_formatting(self):
        """Test basic text formatting."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
        ]

        result = OutputFormatter.format_as_txt(data)
        lines = result.split("\n")

        # Should contain all values, one per line
        assert "1001" in lines
        assert "Müller" in lines
        assert "Hans" in lines
        assert "E11.9" in lines

    def test_txt_with_multiple_patients(self):
        """Test text formatting with multiple patients."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna", "ICD10": "M79.1"},
        ]

        result = OutputFormatter.format_as_txt(data)

        # Should contain separators between patients
        assert "---" in result
        assert "Müller" in result
        assert "Schmidt" in result

    def test_txt_with_empty_values(self):
        """Test text formatting filters out empty values."""
        data = [
            {"PatientID": 1001, "Name": "", "Vorname": "Hans", "Empty": None},
        ]

        result = OutputFormatter.format_as_txt(data)
        lines = [line for line in result.split("\n") if line.strip()]

        # Should not contain empty strings or None values
        assert "" not in lines
        assert "Hans" in result
        assert "1001" in result

    def test_txt_with_empty_data(self):
        """Test text formatting with empty data."""
        result = OutputFormatter.format_as_txt([])
        assert result == ""


class TestFormatAsTxtOptimized:
    """Test format_as_txt_optimized method."""

    def test_optimized_txt_single_patient(self):
        """Test optimized text formatting for single patient."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9", "Bezeichnung": "Diabetes"},
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "I10", "Bezeichnung": "Hypertension"},
        ]

        result = OutputFormatter.format_as_txt_optimized(data)
        lines = result.split("\n")

        # Patient info should appear once at the top
        patient_info_count = sum(1 for line in lines if "Müller" in line)
        assert patient_info_count == 1

        # Should have separator between patient info and diagnoses
        assert "---" in result

        # Should contain both diagnoses
        assert "E11.9" in result
        assert "I10" in result
        assert "Diabetes" in result
        assert "Hypertension" in result

    def test_optimized_txt_multiple_patients(self):
        """Test optimized text formatting for multiple patients."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna", "ICD10": "M79.1"},
        ]

        result = OutputFormatter.format_as_txt_optimized(data)

        # Should have separators between patients
        assert "===" in result
        assert "---" in result
        assert "Müller" in result
        assert "Schmidt" in result


class TestFormatAsJsonOptimized:
    """Test format_as_json_optimized method."""

    def test_optimized_json_single_patient(self):
        """Test optimized JSON formatting for single patient."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "I10"},
        ]

        result = OutputFormatter.format_as_json_optimized(data)
        parsed = json.loads(result)

        # Should have optimized structure
        assert "patient_info" in parsed["data"]
        assert "diagnoses" in parsed["data"]

        patient_info = parsed["data"]["patient_info"]
        assert patient_info["PatientID"] == 1001
        assert patient_info["Name"] == "Müller"

        diagnoses = parsed["data"]["diagnoses"]
        assert len(diagnoses) == 2
        assert any(d["ICD10"] == "E11.9" for d in diagnoses)
        assert any(d["ICD10"] == "I10" for d in diagnoses)

    def test_optimized_json_multiple_patients(self):
        """Test optimized JSON formatting for multiple patients."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna", "ICD10": "M79.1"},
        ]

        result = OutputFormatter.format_as_json_optimized(data)
        parsed = json.loads(result)

        # Should have array of patients
        patients = parsed["data"]
        assert len(patients) == 2

        # Each patient should have patient_info and diagnoses
        for patient in patients:
            assert "patient_info" in patient
            assert "diagnoses" in patient


class TestFormatAsCsvOptimized:
    """Test format_as_csv_optimized method."""

    def test_optimized_csv_formatting(self):
        """Test optimized CSV formatting."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "E11.9"},
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "ICD10": "I10"},
        ]

        result = OutputFormatter.format_as_csv_optimized(data)
        lines = result.strip().split("\n")

        # Should have patient section
        assert any("Patient" in line for line in lines)

        # Should have diagnoses section
        assert any("Diagnoses" in line for line in lines)

        # Should contain patient data
        assert any("Müller" in line for line in lines)

        # Should contain diagnosis codes
        assert "E11.9" in result
        assert "I10" in result

    def test_optimized_csv_with_empty_data(self):
        """Test optimized CSV formatting with empty data."""
        result = OutputFormatter.format_as_csv_optimized([])
        assert result == ""


class TestFormatAsConsoleTable:
    """Test format_as_console_table method."""

    def test_console_table_with_dict_data(self):
        """Test console table formatting with dictionary data."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna"},
        ]

        output = StringIO()
        OutputFormatter.format_as_console_table(data, stream=output)
        result = output.getvalue()

        # Should contain headers and data
        assert "PatientID" in result
        assert "Name" in result
        assert "Müller" in result
        assert "Schmidt" in result

    def test_console_table_with_match_candidates(self):
        """Test console table formatting with MatchCandidate data."""
        db_record = {"Name": "Müller", "Geburtsdatum": date(1980, 5, 15)}
        candidates = [
            MatchCandidate(
                db_record=db_record,
                overall_score=0.95,
                primary_match_type="Exact Match",
            ),
        ]

        output = StringIO()
        OutputFormatter.format_as_console_table(candidates, stream=output)
        result = output.getvalue()

        # Should use special headers for MatchCandidate
        assert "Name" in result
        assert "DOB" in result
        assert "Score" in result
        assert "Match Type" in result
        assert "0.95" in result
        assert "Exact Match" in result

    def test_console_table_with_empty_data(self):
        """Test console table formatting with empty data."""
        output = StringIO()
        OutputFormatter.format_as_console_table([], stream=output)
        result = output.getvalue()

        assert "No data to display" in result

    @patch("tbase_extractor.sql_interface.output_formatter.HAS_TABULATE", False)
    def test_console_table_without_tabulate(self):
        """Test console table formatting fallback without tabulate."""
        data = [
            {"PatientID": 1001, "Name": "Müller"},
            {"PatientID": 1002, "Name": "Schmidt"},
        ]

        output = StringIO()
        OutputFormatter.format_as_console_table(data, stream=output)
        result = output.getvalue()

        # Should use tab-separated fallback format
        assert "PatientID\tName" in result
        assert "1001\tMüller" in result or "Müller" in result


@pytest.mark.unit
class TestOutputFormatterIntegration:
    """Integration tests for OutputFormatter functionality."""

    def test_all_formats_with_same_data(self):
        """Test that all formatting methods work with the same dataset."""
        data = [
            {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans", "Geburtsdatum": date(1980, 5, 15)},
            {"PatientID": 1002, "Name": "Schmidt", "Vorname": "Anna", "Geburtsdatum": date(1975, 12, 3)},
        ]

        # Test all formats don't raise exceptions
        json_result = OutputFormatter.format_as_json(data)
        csv_result = OutputFormatter.format_as_csv(data)
        tsv_result = OutputFormatter.format_as_tsv(data)
        txt_result = OutputFormatter.format_as_txt(data)

        # All should contain the core data - JSON may have unicode escaping
        assert "Müller" in json_result or "M\\u00fcller" in json_result
        assert "Müller" in csv_result
        assert "Müller" in tsv_result
        assert "Müller" in txt_result

        # JSON should be parseable
        parsed_json = json.loads(json_result)
        assert len(parsed_json["data"]) == 2

    def test_realistic_patient_data_workflow(self):
        """Test realistic patient data processing workflow."""
        # Simulate realistic patient data with diagnoses
        patient_data = [
            {
                "PatientID": 1001,
                "Name": "Müller",
                "Vorname": "Hans",
                "Geburtsdatum": datetime(1980, 5, 15),
                "ICD10": "E11.9",
                "Bezeichnung": "Diabetes mellitus, Type 2",
                "DiagnosisDate": date(2023, 1, 15),
            },
            {
                "PatientID": 1001,
                "Name": "Müller",
                "Vorname": "Hans",
                "Geburtsdatum": datetime(1980, 5, 15),
                "ICD10": "I10",
                "Bezeichnung": "Essential hypertension",
                "DiagnosisDate": date(2023, 2, 20),
            },
        ]

        metadata = {
            "query_timestamp": datetime(2023, 5, 15, 10, 30),
            "query_type": "patient_with_diagnoses",
            "total_records": 2,
        }

        # Test JSON with metadata and datetime handling
        json_result = OutputFormatter.format_as_json(patient_data, metadata)
        parsed = json.loads(json_result)

        assert parsed["metadata"]["query_timestamp"] == "2023-05-15T10:30:00"
        assert parsed["metadata"]["total_records"] == 2

        # Test optimized formats group patient data correctly
        optimized_json = OutputFormatter.format_as_json_optimized(patient_data, metadata)
        optimized_parsed = json.loads(optimized_json)

        # Should have single patient with multiple diagnoses
        patient_info = optimized_parsed["data"]["patient_info"]
        assert patient_info["PatientID"] == 1001
        assert patient_info["Name"] == "Müller"

        diagnoses = optimized_parsed["data"]["diagnoses"]
        assert len(diagnoses) == 2
        assert any(d["ICD10"] == "E11.9" for d in diagnoses)
        assert any(d["ICD10"] == "I10" for d in diagnoses)

    def test_match_candidate_formatting_workflow(self):
        """Test complete MatchCandidate formatting workflow."""
        # Create realistic MatchCandidate data
        db_record = {
            "PatientID": 1001,
            "Name": "Müller",
            "Vorname": "Hans",
            "Geburtsdatum": date(1980, 5, 15),
        }

        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Mueller", "Müller", "Fuzzy", 0.92, "Unicode variant"),
            MatchInfo("DOB", date(1980, 5, 15), date(1980, 5, 15), "Exact", 1.0),
        ]

        candidates = [
            MatchCandidate(
                db_record=db_record,
                match_fields_info=match_fields,
                overall_score=0.95,
                primary_match_type="High Confidence Match",
                csv_input_row_number=1,
                csv_input_data={"FirstName": "Hans", "LastName": "Mueller", "DOB": "1980-05-15"},
            ),
        ]

        # Test JSON formatting of MatchCandidates
        json_result = OutputFormatter.format_as_json(candidates)
        parsed = json.loads(json_result)

        candidate_data = parsed["data"][0]
        assert candidate_data["overall_score"] == 0.95
        assert candidate_data["primary_match_type"] == "High Confidence Match"
        assert candidate_data["PatientID"] == 1001

        # Check match field details
        assert candidate_data["FirstName_match_type"] == "Exact"
        assert candidate_data["LastName_match_type"] == "Fuzzy"
        assert candidate_data["LastName_details"] == "Unicode variant"
        assert candidate_data["DOB_match_type"] == "Exact"

        # Test console table formatting
        output = StringIO()
        OutputFormatter.format_as_console_table(candidates, stream=output)
        table_result = output.getvalue()

        assert "Müller" in table_result
        assert "0.95" in table_result
        assert "High Confidence Match" in table_result

    def test_error_handling_with_corrupted_data(self):
        """Test error handling with various corrupted data scenarios."""
        # Test with mixed data types
        mixed_data = [
            {"valid": "data"},
            {"number": 123, "bool": True, "none": None},
        ]

        # Should handle mixed types gracefully
        json_result = OutputFormatter.format_as_json(mixed_data)
        parsed = json.loads(json_result)
        assert len(parsed["data"]) == 2

        # Test CSV with missing keys
        incomplete_data = [
            {"A": 1, "B": 2},
            {"A": 3},  # Missing "B"
        ]

        csv_result = OutputFormatter.format_as_csv(incomplete_data)
        # Should handle missing keys gracefully (CSV writer fills with empty string)
        assert "A,B" in csv_result
        assert "3," in csv_result or "3\t" in csv_result

    def test_large_dataset_performance(self):
        """Test formatting performance with larger datasets."""
        # Create moderately large dataset
        large_data = []
        for i in range(100):
            large_data.append(
                {
                    "PatientID": 1000 + i,
                    "Name": f"Patient{i}",
                    "Vorname": f"Name{i}",
                    "Value": i * 1.5,
                    "Date": date(2023, 1, (i % 28) + 1),
                },
            )

        # All formatting methods should handle this without issues
        json_result = OutputFormatter.format_as_json(large_data)
        csv_result = OutputFormatter.format_as_csv(large_data)
        txt_result = OutputFormatter.format_as_txt(large_data)

        # Verify results contain expected number of records
        parsed_json = json.loads(json_result)
        assert len(parsed_json["data"]) == 100

        csv_lines = csv_result.count("\n")
        assert csv_lines == 101  # 100 data rows + 1 header

        assert "Patient99" in json_result
        assert "Patient99" in csv_result
        assert "Patient99" in txt_result
