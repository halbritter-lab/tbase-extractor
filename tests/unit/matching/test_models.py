"""Unit tests for tbase_extractor.matching.models module."""

from datetime import date

import pytest

from tbase_extractor.matching.models import MatchCandidate, MatchInfo


class TestMatchInfo:
    """Test MatchInfo dataclass functionality."""

    def test_basic_initialization(self):
        """Test basic MatchInfo initialization."""
        match_info = MatchInfo(field_name="FirstName", input_value="Hans", db_value="Hans", match_type="Exact")

        assert match_info.field_name == "FirstName"
        assert match_info.input_value == "Hans"
        assert match_info.db_value == "Hans"
        assert match_info.match_type == "Exact"
        assert match_info.similarity_score is None
        assert match_info.details is None

    def test_initialization_with_optional_fields(self):
        """Test MatchInfo initialization with all fields."""
        match_info = MatchInfo(
            field_name="LastName",
            input_value="Mueller",
            db_value="Müller",
            match_type="Fuzzy",
            similarity_score=0.95,
            details="Unicode variant",
        )

        assert match_info.field_name == "LastName"
        assert match_info.input_value == "Mueller"
        assert match_info.db_value == "Müller"
        assert match_info.match_type == "Fuzzy"
        assert match_info.similarity_score == 0.95
        assert match_info.details == "Unicode variant"

    def test_different_data_types(self):
        """Test MatchInfo with different data types."""
        # String values
        string_match = MatchInfo("Name", "Hans", "Hans", "Exact")
        assert isinstance(string_match.input_value, str)

        # Date values
        test_date = date(1980, 5, 15)
        date_match = MatchInfo("DOB", test_date, test_date, "Exact")
        assert isinstance(date_match.input_value, date)

        # None values
        none_match = MatchInfo("Field", None, "Value", "MissingInput")
        assert none_match.input_value is None

        # Numeric values
        numeric_match = MatchInfo("Age", 25, 25, "Exact")
        assert isinstance(numeric_match.input_value, int)

    def test_equality(self):
        """Test MatchInfo equality comparison."""
        match1 = MatchInfo("Field", "Value1", "Value2", "Fuzzy", 0.8)
        match2 = MatchInfo("Field", "Value1", "Value2", "Fuzzy", 0.8)
        match3 = MatchInfo("Field", "Value1", "Value2", "Fuzzy", 0.9)

        assert match1 == match2
        assert match1 != match3

    def test_repr_and_str(self):
        """Test string representation of MatchInfo."""
        match_info = MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0)

        # Should be able to create string representation
        str_repr = str(match_info)
        assert "FirstName" in str_repr
        assert "Exact" in str_repr

        # Should be able to create repr
        repr_str = repr(match_info)
        assert "MatchInfo" in repr_str


class TestMatchCandidate:
    """Test MatchCandidate dataclass functionality."""

    def test_basic_initialization(self):
        """Test basic MatchCandidate initialization."""
        db_record = {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"}
        candidate = MatchCandidate(db_record=db_record)

        assert candidate.db_record == db_record
        assert candidate.match_fields_info == []
        assert candidate.overall_score == 0.0
        assert candidate.primary_match_type == "NoMatch"
        assert candidate.csv_input_row_number is None
        assert candidate.csv_input_data is None

    def test_initialization_with_match_info(self, sample_match_info):
        """Test MatchCandidate initialization with match info."""
        db_record = {"PatientID": 1001, "Name": "Müller"}
        candidate = MatchCandidate(
            db_record=db_record,
            match_fields_info=sample_match_info,
            overall_score=0.95,
            primary_match_type="Exact Match",
        )

        assert candidate.db_record == db_record
        assert candidate.match_fields_info == sample_match_info
        assert candidate.overall_score == 0.95
        assert candidate.primary_match_type == "Exact Match"

    def test_csv_metadata_fields(self):
        """Test CSV-related metadata fields."""
        db_record = {"PatientID": 1001}
        csv_data = {"FirstName": "Hans", "LastName": "Mueller", "DOB": "1980-05-15"}

        candidate = MatchCandidate(db_record=db_record, csv_input_row_number=5, csv_input_data=csv_data)

        assert candidate.csv_input_row_number == 5
        assert candidate.csv_input_data == csv_data


class TestCalculateOverallScoreAndType:
    """Test MatchCandidate.calculate_overall_score_and_type method."""

    def test_exact_match_all_fields(self):
        """Test calculation with all exact matches."""
        db_record = {"PatientID": 1001, "Name": "Müller", "Vorname": "Hans"}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Müller", "Müller", "Exact", 1.0),
            MatchInfo("DOB", date(1980, 5, 15), date(1980, 5, 15), "Exact", 1.0),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.3, "LastName": 0.4, "DOB": 0.3}
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity", "Mismatch": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        assert candidate.overall_score == 1.0
        assert candidate.primary_match_type == "Exact Match"

    def test_mixed_match_types(self):
        """Test calculation with mixed match types."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Mueller", "Müller", "Fuzzy", 0.9),
            MatchInfo("DOB", date(1980, 5, 15), date(1981, 5, 15), "YearMismatch", 0.7),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.3, "LastName": 0.4, "DOB": 0.3}
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity", "YearMismatch": 0.7, "Mismatch": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Expected: 0.3*1.0 + 0.4*0.9 + 0.3*0.7 = 0.3 + 0.36 + 0.21 = 0.87
        expected_score = 0.87
        assert abs(candidate.overall_score - expected_score) < 0.01
        assert candidate.primary_match_type.startswith("Partial Match:")
        assert "Exact" in candidate.primary_match_type
        assert "Fuzzy" in candidate.primary_match_type
        assert "YearMismatch" in candidate.primary_match_type

    def test_no_significant_match(self):
        """Test calculation with very low scores."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Different", "Mismatch", 0.0),
            MatchInfo("LastName", "Mueller", "Other", "Mismatch", 0.0),
            MatchInfo("DOB", date(1980, 5, 15), date(1990, 8, 20), "Mismatch", 0.0),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.3, "LastName": 0.4, "DOB": 0.3}
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity", "Mismatch": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        assert candidate.overall_score == 0.0
        assert candidate.primary_match_type == "No Significant Match"

    def test_fuzzy_similarity_score_usage(self):
        """Test that fuzzy matches use actual similarity scores."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Smith", "Smyth", "Fuzzy", 0.85),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.5, "LastName": 0.5}
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity", "Mismatch": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Expected: 0.5*1.0 + 0.5*0.85 = 0.5 + 0.425 = 0.925
        expected_score = 0.925
        assert abs(candidate.overall_score - expected_score) < 0.01

    def test_missing_field_weights(self):
        """Test handling of fields not in field_weights."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("UnweightedField", "Value", "Value", "Exact", 1.0),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 1.0}  # Missing UnweightedField
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity", "Mismatch": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Should only consider weighted fields
        assert candidate.overall_score == 1.0
        assert candidate.primary_match_type == "Exact Match"

    def test_missing_score_mapping(self):
        """Test handling of match types not in score_mapping."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Value", "Value", "UnknownType", 1.0),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.5, "LastName": 0.5}
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity"}  # Missing UnknownType

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Unknown type should contribute 0 to score
        assert candidate.overall_score == 0.5  # Only FirstName contributes
        assert "Exact" in candidate.primary_match_type

    def test_empty_match_fields(self):
        """Test calculation with no match fields."""
        db_record = {"PatientID": 1001}
        candidate = MatchCandidate(db_record=db_record, match_fields_info=[])

        field_weights = {"FirstName": 1.0}
        score_mapping = {"Exact": 1.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        assert candidate.overall_score == 0.0
        assert candidate.primary_match_type == "No Significant Match"

    def test_not_compared_fields(self):
        """Test handling of NotCompared fields."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", None, "Value", "NotCompared", None),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.5, "LastName": 0.5}
        score_mapping = {"Exact": 1.0, "NotCompared": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # NotCompared fields should not affect score calculation for exact match determination
        assert candidate.overall_score == 0.5  # Only FirstName contributes
        # The implementation includes all fields in the summary, including NotCompared ones
        assert candidate.primary_match_type == "Partial Match: FirstName:Exact|LastName:NotCompared"

    def test_invalid_similarity_score_for_fuzzy(self):
        """Test handling of fuzzy match with invalid similarity score."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("FirstName", "Hans", "Similar", "Fuzzy", None),  # No similarity score
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 1.0}
        score_mapping = {"Fuzzy": "use_similarity"}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Should handle gracefully and use 0 score
        assert candidate.overall_score == 0.0
        assert candidate.primary_match_type == "No Significant Match"


@pytest.mark.unit
class TestMatchingModelsIntegration:
    """Integration tests for matching models."""

    def test_realistic_patient_matching_scenario(self):
        """Test realistic patient matching scenario."""
        # Simulate a patient record from database
        db_record = {
            "PatientID": 1001,
            "Name": "Müller",
            "Vorname": "Hans",
            "Geburtsdatum": date(1980, 5, 15),
        }

        # Simulate match results from fuzzy matcher
        match_fields = [
            MatchInfo("FirstName", "Hans", "Hans", "Exact", 1.0),
            MatchInfo("LastName", "Mueller", "Müller", "Fuzzy", 0.92),
            MatchInfo("DOB", date(1980, 5, 15), date(1980, 5, 15), "Exact", 1.0),
        ]

        candidate = MatchCandidate(
            db_record=db_record,
            match_fields_info=match_fields,
            csv_input_row_number=3,
            csv_input_data={"FirstName": "Hans", "LastName": "Mueller", "DOB": "1980-05-15"},
        )

        # Typical field weights for patient matching
        field_weights = {"FirstName": 0.3, "LastName": 0.4, "DOB": 0.3}
        score_mapping = {
            "Exact": 1.0,
            "Fuzzy": "use_similarity",
            "YearMismatch": 0.7,
            "Mismatch": 0.0,
            "NotCompared": 0.0,
            "MissingDBValue": -0.1,
        }

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Verify high-quality match
        assert candidate.overall_score > 0.9
        assert "Partial Match:" in candidate.primary_match_type
        assert "Exact" in candidate.primary_match_type
        assert "Fuzzy" in candidate.primary_match_type

        # Verify metadata is preserved
        assert candidate.csv_input_row_number == 3
        assert candidate.csv_input_data["FirstName"] == "Hans"

    def test_field_match_summaries_ordering(self):
        """Test that field match summaries are consistently ordered."""
        db_record = {"PatientID": 1001}
        match_fields = [
            MatchInfo("DOB", date(1980, 5, 15), date(1980, 5, 15), "Exact", 1.0),
            MatchInfo("FirstName", "Hans", "Similar", "Fuzzy", 0.8),
            MatchInfo("LastName", "Mueller", "Different", "Mismatch", 0.0),
        ]

        candidate = MatchCandidate(db_record=db_record, match_fields_info=match_fields)

        field_weights = {"FirstName": 0.3, "LastName": 0.4, "DOB": 0.3}
        score_mapping = {"Exact": 1.0, "Fuzzy": "use_similarity", "Mismatch": 0.0}

        candidate.calculate_overall_score_and_type(field_weights, score_mapping)

        # Match types should be sorted alphabetically in the summary
        assert candidate.primary_match_type.startswith("Partial Match:")

        # Extract the summary part after "Partial Match: "
        summary_part = candidate.primary_match_type.split("Partial Match: ")[1]
        match_types = summary_part.split("|")

        # Should be sorted
        assert match_types == sorted(match_types)

    def test_dataclass_features(self):
        """Test that dataclass features work correctly."""
        db_record = {"PatientID": 1001}
        match_info = MatchInfo("Field", "Input", "DB", "Exact", 1.0)

        # Test dataclass equality
        candidate1 = MatchCandidate(db_record=db_record, match_fields_info=[match_info])
        candidate2 = MatchCandidate(db_record=db_record, match_fields_info=[match_info])

        assert candidate1 == candidate2

        # Test dataclass representation
        repr_str = repr(candidate1)
        assert "MatchCandidate" in repr_str
        assert "db_record" in repr_str

        # Test field access
        assert hasattr(candidate1, "db_record")
        assert hasattr(candidate1, "match_fields_info")
        assert hasattr(candidate1, "overall_score")
