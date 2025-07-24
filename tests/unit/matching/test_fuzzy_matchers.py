"""Unit tests for tbase_extractor.matching.fuzzy_matchers module."""

from datetime import date

import pytest

from tbase_extractor.matching.fuzzy_matchers import FuzzyMatcher
from tbase_extractor.matching.models import MatchInfo


class TestFuzzyMatcherInitialization:
    """Test FuzzyMatcher initialization and configuration."""

    def test_default_initialization(self):
        """Test initialization with default parameters."""
        matcher = FuzzyMatcher()
        assert matcher.string_similarity_threshold == 0.85
        assert matcher.date_year_tolerance == 1

    def test_custom_initialization(self):
        """Test initialization with custom parameters."""
        matcher = FuzzyMatcher(string_similarity_threshold=0.9, date_year_tolerance=2)
        assert matcher.string_similarity_threshold == 0.9
        assert matcher.date_year_tolerance == 2

    def test_invalid_threshold_too_low(self):
        """Test initialization with threshold below 0.0."""
        with pytest.raises(ValueError, match="string_similarity_threshold must be between 0.0 and 1.0"):
            FuzzyMatcher(string_similarity_threshold=-0.1)

    def test_invalid_threshold_too_high(self):
        """Test initialization with threshold above 1.0."""
        with pytest.raises(ValueError, match="string_similarity_threshold must be between 0.0 and 1.0"):
            FuzzyMatcher(string_similarity_threshold=1.1)

    def test_boundary_thresholds(self):
        """Test initialization with boundary threshold values."""
        # Should work with 0.0
        matcher_min = FuzzyMatcher(string_similarity_threshold=0.0)
        assert matcher_min.string_similarity_threshold == 0.0

        # Should work with 1.0
        matcher_max = FuzzyMatcher(string_similarity_threshold=1.0)
        assert matcher_max.string_similarity_threshold == 1.0


class TestCalculateStringSimilarity:
    """Test string similarity calculation."""

    def test_identical_strings(self, fuzzy_matcher):
        """Test similarity of identical strings."""
        similarity = fuzzy_matcher.calculate_string_similarity("hello", "hello")
        assert similarity == 1.0

    def test_completely_different_strings(self, fuzzy_matcher):
        """Test similarity of completely different strings."""
        similarity = fuzzy_matcher.calculate_string_similarity("hello", "xyz")
        assert 0.0 <= similarity < 0.5  # Should be low similarity

    def test_similar_strings(self, fuzzy_matcher):
        """Test similarity of similar strings."""
        similarity = fuzzy_matcher.calculate_string_similarity("Mueller", "Müller")
        assert 0.5 < similarity < 1.0  # Should be high but not perfect

    def test_empty_strings(self, fuzzy_matcher):
        """Test similarity calculation with empty strings."""
        # Both empty
        similarity = fuzzy_matcher.calculate_string_similarity("", "")
        assert similarity == 1.0

        # One empty
        similarity = fuzzy_matcher.calculate_string_similarity("hello", "")
        assert similarity == 0.0

        similarity = fuzzy_matcher.calculate_string_similarity("", "hello")
        assert similarity == 0.0

    def test_case_sensitivity(self, fuzzy_matcher):
        """Test that similarity calculation handles case differences."""
        similarity = fuzzy_matcher.calculate_string_similarity("Hello", "hello")
        assert similarity > 0.8  # Should be very similar despite case difference

    def test_return_type_and_range(self, fuzzy_matcher):
        """Test that similarity always returns float in [0.0, 1.0] range."""
        test_pairs = [
            ("test", "test"),
            ("test", "TEST"),
            ("hello", "world"),
            ("", ""),
            ("a", ""),
            ("", "b"),
            ("similar", "similiar"),  # Common typo
        ]

        for str1, str2 in test_pairs:
            similarity = fuzzy_matcher.calculate_string_similarity(str1, str2)
            assert isinstance(similarity, float)
            assert 0.0 <= similarity <= 1.0


class TestCompareNames:
    """Test name comparison functionality."""

    def test_exact_name_match(self, fuzzy_matcher):
        """Test exact name matching."""
        result = fuzzy_matcher.compare_names("FirstName", "Hans", "Hans")

        assert isinstance(result, MatchInfo)
        assert result.field_name == "FirstName"
        assert result.input_value == "Hans"
        assert result.db_value == "Hans"
        assert result.match_type == "Exact"
        assert result.similarity_score == 1.0

    def test_fuzzy_name_match(self, fuzzy_matcher):
        """Test fuzzy name matching above threshold."""
        # These should be similar enough to pass fuzzy matching
        result = fuzzy_matcher.compare_names("LastName", "Mueller", "Müller")

        assert result.match_type == "Fuzzy"
        assert result.similarity_score is not None
        assert result.similarity_score >= fuzzy_matcher.string_similarity_threshold

    def test_name_mismatch(self, fuzzy_matcher):
        """Test name matching below threshold."""
        result = fuzzy_matcher.compare_names("FirstName", "Hans", "completely_different")

        assert result.match_type == "Mismatch"
        assert result.similarity_score is not None
        assert result.similarity_score < fuzzy_matcher.string_similarity_threshold

    def test_missing_input_name(self, fuzzy_matcher):
        """Test comparison with missing input name."""
        # None input
        result = fuzzy_matcher.compare_names("FirstName", None, "Hans")
        assert result.match_type == "NotCompared"
        assert "Input name not provided" in result.details

        # Empty string input
        result = fuzzy_matcher.compare_names("FirstName", "", "Hans")
        assert result.match_type == "NotCompared"
        assert "Input name not provided" in result.details

        # Whitespace-only input
        result = fuzzy_matcher.compare_names("FirstName", "   ", "Hans")
        assert result.match_type == "NotCompared"
        assert "Input name not provided" in result.details

    def test_missing_db_name(self, fuzzy_matcher):
        """Test comparison with missing database name."""
        # None database value
        result = fuzzy_matcher.compare_names("FirstName", "Hans", None)
        assert result.match_type == "MissingDBValue"
        assert "DB name not provided" in result.details

        # Empty string database value
        result = fuzzy_matcher.compare_names("FirstName", "Hans", "")
        assert result.match_type == "MissingDBValue"
        assert "DB name not provided" in result.details

    def test_case_insensitive_matching(self, fuzzy_matcher):
        """Test that name matching is case insensitive."""
        result = fuzzy_matcher.compare_names("FirstName", "HANS", "hans")
        assert result.match_type == "Exact"
        assert result.similarity_score == 1.0

    def test_whitespace_handling(self, fuzzy_matcher):
        """Test handling of whitespace in names."""
        result = fuzzy_matcher.compare_names("FirstName", "  Hans  ", " Hans ")
        assert result.match_type == "Exact"
        assert result.similarity_score == 1.0

    def test_different_thresholds(self):
        """Test behavior with different similarity thresholds."""
        high_threshold_matcher = FuzzyMatcher(string_similarity_threshold=0.95)
        low_threshold_matcher = FuzzyMatcher(string_similarity_threshold=0.3)

        # Test case that might pass low threshold but fail high threshold
        input_name = "Smith"
        db_name = "Smyth"

        high_result = high_threshold_matcher.compare_names("LastName", input_name, db_name)
        low_result = low_threshold_matcher.compare_names("LastName", input_name, db_name)

        # With different thresholds, results might differ
        assert high_result.match_type in ["Fuzzy", "Mismatch"]
        assert low_result.match_type in ["Fuzzy", "Mismatch"]

        # But similarity scores should be the same
        assert high_result.similarity_score == low_result.similarity_score


class TestCompareDates:
    """Test date comparison functionality."""

    def test_exact_date_match(self, fuzzy_matcher):
        """Test exact date matching."""
        test_date = date(1980, 5, 15)
        result = fuzzy_matcher.compare_dates(test_date, test_date)

        assert result.field_name == "DOB"
        assert result.input_value == test_date
        assert result.db_value == test_date
        assert result.match_type == "Exact"
        assert result.similarity_score == 1.0

    def test_year_mismatch_within_tolerance(self, fuzzy_matcher):
        """Test year mismatch within tolerance."""
        input_date = date(1980, 5, 15)
        db_date = date(1981, 5, 15)  # Same month/day, 1 year difference

        result = fuzzy_matcher.compare_dates(input_date, db_date)

        assert result.match_type == "YearMismatch"
        assert result.similarity_score == 0.7
        assert "Year diff: 1" in result.details

    def test_year_mismatch_beyond_tolerance(self, fuzzy_matcher):
        """Test year mismatch beyond tolerance."""
        input_date = date(1980, 5, 15)
        db_date = date(1985, 5, 15)  # Same month/day, 5 years difference

        result = fuzzy_matcher.compare_dates(input_date, db_date)

        assert result.match_type == "Mismatch"
        assert result.similarity_score == 0.0

    def test_different_month_day(self, fuzzy_matcher):
        """Test dates with different month/day but same year."""
        input_date = date(1980, 5, 15)
        db_date = date(1980, 6, 20)  # Same year, different month/day

        result = fuzzy_matcher.compare_dates(input_date, db_date)

        assert result.match_type == "Mismatch"
        assert result.similarity_score == 0.0

    def test_missing_input_date(self, fuzzy_matcher):
        """Test comparison with missing input date."""
        db_date = date(1980, 5, 15)
        result = fuzzy_matcher.compare_dates(None, db_date)

        assert result.match_type == "NotCompared"
        assert "Input DOB not provided" in result.details
        assert result.input_value is None
        assert result.db_value == db_date

    def test_missing_db_date(self, fuzzy_matcher):
        """Test comparison with missing database date."""
        input_date = date(1980, 5, 15)
        result = fuzzy_matcher.compare_dates(input_date, None)

        assert result.match_type == "MissingDBValue"
        assert "DB DOB not provided" in result.details
        assert result.input_value == input_date
        assert result.db_value is None

    def test_both_dates_missing(self, fuzzy_matcher):
        """Test comparison with both dates missing."""
        result = fuzzy_matcher.compare_dates(None, None)

        assert result.match_type == "NotCompared"
        assert "Input DOB not provided" in result.details

    def test_year_tolerance_configuration(self):
        """Test different year tolerance settings."""
        strict_matcher = FuzzyMatcher(date_year_tolerance=0)
        lenient_matcher = FuzzyMatcher(date_year_tolerance=3)

        input_date = date(1980, 5, 15)
        db_date = date(1982, 5, 15)  # 2 years difference

        strict_result = strict_matcher.compare_dates(input_date, db_date)
        lenient_result = lenient_matcher.compare_dates(input_date, db_date)

        # Strict should be mismatch (tolerance=0, diff=2)
        assert strict_result.match_type == "Mismatch"

        # Lenient should be year mismatch (tolerance=3, diff=2)
        assert lenient_result.match_type == "YearMismatch"
        assert "Year diff: 2" in lenient_result.details

    def test_edge_case_leap_year(self, fuzzy_matcher):
        """Test leap year edge cases."""
        # February 29 in leap year vs non-leap year
        leap_date = date(2000, 2, 29)  # Leap year
        non_leap_date = date(2001, 2, 28)  # Non-leap year, closest date

        result = fuzzy_matcher.compare_dates(leap_date, non_leap_date)

        # Should be mismatch since day is different
        assert result.match_type == "Mismatch"

    def test_year_mismatch_boundary(self, fuzzy_matcher):
        """Test year mismatch at exact tolerance boundary."""
        input_date = date(1980, 5, 15)

        # Exactly at tolerance (1 year)
        db_date_at_tolerance = date(1981, 5, 15)
        result_at = fuzzy_matcher.compare_dates(input_date, db_date_at_tolerance)
        assert result_at.match_type == "YearMismatch"

        # Just beyond tolerance (2 years)
        db_date_beyond = date(1982, 5, 15)
        result_beyond = fuzzy_matcher.compare_dates(input_date, db_date_beyond)
        assert result_beyond.match_type == "Mismatch"


@pytest.mark.unit
class TestFuzzyMatcherIntegration:
    """Integration tests for FuzzyMatcher functionality."""

    def test_realistic_name_scenarios(self, fuzzy_matcher):
        """Test realistic name matching scenarios."""
        test_cases = [
            # (input, db_value, expected_match_type)
            ("Smith", "Smith", "Exact"),
            ("Smith", "Smyth", "Fuzzy"),  # Common variant
            ("McDonald", "MacDonald", "Fuzzy"),  # Mc vs Mac
            ("O'Connor", "OConnor", "Fuzzy"),  # Apostrophe variants
            ("Müller", "Mueller", "Fuzzy"),  # Umlaut variants
            ("Jones", "Jackson", "Mismatch"),  # Completely different
        ]

        for input_name, db_name, expected_type in test_cases:
            result = fuzzy_matcher.compare_names("LastName", input_name, db_name)
            assert result.match_type == expected_type, f"Failed for {input_name} vs {db_name}"

    def test_realistic_date_scenarios(self, fuzzy_matcher):
        """Test realistic date matching scenarios."""
        base_date = date(1980, 5, 15)

        test_cases = [
            # (input_date, db_date, expected_match_type)
            (base_date, base_date, "Exact"),
            (base_date, date(1981, 5, 15), "YearMismatch"),  # 1 year off
            (base_date, date(1980, 5, 16), "Mismatch"),  # Different day
            (base_date, date(1980, 6, 15), "Mismatch"),  # Different month
            (base_date, date(1975, 5, 15), "Mismatch"),  # Too many years off
        ]

        for input_date, db_date, expected_type in test_cases:
            result = fuzzy_matcher.compare_dates(input_date, db_date)
            assert result.match_type == expected_type, f"Failed for {input_date} vs {db_date}"

    def test_match_info_consistency(self, fuzzy_matcher):
        """Test that MatchInfo objects are consistently structured."""
        # Test name comparison
        name_result = fuzzy_matcher.compare_names("FirstName", "Test", "Test")
        assert hasattr(name_result, "field_name")
        assert hasattr(name_result, "input_value")
        assert hasattr(name_result, "db_value")
        assert hasattr(name_result, "match_type")
        assert hasattr(name_result, "similarity_score")

        # Test date comparison
        test_date = date(1980, 1, 1)
        date_result = fuzzy_matcher.compare_dates(test_date, test_date)
        assert hasattr(date_result, "field_name")
        assert hasattr(date_result, "input_value")
        assert hasattr(date_result, "db_value")
        assert hasattr(date_result, "match_type")
        assert hasattr(date_result, "similarity_score")

        # Field name should be consistent for dates
        assert date_result.field_name == "DOB"
