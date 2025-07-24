"""Unit tests for tbase_extractor.config module."""

import pytest

from tbase_extractor.config import (
    APP_VERSION,
    DEFAULT_DOB_COLUMN,
    DEFAULT_FN_COLUMN,
    DEFAULT_ID_COLUMN,
    DEFAULT_LN_COLUMN,
    DOB_FORMAT,
    FILE_EXTENSION_MAP,
    VALID_FILENAME_CHARS,
    VALID_OUTPUT_FORMATS,
    get_env_or_default,
    sanitize_filename,
)


class TestConfigConstants:
    """Test configuration constants are properly defined."""

    def test_app_version_format(self):
        """Test that APP_VERSION follows semantic versioning."""
        assert isinstance(APP_VERSION, str)
        assert len(APP_VERSION.split(".")) >= 2  # At least major.minor

    def test_default_columns(self):
        """Test default column names are strings."""
        assert isinstance(DEFAULT_ID_COLUMN, str)
        assert isinstance(DEFAULT_FN_COLUMN, str)
        assert isinstance(DEFAULT_LN_COLUMN, str)
        assert isinstance(DEFAULT_DOB_COLUMN, str)

        # Check they're not empty
        assert len(DEFAULT_ID_COLUMN) > 0
        assert len(DEFAULT_FN_COLUMN) > 0
        assert len(DEFAULT_LN_COLUMN) > 0
        assert len(DEFAULT_DOB_COLUMN) > 0

    def test_dob_format(self):
        """Test DOB format is valid strftime format."""
        assert isinstance(DOB_FORMAT, str)
        assert "%Y" in DOB_FORMAT  # Should include year
        assert "%m" in DOB_FORMAT  # Should include month
        assert "%d" in DOB_FORMAT  # Should include day

    def test_valid_output_formats(self):
        """Test output formats list is properly defined."""
        assert isinstance(VALID_OUTPUT_FORMATS, list)
        assert len(VALID_OUTPUT_FORMATS) > 0
        assert "json" in VALID_OUTPUT_FORMATS
        assert "csv" in VALID_OUTPUT_FORMATS
        assert "stdout" in VALID_OUTPUT_FORMATS

    def test_file_extension_map(self):
        """Test file extension mapping is consistent."""
        assert isinstance(FILE_EXTENSION_MAP, dict)

        # Check that all mapped formats are in valid formats
        for ext, format_name in FILE_EXTENSION_MAP.items():
            assert format_name in VALID_OUTPUT_FORMATS
            assert ext.startswith(".")

    def test_valid_filename_chars(self):
        """Test valid filename characters set."""
        assert isinstance(VALID_FILENAME_CHARS, set)
        assert len(VALID_FILENAME_CHARS) > 0

        # Should include alphanumeric
        assert "a" in VALID_FILENAME_CHARS
        assert "A" in VALID_FILENAME_CHARS
        assert "0" in VALID_FILENAME_CHARS
        assert "9" in VALID_FILENAME_CHARS

        # Should include safe special chars
        assert "-" in VALID_FILENAME_CHARS
        assert "_" in VALID_FILENAME_CHARS

        # Should NOT include unsafe chars
        assert "/" not in VALID_FILENAME_CHARS
        assert "\\" not in VALID_FILENAME_CHARS
        assert "<" not in VALID_FILENAME_CHARS
        assert ">" not in VALID_FILENAME_CHARS


class TestGetEnvOrDefault:
    """Test get_env_or_default function."""

    def test_existing_env_var(self, monkeypatch):
        """Test retrieval of existing environment variable."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        result = get_env_or_default("TEST_VAR", "default")
        assert result == "test_value"

    def test_missing_env_var_with_default(self, monkeypatch):
        """Test default value when env var doesn't exist."""
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        result = get_env_or_default("NONEXISTENT_VAR", "default_value")
        assert result == "default_value"

    def test_missing_env_var_no_default(self, monkeypatch):
        """Test empty string when no env var and no default."""
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        result = get_env_or_default("NONEXISTENT_VAR")
        assert result == ""

    def test_empty_env_var(self, monkeypatch):
        """Test behavior with empty environment variable."""
        monkeypatch.setenv("EMPTY_VAR", "")
        result = get_env_or_default("EMPTY_VAR", "default")
        assert result == ""  # Empty string should be returned, not default


class TestSanitizeFilename:
    """Test sanitize_filename function."""

    def test_clean_filename(self):
        """Test filename that doesn't need sanitization."""
        clean_name = "valid_filename-123"
        result = sanitize_filename(clean_name)
        assert result == clean_name

    def test_filename_with_spaces(self):
        """Test filename with spaces (should be removed)."""
        dirty_name = "file name with spaces"
        result = sanitize_filename(dirty_name)
        assert result == "filenamewithspaces"
        assert " " not in result

    def test_filename_with_special_chars(self):
        """Test filename with various special characters."""
        dirty_name = "file<>name|with:many*special?chars"
        result = sanitize_filename(dirty_name)
        assert result == "filenamewithmanyspecialchars"

        # Ensure no unsafe characters remain
        unsafe_chars = '<>|:*?/"\\'
        for char in unsafe_chars:
            assert char not in result

    def test_filename_with_unicode(self):
        """Test filename with unicode characters."""
        dirty_name = "file_ñämé_with_ümläuts"
        result = sanitize_filename(dirty_name)
        # Should keep underscores and alphanumeric ASCII
        assert "file" in result
        assert "_" in result
        # Unicode should be removed
        assert "ñ" not in result
        assert "ä" not in result
        assert "ü" not in result

    def test_empty_filename(self):
        """Test empty filename."""
        result = sanitize_filename("")
        assert result == ""

    def test_filename_only_invalid_chars(self):
        """Test filename with only invalid characters."""
        dirty_name = '<>|:*?/"\\'
        result = sanitize_filename(dirty_name)
        assert result == ""

    def test_filename_preservation(self):
        """Test that valid characters are preserved correctly."""
        test_cases = [
            ("test123", "test123"),
            ("test-file", "test-file"),
            ("test_file", "test_file"),
            ("TestFile", "TestFile"),
            ("file.txt", "filetxt"),  # Dots are removed
            ("123", "123"),
            ("ABC-123_test", "ABC-123_test"),
        ]

        for input_name, expected in test_cases:
            result = sanitize_filename(input_name)
            assert result == expected, f"Failed for input: {input_name}"


@pytest.mark.unit
class TestConfigIntegration:
    """Integration tests for config module functionality."""

    def test_config_values_consistency(self):
        """Test that configuration values are internally consistent."""
        # All file extensions should map to valid output formats
        for _ext, format_name in FILE_EXTENSION_MAP.items():
            assert format_name in VALID_OUTPUT_FORMATS

        # Default columns should be valid identifiers
        defaults = [DEFAULT_ID_COLUMN, DEFAULT_FN_COLUMN, DEFAULT_LN_COLUMN, DEFAULT_DOB_COLUMN]
        for default in defaults:
            assert default.isidentifier() or default.replace("_", "a").isidentifier()

    def test_sanitize_preserves_valid_filenames(self):
        """Test that sanitize_filename preserves already valid filenames."""
        valid_examples = [
            "patient_123",
            "report-2023",
            "DataExport_Final",
            "ABC123def",
        ]

        for filename in valid_examples:
            result = sanitize_filename(filename)
            assert result == filename

            # Verify result only contains valid chars
            for char in result:
                assert char in VALID_FILENAME_CHARS
