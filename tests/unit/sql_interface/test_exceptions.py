"""Unit tests for tbase_extractor.sql_interface.exceptions module."""

import pytest

from tbase_extractor.sql_interface.exceptions import (
    DatabaseConnectionError,
    InvalidQueryParametersError,
    QueryExecutionError,
    QueryTemplateNotFoundError,
)


class TestQueryTemplateNotFoundError:
    """Test QueryTemplateNotFoundError exception."""

    def test_basic_exception(self):
        """Test basic exception creation and properties."""
        error_message = "Template not found: test.sql"
        exception = QueryTemplateNotFoundError(error_message)

        assert str(exception) == error_message
        assert isinstance(exception, Exception)

    def test_exception_inheritance(self):
        """Test that exception inherits from Exception."""
        exception = QueryTemplateNotFoundError("test")
        assert isinstance(exception, Exception)

    def test_exception_can_be_raised(self):
        """Test that exception can be raised and caught."""
        with pytest.raises(QueryTemplateNotFoundError) as exc_info:
            raise QueryTemplateNotFoundError("Test template error")

        assert "Test template error" in str(exc_info.value)

    def test_exception_with_empty_message(self):
        """Test exception with empty message."""
        exception = QueryTemplateNotFoundError("")
        assert str(exception) == ""

    def test_exception_with_none_message(self):
        """Test exception with None message."""
        exception = QueryTemplateNotFoundError(None)
        assert str(exception) == "None"


class TestDatabaseConnectionError:
    """Test DatabaseConnectionError exception."""

    def test_basic_exception(self):
        """Test basic exception creation and properties."""
        error_message = "Failed to connect to database"
        exception = DatabaseConnectionError(error_message)

        assert str(exception) == error_message
        assert isinstance(exception, Exception)

    def test_exception_inheritance(self):
        """Test that exception inherits from Exception."""
        exception = DatabaseConnectionError("test")
        assert isinstance(exception, Exception)

    def test_exception_can_be_raised(self):
        """Test that exception can be raised and caught."""
        with pytest.raises(DatabaseConnectionError) as exc_info:
            raise DatabaseConnectionError("Connection timeout")

        assert "Connection timeout" in str(exc_info.value)

    def test_exception_with_detailed_message(self):
        """Test exception with detailed error message."""
        error_details = "Failed to connect to SQL Server: timeout expired"
        exception = DatabaseConnectionError(error_details)

        assert str(exception) == error_details
        assert "SQL Server" in str(exception)
        assert "timeout" in str(exception)


class TestQueryExecutionError:
    """Test QueryExecutionError exception."""

    def test_basic_exception(self):
        """Test basic exception creation and properties."""
        error_message = "Query execution failed"
        exception = QueryExecutionError(error_message)

        assert str(exception) == error_message
        assert isinstance(exception, Exception)

    def test_exception_inheritance(self):
        """Test that exception inherits from Exception."""
        exception = QueryExecutionError("test")
        assert isinstance(exception, Exception)

    def test_exception_can_be_raised(self):
        """Test that exception can be raised and caught."""
        with pytest.raises(QueryExecutionError) as exc_info:
            raise QueryExecutionError("Syntax error in SQL")

        assert "Syntax error in SQL" in str(exc_info.value)

    def test_exception_with_sql_error_details(self):
        """Test exception with SQL error details."""
        sql_error = "SQLSTATE 42000: Syntax error near 'FROM'"
        exception = QueryExecutionError(sql_error)

        assert str(exception) == sql_error
        assert "SQLSTATE" in str(exception)
        assert "42000" in str(exception)


class TestInvalidQueryParametersError:
    """Test InvalidQueryParametersError exception."""

    def test_basic_exception(self):
        """Test basic exception creation and properties."""
        error_message = "Invalid parameters provided"
        exception = InvalidQueryParametersError(error_message)

        assert str(exception) == error_message
        assert isinstance(exception, Exception)

    def test_exception_inheritance(self):
        """Test that exception inherits from Exception."""
        exception = InvalidQueryParametersError("test")
        assert isinstance(exception, Exception)

    def test_exception_can_be_raised(self):
        """Test that exception can be raised and caught."""
        with pytest.raises(InvalidQueryParametersError) as exc_info:
            raise InvalidQueryParametersError("Parameter count mismatch")

        assert "Parameter count mismatch" in str(exc_info.value)

    def test_exception_with_parameter_details(self):
        """Test exception with parameter error details."""
        param_error = "Expected 3 parameters, got 2: missing patient_id"
        exception = InvalidQueryParametersError(param_error)

        assert str(exception) == param_error
        assert "Expected 3" in str(exception)
        assert "patient_id" in str(exception)


@pytest.mark.unit
class TestExceptionsIntegration:
    """Integration tests for SQL interface exceptions."""

    def test_all_exceptions_are_exceptions(self):
        """Test that all custom exceptions inherit from Exception."""
        exceptions = [
            QueryTemplateNotFoundError("test"),
            DatabaseConnectionError("test"),
            QueryExecutionError("test"),
            InvalidQueryParametersError("test"),
        ]

        for exc in exceptions:
            assert isinstance(exc, Exception)

    def test_exceptions_can_be_caught_generically(self):
        """Test that custom exceptions can be caught as generic Exception."""
        test_exceptions = [
            (QueryTemplateNotFoundError, "Template error"),
            (DatabaseConnectionError, "Connection error"),
            (QueryExecutionError, "Execution error"),
            (InvalidQueryParametersError, "Parameter error"),
        ]

        for exception_class, message in test_exceptions:
            with pytest.raises(Exception) as exc_info:
                raise exception_class(message)

            assert isinstance(exc_info.value, exception_class)
            assert message in str(exc_info.value)

    def test_exceptions_with_chaining(self):
        """Test exception chaining scenarios."""
        # Simulate exception chaining that might occur in real usage
        original_error = ConnectionError("Network unreachable")

        with pytest.raises(DatabaseConnectionError) as exc_info:
            try:
                raise original_error
            except ConnectionError as e:
                raise DatabaseConnectionError(f"Database connection failed: {e}") from e

        assert "Database connection failed" in str(exc_info.value)
        assert "Network unreachable" in str(exc_info.value)
        assert exc_info.value.__cause__ == original_error

    def test_realistic_error_scenarios(self):
        """Test realistic error scenarios that might occur in application."""
        # Template not found scenario
        with pytest.raises(QueryTemplateNotFoundError):
            raise QueryTemplateNotFoundError("SQL template file not found: /templates/missing_query.sql")

        # Database connection scenario
        with pytest.raises(DatabaseConnectionError):
            raise DatabaseConnectionError("Unable to connect to SQL Server: Login failed for user 'testuser'")

        # Query execution scenario
        with pytest.raises(QueryExecutionError):
            raise QueryExecutionError("Error executing query: SQLSTATE 42S02 - Invalid object name 'NonexistentTable'")

        # Invalid parameters scenario
        with pytest.raises(InvalidQueryParametersError):
            raise InvalidQueryParametersError(
                "Parameter validation failed: patient_id must be a positive integer, got 'invalid'",
            )

    def test_exception_messages_are_preserved(self):
        """Test that exception messages are properly preserved."""
        test_cases = [
            (QueryTemplateNotFoundError, "Custom template message"),
            (DatabaseConnectionError, "Custom connection message"),
            (QueryExecutionError, "Custom execution message"),
            (InvalidQueryParametersError, "Custom parameters message"),
        ]

        for exception_class, original_message in test_cases:
            try:
                raise exception_class(original_message)
            except exception_class as e:
                assert str(e) == original_message

    def test_exceptions_work_with_logging(self):
        """Test that exceptions work properly with logging."""
        from unittest.mock import Mock

        mock_logger = Mock()

        # Test logging different exception types
        exceptions_to_test = [
            QueryTemplateNotFoundError("Template logging test"),
            DatabaseConnectionError("Connection logging test"),
            QueryExecutionError("Execution logging test"),
            InvalidQueryParametersError("Parameters logging test"),
        ]

        for exc in exceptions_to_test:
            try:
                raise exc
            except Exception as e:
                mock_logger.error(f"Exception occurred: {e}")

        # Verify all exceptions were logged
        assert mock_logger.error.call_count == 4

        # Verify logging content
        call_args = [call[0][0] for call in mock_logger.error.call_args_list]
        assert any("Template logging test" in arg for arg in call_args)
        assert any("Connection logging test" in arg for arg in call_args)
        assert any("Execution logging test" in arg for arg in call_args)
        assert any("Parameters logging test" in arg for arg in call_args)
