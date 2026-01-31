"""Custom exceptions for the prefect_cwl package."""

class ValidationError(Exception):
    """Raised when user-provided CWL or runtime values are invalid."""
    pass