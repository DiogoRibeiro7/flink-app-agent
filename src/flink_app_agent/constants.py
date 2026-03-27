"""Small shared constants and exceptions used across the local generation pipeline."""

GENERATION_REPORT_FILENAME = "generation_report.json"


class ProviderExtractionError(ValueError):
    """Raised when a provider-backed extraction call fails or returns invalid output."""
