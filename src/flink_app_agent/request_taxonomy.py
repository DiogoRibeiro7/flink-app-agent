"""Small shared request taxonomy for the pipeline."""

from __future__ import annotations


REQUEST_CATEGORY_SUPPORTED = "supported"
REQUEST_CATEGORY_INVALID = "invalid"
REQUEST_CATEGORY_AMBIGUOUS = "ambiguous"
REQUEST_CATEGORY_UNSUPPORTED = "unsupported"


class UnsupportedRequestError(ValueError):
    """Raised when a request is understandable but outside current feature scope."""

