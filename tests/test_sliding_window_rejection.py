"""Regression coverage for unsupported sliding-window requests."""

from __future__ import annotations

import pytest

from flink_app_agent.llm import build_default_extraction_service, build_provider_extraction_service
from flink_app_agent.request_taxonomy import UnsupportedRequestError


REQUEST = (
    "Read from Kafka events, group by user_id, count events using sliding windows, "
    "write to Kafka aggregated-events"
)


def test_deterministic_extraction_rejects_sliding_windows() -> None:
    """Sliding-window wording must not silently select the tumbling template."""
    with pytest.raises(UnsupportedRequestError, match="sliding windows are not supported"):
        build_default_extraction_service().extract(REQUEST)


def test_provider_extraction_rejects_sliding_windows_before_provider_call() -> None:
    """The shared guard should reject sliding windows before provider interpretation."""
    provider_called = False

    def call_provider(request: str, prompt: str) -> str:
        nonlocal provider_called
        provider_called = True
        return "{}"

    with pytest.raises(UnsupportedRequestError, match="sliding windows are not supported"):
        build_provider_extraction_service(call_provider).extract(REQUEST)

    assert provider_called is False
