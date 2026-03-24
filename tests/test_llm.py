"""Tests for prompt loading and deterministic spec extraction."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import (
    FilePromptRepository,
    SpecExtractionService,
    SpecParsingError,
    StubSpecExtractor,
    load_prompt,
)


def test_load_prompt_reads_prompt_file() -> None:
    """Prompt loading should return the prompt text from the package directory."""
    prompt = load_prompt("extract_spec.md")

    assert "FlinkJobSpec" in prompt
    assert "two_events_within_window" in prompt


def test_stub_parser_returns_valid_spec_for_supported_request() -> None:
    """The stub parser should build a validated spec for the supported pattern."""
    extractor = StubSpecExtractor()
    spec = extractor.extract_spec(
        (
            "Build a Kafka job named fraud alerts with source topic payments, "
            "sink topic alerts, key by account_id, event time field event_time, "
            "and emit SuspiciousTransfer within 15 minutes."
        )
    )

    assert spec.job_name == "fraud-alerts"
    assert spec.source_topic == "payments"
    assert spec.sink_topic == "alerts"
    assert spec.key_by == "account_id"
    assert spec.event_time_field == "event_time"
    assert spec.output_event_name == "SuspiciousTransfer"
    assert spec.time_window_minutes == 15


def test_stub_parser_rejects_unsupported_request() -> None:
    """Requests outside the restricted pattern should fail clearly."""
    extractor = StubSpecExtractor()

    with pytest.raises(SpecParsingError, match="Supported requests must mention"):
        extractor.extract_spec("Create a streaming job that sends alerts quickly.")


def test_extraction_service_coordinates_prompt_loading_and_parsing() -> None:
    """The service should combine the prompt repository and extractor cleanly."""
    service = SpecExtractionService(
        extractor=StubSpecExtractor(),
        prompt_repository=FilePromptRepository(),
    )

    spec = service.extract(
        "Build a Kafka job with source topic payments, sink topic alerts, key by account_id, emit AlertRaised within 5 minutes."
    )

    assert spec.output_event_name == "AlertRaised"
    assert spec.time_window_minutes == 5
