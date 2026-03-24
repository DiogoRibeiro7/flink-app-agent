"""Tests for the v0.1 deterministic extractor and prompt loading."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import SpecParsingError, StubSpecExtractor, load_prompt


def test_load_prompt_reads_extract_prompt() -> None:
    """Prompt loading should return the local extraction prompt text."""
    prompt = load_prompt("extract_spec.md")

    assert "FlinkJobSpec" in prompt
    assert "two_events_within_window" in prompt


def test_stub_extractor_parses_supported_v0_request() -> None:
    """The stub should parse the single supported v0.1 request shape."""
    spec = StubSpecExtractor().extract_spec(
        "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes"
    )

    assert spec.model_dump() == {
        "job_name": "bedout-job",
        "source_topic": "sensor-events",
        "sink_topic": "inferred-events",
        "key_by": "user_id",
        "event_time_field": "ts",
        "input_event_name": "InputEvent",
        "output_event_name": "BedOut",
        "rule_type": "two_events_within_window",
        "rule_condition": "emit BedOut when two keyed events match within 20 minutes",
        "time_window_minutes": 20,
    }


def test_stub_extractor_rejects_unsupported_request() -> None:
    """Requests outside the narrow v0.1 wording should fail clearly."""
    extractor = StubSpecExtractor()

    with pytest.raises(SpecParsingError, match="source_topic"):
        extractor.extract_spec("Consume sensor-events, key by user_id, emit BED_OUT within 20 minutes")
