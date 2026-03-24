"""Tests for prompt loading and deterministic spec extraction."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import (
    FilePromptRepository,
    OpenAISpecPayloadExtractor,
    PydanticSpecValidator,
    ServiceBackedSpecExtractor,
    SimpleRequestPreprocessor,
    SpecExtractionService,
    SpecParsingError,
    StubSpecExtractor,
    StubSpecPayloadExtractor,
    build_default_extraction_service,
    build_default_spec_extractor,
    load_prompt,
)


def test_load_prompt_reads_prompt_file() -> None:
    """Prompt loading should return the prompt text from the package directory."""
    prompt = load_prompt("extract_spec.md")

    assert "FlinkJobSpec" in prompt
    assert "two_events_within_window" in prompt


def test_request_preprocessor_normalizes_whitespace() -> None:
    """Request preprocessing should normalize repeated whitespace only."""
    preprocessor = SimpleRequestPreprocessor()

    assert preprocessor.preprocess(" Read   from Kafka   topic sensor-events ") == "Read from Kafka topic sensor-events"


@pytest.mark.parametrize(
    ("user_request", "expected"),
    [
        (
            (
                "Build a Kafka job named fraud alerts with source topic payments, "
                "sink topic alerts, key by account_id, event time field event_time, "
                "and emit SuspiciousTransfer within 15 minutes."
            ),
            {
                "job_name": "fraud-alerts",
                "source_topic": "payments",
                "sink_topic": "alerts",
                "key_by": "account_id",
                "event_time_field": "event_time",
                "output_event_name": "SuspiciousTransfer",
                "time_window_minutes": 15,
            },
        ),
        (
            "Read from Kafka topic sensor-events and publish BED_OUT events to inferred-events, key by user_id, within 20 minutes.",
            {
                "job_name": "bedout-job",
                "source_topic": "sensor-events",
                "sink_topic": "inferred-events",
                "key_by": "user_id",
                "event_time_field": "event_time",
                "output_event_name": "BedOut",
                "time_window_minutes": 20,
            },
        ),
        (
            "Consume sensor-events, key by user_id, emit BED_OUT within 20 minutes.",
            {
                "job_name": "bedout-job",
                "source_topic": "sensor-events",
                "sink_topic": "output-topic",
                "key_by": "user_id",
                "event_time_field": "event_time",
                "output_event_name": "BedOut",
                "time_window_minutes": 20,
            },
        ),
        (
            "Build a Flink job that reads sensor-events, groups by user_id, and writes BED_OUT to inferred-events within 20 minutes.",
            {
                "job_name": "bedout-job",
                "source_topic": "sensor-events",
                "sink_topic": "inferred-events",
                "key_by": "user_id",
                "event_time_field": "event_time",
                "output_event_name": "BedOut",
                "time_window_minutes": 20,
            },
        ),
        (
            "Read telemetry-events, keyed by device_id, use observed_at as event time, and emit TEMP_SPIKE to device-alerts within a 10 minute window.",
            {
                "job_name": "tempspike-job",
                "source_topic": "telemetry-events",
                "sink_topic": "device-alerts",
                "key_by": "device_id",
                "event_time_field": "observed_at",
                "output_event_name": "TempSpike",
                "time_window_minutes": 10,
            },
        ),
    ],
)
def test_stub_parser_returns_valid_spec_for_supported_variants(
    user_request: str,
    expected: dict[str, str | int],
) -> None:
    """The stub parser should build deterministic specs for supported variants."""
    service = SpecExtractionService(
        preprocessor=SimpleRequestPreprocessor(),
        payload_extractor=StubSpecPayloadExtractor(),
        validator=PydanticSpecValidator(),
        prompt_repository=FilePromptRepository(),
    )
    extractor = ServiceBackedSpecExtractor(extraction_service=service)
    first_spec = extractor.extract_spec(user_request)
    second_spec = extractor.extract_spec(user_request)

    assert first_spec.model_dump()["job_name"] == expected["job_name"]
    assert first_spec.model_dump()["source_topic"] == expected["source_topic"]
    assert first_spec.model_dump()["sink_topic"] == expected["sink_topic"]
    assert first_spec.model_dump()["key_by"] == expected["key_by"]
    assert first_spec.model_dump()["event_time_field"] == expected["event_time_field"]
    assert first_spec.model_dump()["output_event_name"] == expected["output_event_name"]
    assert first_spec.model_dump()["time_window_minutes"] == expected["time_window_minutes"]
    assert first_spec.model_dump() == second_spec.model_dump()


@pytest.mark.parametrize(
    ("user_request", "message"),
    [
        (
            "Read from Kafka topic sensor-events and publish BED_OUT events to inferred-events within 20 minutes.",
            "key_by",
        ),
        (
            "Consume sensor-events, key by user_id, and publish to inferred-events within 20 minutes.",
            "output_event_name",
        ),
        (
            "Build a Flink job that groups by user_id and emits BED_OUT within 20 minutes.",
            "source_topic",
        ),
    ],
)
def test_stub_parser_rejects_invalid_requests(user_request: str, message: str) -> None:
    """Requests missing essential fields should fail with clear parsing errors."""
    extractor = build_default_spec_extractor()

    with pytest.raises(SpecParsingError, match=message):
        extractor.extract_spec(user_request)


def test_extraction_service_coordinates_prompt_loading_and_parsing() -> None:
    """The service should combine the prompt repository and extractor cleanly."""
    spec = build_default_extraction_service().extract(
        "Build a Kafka job with source topic payments, sink topic alerts, key by account_id, emit AlertRaised within 5 minutes."
    )

    assert spec.output_event_name == "AlertRaised"
    assert spec.time_window_minutes == 5


def test_default_extractor_is_service_backed_stub() -> None:
    """The current default extractor should remain the stub-backed implementation."""
    extractor = build_default_spec_extractor()

    assert isinstance(extractor, StubSpecExtractor)
    assert isinstance(extractor.extraction_service.payload_extractor, StubSpecPayloadExtractor)


def test_openai_adapter_is_placeholder_only() -> None:
    """The future provider adapter should not make real calls yet."""
    adapter = OpenAISpecPayloadExtractor()

    with pytest.raises(NotImplementedError, match="not implemented yet"):
        adapter.extract_payload("request", "prompt")
