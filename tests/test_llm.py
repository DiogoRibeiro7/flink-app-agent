"""Tests for the deterministic extractor and prompt loading."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import (
    DeterministicSpecPayloadExtractor,
    FilePromptRepository,
    OpenAISpecPayloadExtractor,
    PydanticSpecValidator,
    ServiceBackedSpecExtractor,
    SimpleRequestPreprocessor,
    SpecExtractionService,
    SpecParsingError,
    StubSpecExtractor,
    build_default_spec_extractor,
    load_prompt,
)


def test_load_prompt_reads_extract_prompt() -> None:
    """Prompt loading should return the local extraction prompt text."""
    prompt = load_prompt("extract_spec.md")

    assert "FlinkJobSpec" in prompt
    assert "two_events_within_window" in prompt


def test_request_preprocessor_normalizes_whitespace() -> None:
    """Request preprocessing should normalize repeated whitespace only."""
    preprocessor = SimpleRequestPreprocessor()

    assert preprocessor.preprocess(" Read   from Kafka   sensor-events ") == "Read from Kafka sensor-events"


@pytest.mark.parametrize(
    ("user_request", "expected"),
    [
        (
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes",
            {
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
            },
        ),
        (
            "Consume sensor-events from Kafka, key by user_id, emit BED_OUT events within 20 minutes",
            {
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
            },
        ),
        (
            "Build a Flink job reading sensor-events, keying by user_id, and writing BED_OUT within 20 minutes",
            {
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
            },
        ),
        (
            "Read topic sensor-events, group by user_id, emit BED_OUT within 20 minutes",
            {
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
            },
        ),
        (
            "Read from Kafka device-events, keyed by device_id, emit TEMP_SPIKE within 15 minutes",
            {
                "job_name": "tempspike-job",
                "source_topic": "device-events",
                "sink_topic": "inferred-events",
                "key_by": "device_id",
                "event_time_field": "ts",
                "input_event_name": "InputEvent",
                "output_event_name": "TempSpike",
                "rule_type": "two_events_within_window",
                "rule_condition": "emit TempSpike when two keyed events match within 15 minutes",
                "time_window_minutes": 15,
            },
        ),
    ],
)
def test_stub_extractor_parses_supported_request_variants(
    user_request: str,
    expected: dict[str, str | int],
) -> None:
    """The stub should parse several supported deterministic request variants."""
    extractor = StubSpecExtractor()

    first_spec = extractor.extract_spec(user_request)
    second_spec = extractor.extract_spec(user_request)

    assert first_spec.model_dump() == expected
    assert first_spec.model_dump() == second_spec.model_dump()


@pytest.mark.parametrize(
    ("user_request", "message"),
    [
        (
            "Key by user_id, emit BED_OUT within 20 minutes",
            "source_topic",
        ),
        (
            "Read from Kafka sensor-events, emit BED_OUT within 20 minutes",
            "key_by",
        ),
        (
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT",
            "time_window_minutes",
        ),
    ],
)
def test_stub_extractor_rejects_invalid_requests(user_request: str, message: str) -> None:
    """Requests missing essential fields should fail with precise parsing errors."""
    extractor = StubSpecExtractor()

    with pytest.raises(SpecParsingError, match=message):
        extractor.extract_spec(user_request)


def test_service_backed_extractor_preserves_current_behavior() -> None:
    """The extraction service should preserve the current deterministic behavior."""
    service = SpecExtractionService(
        preprocessor=SimpleRequestPreprocessor(),
        prompt_repository=FilePromptRepository(),
        payload_extractor=DeterministicSpecPayloadExtractor(),
        validator=PydanticSpecValidator(),
    )
    extractor = ServiceBackedSpecExtractor(extraction_service=service)

    spec = extractor.extract_spec(
        "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes"
    )

    assert spec.job_name == "bedout-job"
    assert spec.source_topic == "sensor-events"
    assert spec.output_event_name == "BedOut"


def test_default_extractor_is_still_the_deterministic_stub() -> None:
    """The default extractor should remain stub-backed after the refactor."""
    extractor = build_default_spec_extractor()

    assert isinstance(extractor, StubSpecExtractor)
    assert isinstance(extractor.extraction_service.payload_extractor, DeterministicSpecPayloadExtractor)


def test_model_backed_extractor_skeleton_is_not_active() -> None:
    """The future provider-backed extractor skeleton should not make real calls yet."""
    extractor = OpenAISpecPayloadExtractor()

    with pytest.raises(NotImplementedError, match="not implemented yet"):
        extractor.extract_payload("request", "prompt")
