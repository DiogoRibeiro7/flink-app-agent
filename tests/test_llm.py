"""Tests for the deterministic and provider-backed extractors."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pydantic import ValidationError

from flink_app_agent.llm import (
    DeterministicSpecPayloadExtractor,
    FilePromptRepository,
    ProviderExtractionError,
    ProviderSpecPayloadExtractor,
    PydanticSpecValidator,
    ServiceBackedSpecExtractor,
    SimpleRequestPreprocessor,
    SpecExtractionService,
    SpecParsingError,
    StubSpecExtractor,
    build_default_spec_extractor,
    build_provider_spec_extractor,
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
                "job_family": "keyed_temporal_rule",
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
                "job_family": "keyed_temporal_rule",
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
                "job_family": "keyed_temporal_rule",
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
                "job_family": "keyed_temporal_rule",
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
                "job_family": "keyed_temporal_rule",
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
        (
            "Stream from Kafka sensor-events, partition by user_id, emit BED_OUT within 20 minutes",
            {
                "job_family": "keyed_temporal_rule",
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
            "Read from Kafka sensor-events, key by device_id, count events within 5 minutes",
            {
                "job_family": "windowed_aggregation",
                "job_name": "sensor-events-count-job",
                "source_topic": "sensor-events",
                "sink_topic": "aggregated-events",
                "key_by": "device_id",
                "event_time_field": "ts",
                "input_event_name": "InputEvent",
                "output_event_name": "SensorEventsCount",
                "rule_type": "count_by_key_window",
                "rule_condition": "count events by device_id within 5 minutes",
                "time_window_minutes": 5,
            },
        ),
        (
            "Build a Flink job reading sensor-events, group by device_id, aggregate count every 10 minutes",
            {
                "job_family": "windowed_aggregation",
                "job_name": "sensor-events-count-job",
                "source_topic": "sensor-events",
                "sink_topic": "aggregated-events",
                "key_by": "device_id",
                "event_time_field": "ts",
                "input_event_name": "InputEvent",
                "output_event_name": "SensorEventsCount",
                "rule_type": "count_by_key_window",
                "rule_condition": "count events by device_id within 10 minutes",
                "time_window_minutes": 10,
            },
        ),
        (
            "Listen to Kafka device-events, partition by device_id, count events within 15 minutes",
            {
                "job_family": "windowed_aggregation",
                "job_name": "device-events-count-job",
                "source_topic": "device-events",
                "sink_topic": "aggregated-events",
                "key_by": "device_id",
                "event_time_field": "ts",
                "input_event_name": "InputEvent",
                "output_event_name": "DeviceEventsCount",
                "rule_type": "count_by_key_window",
                "rule_condition": "count events by device_id within 15 minutes",
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
        (
            "Read from Kafka sensor-events, count events within 5 minutes",
            "key_by",
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


def test_service_backed_extractor_supports_windowed_aggregation_family() -> None:
    """The extraction service should support the aggregation family explicitly."""
    service = SpecExtractionService(
        preprocessor=SimpleRequestPreprocessor(),
        prompt_repository=FilePromptRepository(),
        payload_extractor=DeterministicSpecPayloadExtractor(),
        validator=PydanticSpecValidator(),
    )
    extractor = ServiceBackedSpecExtractor(extraction_service=service)

    spec = extractor.extract_spec(
        "Read from Kafka sensor-events, group by device_id, count events within 5 minutes"
    )

    assert spec.rule_type == "count_by_key_window"
    assert spec.sink_topic == "aggregated-events"
    assert spec.output_event_name == "SensorEventsCount"


def test_default_extractor_is_still_the_deterministic_stub() -> None:
    """The default extractor should remain stub-backed after the refactor."""
    extractor = build_default_spec_extractor()

    assert isinstance(extractor, StubSpecExtractor)
    assert isinstance(extractor.extraction_service.payload_extractor, DeterministicSpecPayloadExtractor)


VALID_PROVIDER_RESPONSE = json.dumps({
    "job_family": "keyed_temporal_rule",
    "job_name": "fraud-alert-job",
    "source_topic": "payments",
    "sink_topic": "alerts",
    "key_by": "account_id",
    "event_time_field": "event_time",
    "input_event_name": "InputEvent",
    "output_event_name": "AlertEvent",
    "rule_type": "two_events_within_window",
    "rule_condition": "second payment occurs within 10 minutes",
    "time_window_minutes": 10,
})

VALID_AGGREGATION_RESPONSE = json.dumps({
    "job_family": "windowed_aggregation",
    "job_name": "sensor-events-count-job",
    "source_topic": "sensor-events",
    "sink_topic": "aggregated-events",
    "key_by": "device_id",
    "event_time_field": "ts",
    "input_event_name": "InputEvent",
    "output_event_name": "SensorEventsCount",
    "rule_type": "count_by_key_window",
    "rule_condition": "count events by device_id within 5 minutes",
    "time_window_minutes": 5,
})


def test_provider_extractor_parses_valid_json_response() -> None:
    """A valid provider JSON response should produce a validated spec."""
    def mock_provider(request: str, prompt: str) -> str:
        return VALID_PROVIDER_RESPONSE

    extractor = build_provider_spec_extractor(mock_provider)
    spec = extractor.extract_spec("any request text")

    assert spec.job_name == "fraud-alert-job"
    assert spec.source_topic == "payments"
    assert spec.rule_type == "two_events_within_window"
    assert spec.job_family == "keyed_temporal_rule"


def test_provider_extractor_supports_windowed_aggregation() -> None:
    """A provider response for the aggregation family should validate correctly."""
    def mock_provider(request: str, prompt: str) -> str:
        return VALID_AGGREGATION_RESPONSE

    extractor = build_provider_spec_extractor(mock_provider)
    spec = extractor.extract_spec("any request text")

    assert spec.job_family == "windowed_aggregation"
    assert spec.rule_type == "count_by_key_window"
    assert spec.output_event_name == "SensorEventsCount"


def test_provider_extractor_passes_prompt_to_provider() -> None:
    """The extraction prompt text should be forwarded to the provider callable."""
    received_prompt = []

    def mock_provider(request: str, prompt: str) -> str:
        received_prompt.append(prompt)
        return VALID_PROVIDER_RESPONSE

    extractor = build_provider_spec_extractor(mock_provider)
    extractor.extract_spec("any request")

    assert len(received_prompt) == 1
    assert "FlinkJobSpec" in received_prompt[0]
    assert "job_family" in received_prompt[0]


def test_provider_extractor_rejects_invalid_json() -> None:
    """Non-JSON provider output should fail with a clear error."""
    def mock_provider(request: str, prompt: str) -> str:
        return "this is not json"

    extractor = build_provider_spec_extractor(mock_provider)

    with pytest.raises(ProviderExtractionError, match="invalid JSON"):
        extractor.extract_spec("any request")


def test_provider_extractor_rejects_non_object_json() -> None:
    """A JSON array instead of object should fail clearly."""
    def mock_provider(request: str, prompt: str) -> str:
        return '[1, 2, 3]'

    extractor = build_provider_spec_extractor(mock_provider)

    with pytest.raises(ProviderExtractionError, match="expected a JSON object"):
        extractor.extract_spec("any request")


def test_provider_extractor_wraps_provider_exceptions() -> None:
    """Exceptions from the provider callable should be wrapped clearly."""
    def failing_provider(request: str, prompt: str) -> str:
        raise ConnectionError("network timeout")

    extractor = build_provider_spec_extractor(failing_provider)

    with pytest.raises(ProviderExtractionError, match="Provider call failed"):
        extractor.extract_spec("any request")


def test_provider_output_with_invalid_spec_fails_validation() -> None:
    """Provider output that is valid JSON but has bad field values should fail validation."""
    def mock_provider(request: str, prompt: str) -> str:
        return json.dumps({
            "job_family": "unsupported_family",
            "job_name": "test-job",
            "source_topic": "topic",
            "sink_topic": "sink",
            "key_by": "key",
            "event_time_field": "ts",
            "input_event_name": "In",
            "output_event_name": "Out",
            "rule_type": "two_events_within_window",
            "rule_condition": "rule",
            "time_window_minutes": 5,
        })

    extractor = build_provider_spec_extractor(mock_provider)

    with pytest.raises(ValidationError, match="job_family"):
        extractor.extract_spec("any request")


def test_provider_output_with_missing_fields_fails_validation() -> None:
    """Provider output missing required fields should fail validation."""
    def mock_provider(request: str, prompt: str) -> str:
        return json.dumps({"job_name": "test-job"})

    extractor = build_provider_spec_extractor(mock_provider)

    with pytest.raises(ValidationError):
        extractor.extract_spec("any request")


def test_default_extractor_is_deterministic_not_provider() -> None:
    """The default extractor must remain the deterministic stub."""
    extractor = build_default_spec_extractor()

    assert isinstance(extractor, StubSpecExtractor)
    assert isinstance(
        extractor.extraction_service.payload_extractor,
        DeterministicSpecPayloadExtractor,
    )


def test_provider_extractor_uses_provider_payload_extractor() -> None:
    """The provider-backed extractor should use ProviderSpecPayloadExtractor internally."""
    def mock_provider(request: str, prompt: str) -> str:
        return VALID_PROVIDER_RESPONSE

    extractor = build_provider_spec_extractor(mock_provider)

    assert isinstance(extractor, ServiceBackedSpecExtractor)
    assert isinstance(
        extractor.extraction_service.payload_extractor,
        ProviderSpecPayloadExtractor,
    )
