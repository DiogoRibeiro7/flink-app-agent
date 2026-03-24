"""Tests for the strict Flink job specification model."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.spec import ALLOWED_AGGREGATION_TYPE, ALLOWED_RULE_TYPE, FlinkJobSpec


def test_demo_factory_returns_valid_spec() -> None:
    """The demo helper should build a valid first-version spec."""
    spec = FlinkJobSpec.demo()

    assert spec.job_name == "fraud-alert-job"
    assert spec.rule_type == ALLOWED_RULE_TYPE
    assert spec.time_window_minutes == 10


def test_windowed_aggregation_demo_returns_valid_spec() -> None:
    """The aggregation demo helper should build a valid second-family spec."""
    spec = FlinkJobSpec.demo_windowed_aggregation()

    assert spec.rule_type == ALLOWED_AGGREGATION_TYPE
    assert spec.output_event_name == "WindowedCount"
    assert spec.time_window_minutes == 5


def test_model_normalizes_supported_fields() -> None:
    """The model should normalize fields that have a safe deterministic form."""
    spec = FlinkJobSpec(
        job_name="  Sensor Occupancy Alerts  ",
        source_topic=" sensor-events ",
        sink_topic=" inferred-events ",
        key_by=" user id ",
        event_time_field=" event-time ",
        input_event_name="InputEvent",
        output_event_name=" bed_out ",
        rule_type=ALLOWED_RULE_TYPE,
        rule_condition="match within 20 minutes",
        time_window_minutes=20,
    )

    assert spec.job_name == "sensor-occupancy-alerts"
    assert spec.source_topic == "sensor-events"
    assert spec.sink_topic == "inferred-events"
    assert spec.key_by == "user_id"
    assert spec.event_time_field == "event_time"
    assert spec.output_event_name == "BedOut"


def test_template_dict_is_ready_for_substitution() -> None:
    """The template dictionary should expose plain string values."""
    spec = FlinkJobSpec.demo()

    assert spec.to_template_dict() == {
        "JOB_NAME": "fraud-alert-job",
        "SOURCE_TOPIC": "payments",
        "SINK_TOPIC": "alerts",
        "KEY_BY": "account_id",
        "EVENT_TIME_FIELD": "event_time",
        "INPUT_EVENT_NAME": "PaymentEvent",
        "OUTPUT_EVENT_NAME": "AlertEvent",
        "RULE_TYPE": "two_events_within_window",
        "RULE_CONDITION": "second payment occurs within 10 minutes with amount > 5000",
        "TIME_WINDOW_MINUTES": "10",
    }


def test_job_name_is_rejected_when_no_safe_characters_remain() -> None:
    """Job names should fail if normalization removes all meaningful content."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["job_name"] = "!!!"

    with pytest.raises(ValidationError, match="job_name must contain at least one letter or number"):
        FlinkJobSpec.model_validate(payload)


def test_topics_cannot_be_empty() -> None:
    """Blank topics should fail validation."""
    source_payload = FlinkJobSpec.demo().model_dump()
    source_payload["source_topic"] = "   "

    with pytest.raises(ValidationError):
        FlinkJobSpec.model_validate(source_payload)

    sink_payload = FlinkJobSpec.demo().model_dump()
    sink_payload["sink_topic"] = ""

    with pytest.raises(ValidationError):
        FlinkJobSpec.model_validate(sink_payload)


def test_key_by_must_be_a_valid_identifier() -> None:
    """Invalid key fields should be rejected when they cannot be normalized safely."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["key_by"] = "123-user"

    with pytest.raises(ValidationError, match="key_by must be a valid identifier"):
        FlinkJobSpec.model_validate(payload)


def test_event_time_field_must_be_a_valid_identifier() -> None:
    """Invalid event-time fields should be rejected when they start with a number."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["event_time_field"] = "9event_time"

    with pytest.raises(ValidationError, match="event_time_field must be a valid identifier"):
        FlinkJobSpec.model_validate(payload)


def test_output_event_name_is_normalized() -> None:
    """Output event names should normalize to a PascalCase class name."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["output_event_name"] = "bed out"

    spec = FlinkJobSpec.model_validate(payload)

    assert spec.output_event_name == "BedOut"


def test_output_event_name_is_rejected_when_empty_after_normalization() -> None:
    """Output event names should fail when no class name content remains."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["output_event_name"] = "___"

    with pytest.raises(ValidationError, match="output_event_name must not be empty"):
        FlinkJobSpec.model_validate(payload)


def test_time_window_minutes_must_be_positive() -> None:
    """The time window must be greater than zero."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["time_window_minutes"] = 0

    with pytest.raises(ValidationError):
        FlinkJobSpec.model_validate(payload)


def test_rule_type_is_restricted() -> None:
    """Only the single supported rule type should validate."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["rule_type"] = "threshold"

    with pytest.raises(ValidationError, match="rule_type must be one of"):
        FlinkJobSpec.model_validate(payload)
