"""Tests for the Flink job specification model."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.spec import ALLOWED_RULE_TYPE, FlinkJobSpec, WINDOWED_AGGREGATION_RULE_TYPE


def test_valid_spec_creation() -> None:
    """A complete valid payload should build a spec successfully."""
    spec = FlinkJobSpec(
        job_name="Fraud Alert Job",
        source_topic="payments",
        sink_topic="alerts",
        key_by="account_id",
        event_time_field="event_time",
        input_event_name="InputEvent",
        output_event_name="AlertEvent",
        rule_type=ALLOWED_RULE_TYPE,
        rule_condition="second payment occurs within 10 minutes",
        time_window_minutes=10,
    )

    assert spec.job_name == "fraud-alert-job"
    assert spec.source_topic == "payments"
    assert spec.sink_topic == "alerts"
    assert spec.rule_type == ALLOWED_RULE_TYPE
    assert spec.input_event_name == "InputEvent"
    assert spec.output_event_name == "AlertEvent"


def test_invalid_empty_topic() -> None:
    """Blank topics should fail validation with a clear message."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["source_topic"] = "   "

    with pytest.raises(ValidationError, match="source_topic must not be empty"):
        FlinkJobSpec.model_validate(payload)

    payload = FlinkJobSpec.demo().model_dump()
    payload["sink_topic"] = "  "

    with pytest.raises(ValidationError, match="sink_topic must not be empty"):
        FlinkJobSpec.model_validate(payload)


def test_invalid_time_window() -> None:
    """The time window must be greater than zero."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["time_window_minutes"] = 0

    with pytest.raises(ValidationError):
        FlinkJobSpec.model_validate(payload)


def test_invalid_rule_type() -> None:
    """Only the current supported rule types should validate."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["rule_type"] = "unsupported_rule_type"

    with pytest.raises(
        ValidationError,
        match="rule_type must be one of:",
    ):
        FlinkJobSpec.model_validate(payload)


def test_windowed_aggregation_rule_type_is_supported() -> None:
    """The second supported rule type should validate successfully."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["rule_type"] = WINDOWED_AGGREGATION_RULE_TYPE
    payload["rule_condition"] = "count events by account_id within 10 minutes"

    spec = FlinkJobSpec.model_validate(payload)

    assert spec.rule_type == WINDOWED_AGGREGATION_RULE_TYPE


def test_job_name_is_normalized() -> None:
    """Job names should normalize into a filesystem-safe form."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["job_name"] = "  Sensor Occupancy Alerts  "

    spec = FlinkJobSpec.model_validate(payload)

    assert spec.job_name == "sensor-occupancy-alerts"


def test_identifier_fields_are_normalized() -> None:
    """Identifier-like fields should normalize to safe underscore-based names."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["key_by"] = " user id "
    payload["event_time_field"] = " event-time "

    spec = FlinkJobSpec.model_validate(payload)

    assert spec.key_by == "user_id"
    assert spec.event_time_field == "event_time"


def test_invalid_identifier_fields_are_rejected() -> None:
    """Identifier-like fields should fail if they still start with a number."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["key_by"] = "123-user"

    with pytest.raises(ValidationError, match="key_by must be a valid identifier"):
        FlinkJobSpec.model_validate(payload)

    payload = FlinkJobSpec.demo().model_dump()
    payload["event_time_field"] = "9event-time"

    with pytest.raises(ValidationError, match="event_time_field must be a valid identifier"):
        FlinkJobSpec.model_validate(payload)


def test_class_name_fields_are_normalized() -> None:
    """Class-name fields should normalize to PascalCase names."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["input_event_name"] = " input event "
    payload["output_event_name"] = " alert event "

    spec = FlinkJobSpec.model_validate(payload)

    assert spec.input_event_name == "InputEvent"
    assert spec.output_event_name == "AlertEvent"


def test_whitespace_cleanup_is_applied_to_topics_and_rule_condition() -> None:
    """Whitespace trimming should apply before validation on plain string fields."""
    spec = FlinkJobSpec(
        job_name="demo-job",
        source_topic=" payments ",
        sink_topic=" alerts ",
        key_by="account_id",
        event_time_field="event_time",
        input_event_name="InputEvent",
        output_event_name="AlertEvent",
        rule_type=ALLOWED_RULE_TYPE,
        rule_condition="  second payment occurs within 10 minutes  ",
        time_window_minutes=10,
    )

    assert spec.source_topic == "payments"
    assert spec.sink_topic == "alerts"
    assert spec.rule_condition == "second payment occurs within 10 minutes"


def test_template_dict_is_flat() -> None:
    """The template dictionary should expose plain string values only."""
    spec = FlinkJobSpec.demo()

    assert spec.to_template_dict() == {
        "JOB_NAME": "fraud-alert-job",
        "SOURCE_TOPIC": "payments",
        "SINK_TOPIC": "alerts",
        "KEY_BY": "account_id",
        "EVENT_TIME_FIELD": "event_time",
        "INPUT_EVENT_NAME": "InputEvent",
        "OUTPUT_EVENT_NAME": "AlertEvent",
        "RULE_TYPE": "two_events_within_window",
        "RULE_CONDITION": "second payment occurs within 10 minutes",
        "TIME_WINDOW_MINUTES": "10",
    }
