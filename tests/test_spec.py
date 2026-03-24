"""Tests for the strict Flink job specification model."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.spec import ALLOWED_RULE_TYPE, FlinkJobSpec


def test_demo_factory_returns_valid_spec() -> None:
    """The demo helper should build a valid first-version spec."""
    spec = FlinkJobSpec.demo()

    assert spec.job_name == "fraud-alert-job"
    assert spec.rule_type == ALLOWED_RULE_TYPE
    assert spec.time_window_minutes == 10


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


def test_job_name_must_be_filesystem_safe() -> None:
    """Invalid filesystem characters should be rejected."""
    payload = FlinkJobSpec.demo().model_dump()
    payload["job_name"] = "fraud alert/job"

    with pytest.raises(ValidationError):
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

    with pytest.raises(ValidationError):
        FlinkJobSpec.model_validate(payload)
