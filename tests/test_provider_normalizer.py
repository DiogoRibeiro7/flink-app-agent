"""Tests for structured provider output normalization."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import ProviderExtractionError
from flink_app_agent.provider_normalizer import normalize_provider_payload


def _valid_keyed_rule_payload() -> dict:
    return {
        "job_family": "keyed_temporal_rule",
        "job_name": "fraud-alert-job",
        "source_topic": "payments",
        "sink_topic": "alerts",
        "key_by": "account_id",
        "event_time_field": "event_time",
        "input_event_name": "InputEvent",
        "output_event_name": "AlertEvent",
        "rule_type": "two_events_within_window",
        "rule_condition": "second payment within 10 minutes",
        "time_window_minutes": 10,
    }


def _valid_aggregation_payload() -> dict:
    return {
        "job_family": "windowed_aggregation",
        "job_name": "sensor-count-job",
        "source_topic": "sensor-events",
        "sink_topic": "aggregated-events",
        "key_by": "device_id",
        "event_time_field": "ts",
        "input_event_name": "InputEvent",
        "output_event_name": "SensorCount",
        "rule_type": "count_by_key_window",
        "rule_condition": "count by device_id within 5 minutes",
        "time_window_minutes": 5,
    }


# --- Valid structured output ---


def test_valid_keyed_rule_payload_passes() -> None:
    """A clean keyed rule payload should pass normalization unchanged."""
    raw = _valid_keyed_rule_payload()

    result = normalize_provider_payload(raw)

    assert result == raw


def test_valid_aggregation_payload_passes() -> None:
    """A clean aggregation payload should pass normalization unchanged."""
    raw = _valid_aggregation_payload()

    result = normalize_provider_payload(raw)

    assert result == raw


# --- Alias mapping ---


def test_camel_case_aliases_are_mapped() -> None:
    """camelCase field names should be mapped to snake_case."""
    raw = {
        "jobFamily": "keyed_temporal_rule",
        "jobName": "fraud-alert-job",
        "sourceTopic": "payments",
        "sinkTopic": "alerts",
        "keyBy": "account_id",
        "eventTimeField": "event_time",
        "inputEventName": "InputEvent",
        "outputEventName": "AlertEvent",
        "ruleType": "two_events_within_window",
        "ruleCondition": "second payment within 10 minutes",
        "timeWindowMinutes": 10,
    }

    result = normalize_provider_payload(raw)

    assert result["job_family"] == "keyed_temporal_rule"
    assert result["source_topic"] == "payments"
    assert result["time_window_minutes"] == 10


def test_hyphenated_aliases_are_mapped() -> None:
    """Hyphenated field names should be mapped to snake_case."""
    raw = {
        "job-family": "keyed_temporal_rule",
        "job-name": "fraud-alert-job",
        "source-topic": "payments",
        "sink-topic": "alerts",
        "key-by": "account_id",
        "event-time-field": "event_time",
        "input_event_name": "InputEvent",
        "output_event_name": "AlertEvent",
        "rule-type": "two_events_within_window",
        "rule-condition": "second payment within 10 minutes",
        "time-window-minutes": 10,
    }

    result = normalize_provider_payload(raw)

    assert result["job_family"] == "keyed_temporal_rule"
    assert result["key_by"] == "account_id"


# --- Extra fields stripped ---


def test_extra_fields_are_stripped() -> None:
    """Provider-added explanation fields should be silently removed."""
    raw = _valid_keyed_rule_payload()
    raw["explanation"] = "I chose this because..."
    raw["confidence"] = 0.95
    raw["_internal"] = True

    result = normalize_provider_payload(raw)

    assert "explanation" not in result
    assert "confidence" not in result
    assert "_internal" not in result
    assert result["job_name"] == "fraud-alert-job"


# --- Partial payloads preserved for ambiguity assessment ---


def test_missing_single_field_is_preserved_for_later_assessment() -> None:
    """A payload missing one field should remain intact for later ambiguity checks."""
    raw = _valid_keyed_rule_payload()
    del raw["source_topic"]

    result = normalize_provider_payload(raw)

    assert "source_topic" not in result
    assert result["job_family"] == "keyed_temporal_rule"


def test_missing_multiple_fields_are_preserved_when_types_are_safe() -> None:
    """A partial payload should still normalize aliases and known fields safely."""
    raw = {"job_name": "test-job"}

    result = normalize_provider_payload(raw)

    assert result == {"job_name": "test-job"}


def test_empty_payload_stays_empty() -> None:
    """An empty payload should pass through so ambiguity can be classified later."""
    assert normalize_provider_payload({}) == {}


# --- Type coercion ---


def test_string_time_window_is_coerced_to_int() -> None:
    """A string time_window_minutes should be coerced to int."""
    raw = _valid_keyed_rule_payload()
    raw["time_window_minutes"] = "10"

    result = normalize_provider_payload(raw)

    assert result["time_window_minutes"] == 10
    assert isinstance(result["time_window_minutes"], int)


def test_float_time_window_is_coerced_to_int() -> None:
    """A whole-number float time_window_minutes should be coerced to int."""
    raw = _valid_keyed_rule_payload()
    raw["time_window_minutes"] = 10.0

    result = normalize_provider_payload(raw)

    assert result["time_window_minutes"] == 10
    assert isinstance(result["time_window_minutes"], int)


def test_non_whole_float_time_window_fails() -> None:
    """A fractional float time_window_minutes should fail."""
    raw = _valid_keyed_rule_payload()
    raw["time_window_minutes"] = 10.5

    with pytest.raises(ProviderExtractionError, match="whole number"):
        normalize_provider_payload(raw)


def test_non_numeric_string_time_window_fails() -> None:
    """A non-numeric string time_window_minutes should fail."""
    raw = _valid_keyed_rule_payload()
    raw["time_window_minutes"] = "ten"

    with pytest.raises(ProviderExtractionError, match="must be an integer"):
        normalize_provider_payload(raw)


def test_non_string_field_value_fails() -> None:
    """A non-string value for a string field should fail."""
    raw = _valid_keyed_rule_payload()
    raw["source_topic"] = 42

    with pytest.raises(ProviderExtractionError, match="source_topic must be a string"):
        normalize_provider_payload(raw)


# --- Family/rule_type coherence ---


def test_keyed_rule_with_wrong_rule_type_fails() -> None:
    """A keyed rule family with aggregation rule_type should fail."""
    raw = _valid_keyed_rule_payload()
    raw["rule_type"] = "count_by_key_window"

    with pytest.raises(ProviderExtractionError, match="Incoherent"):
        normalize_provider_payload(raw)


def test_aggregation_with_wrong_rule_type_fails() -> None:
    """An aggregation family with keyed rule_type should fail."""
    raw = _valid_aggregation_payload()
    raw["rule_type"] = "two_events_within_window"

    with pytest.raises(ProviderExtractionError, match="Incoherent"):
        normalize_provider_payload(raw)


def test_keyed_rule_with_correct_rule_type_passes() -> None:
    """A keyed rule family with matching rule_type should pass."""
    raw = _valid_keyed_rule_payload()

    result = normalize_provider_payload(raw)

    assert result["job_family"] == "keyed_temporal_rule"
    assert result["rule_type"] == "two_events_within_window"


def test_aggregation_with_correct_rule_type_passes() -> None:
    """An aggregation family with matching rule_type should pass."""
    raw = _valid_aggregation_payload()

    result = normalize_provider_payload(raw)

    assert result["job_family"] == "windowed_aggregation"
    assert result["rule_type"] == "count_by_key_window"
