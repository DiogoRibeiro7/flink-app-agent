"""Tests for provider extraction fallback behavior."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.ambiguity import AmbiguousRequestError
from flink_app_agent.config import ExtractorConfig
from flink_app_agent.constants import ProviderExtractionError
from flink_app_agent.llm import SpecParsingError
from flink_app_agent.main import parse_request
from flink_app_agent.request_taxonomy import UnsupportedRequestError


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
    "rule_condition": "second payment within 10 minutes",
    "time_window_minutes": 10,
})


def _success_provider(request: str, prompt: str) -> str:
    return VALID_PROVIDER_RESPONSE


def _failing_provider(request: str, prompt: str) -> str:
    raise ConnectionError("provider unreachable")


def _bad_json_provider(request: str, prompt: str) -> str:
    return "not json"


# --- Provider success ---


def test_provider_success_returns_provider_outcome() -> None:
    """Successful provider extraction should report extractor_used='provider'."""
    config = ExtractorConfig(
        mode="provider", fallback="fail", call_provider=_success_provider,
    )

    spec, outcome = parse_request("any request", extractor_config=config)

    assert spec.job_name == "fraud-alert-job"
    assert outcome.requested_mode == "provider"
    assert outcome.extractor_used == "provider"
    assert outcome.actual_path == ("provider",)
    assert outcome.fallback_triggered is False
    assert outcome.fallback_reason is None
    assert outcome.provider_error is None
    assert outcome.provider_status == "available"
    assert outcome.provider_quality == "acceptable"
    assert outcome.provider_quality_summary == "acceptable"
    assert outcome.warnings == ()
    assert outcome.errors == ()


# --- Provider failure with fallback=fail ---


def test_provider_failure_without_fallback_raises() -> None:
    """When fallback='fail', provider errors should propagate."""
    config = ExtractorConfig(
        mode="provider", fallback="fail", call_provider=_failing_provider,
    )

    with pytest.raises(ProviderExtractionError):
        parse_request(
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
            extractor_config=config,
        )


def test_provider_bad_json_without_fallback_raises() -> None:
    """When fallback='fail', invalid JSON from provider should propagate."""
    config = ExtractorConfig(
        mode="provider", fallback="fail", call_provider=_bad_json_provider,
    )

    with pytest.raises(ProviderExtractionError):
        parse_request(
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
            extractor_config=config,
        )


# --- Provider failure with fallback=deterministic ---


def test_provider_failure_with_deterministic_fallback_succeeds() -> None:
    """When fallback='deterministic', provider failure should fall back to deterministic."""
    config = ExtractorConfig(
        mode="provider", fallback="deterministic", call_provider=_failing_provider,
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.requested_mode == "provider"
    assert outcome.fallback_policy == "deterministic"
    assert outcome.extractor_used == "deterministic"
    assert outcome.actual_path == ("provider", "deterministic")
    assert outcome.fallback_triggered is True
    assert "provider unreachable" in outcome.fallback_reason
    assert "provider unreachable" in outcome.provider_error
    assert outcome.provider_status == "unavailable"
    assert outcome.warnings == (
        "Provider extraction failed; deterministic fallback was used.",
    )
    assert len(outcome.errors) == 1
    assert "provider unreachable" in outcome.errors[0]


def test_provider_bad_json_with_deterministic_fallback_succeeds() -> None:
    """When fallback='deterministic', bad JSON should fall back to deterministic."""
    config = ExtractorConfig(
        mode="provider", fallback="deterministic", call_provider=_bad_json_provider,
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.extractor_used == "deterministic"
    assert outcome.actual_path == ("provider", "deterministic")
    assert outcome.fallback_triggered is True
    assert "invalid JSON" in outcome.fallback_reason
    assert outcome.provider_error is not None
    assert outcome.provider_status == "unavailable"
    assert outcome.provider_quality == "unusable"


def test_provider_incomplete_output_is_marked_ambiguous_under_minor_defaults() -> None:
    """Missing low-risk fields should be visible as ambiguous provider quality."""
    def incomplete_provider(_: str, __: str) -> str:
        return json.dumps({
            "job_family": "keyed_temporal_rule",
            "job_name": "fraud-alert-job",
            "source_topic": "payments",
            "key_by": "account_id",
            "event_time_field": "event_time",
            "input_event_name": "InputEvent",
            "output_event_name": "AlertEvent",
            "rule_type": "two_events_within_window",
            "rule_condition": "second payment within 10 minutes",
            "time_window_minutes": 10,
        })

    config = ExtractorConfig(
        mode="provider",
        fallback="fail",
        ambiguity_policy="minor_defaults",
        call_provider=incomplete_provider,
    )

    spec, outcome = parse_request("any request", extractor_config=config)

    assert spec.sink_topic == "inferred-events"
    assert outcome.provider_quality == "ambiguous"
    assert outcome.provider_quality_summary == (
        "Provider output remains ambiguous after normalization: missing_sink_topic"
    )


def test_provider_coherent_output_ignores_incidental_rule_wording() -> None:
    """Quality should trust normalized family/rule metadata, not condition keywords."""
    def coherent_provider(_: str, __: str) -> str:
        return json.dumps({
            "job_family": "windowed_aggregation",
            "job_name": "sensor-events-count-job",
            "source_topic": "sensor-events",
            "sink_topic": "aggregated-events",
            "key_by": "device_id",
            "event_time_field": "event_time",
            "input_event_name": "InputEvent",
            "output_event_name": "SensorEventsCount",
            "rule_type": "count_by_key_window",
            "rule_condition": "emit alert events quickly",
            "time_window_minutes": 5,
        })

    config = ExtractorConfig(
        mode="provider",
        fallback="fail",
        call_provider=coherent_provider,
    )

    spec, outcome = parse_request("any request", extractor_config=config)

    assert spec.job_family == "windowed_aggregation"
    assert spec.rule_type == "count_by_key_window"
    assert outcome.provider_quality == "acceptable"


def test_provider_unusable_output_triggers_deterministic_fallback() -> None:
    """Unusable provider output should degrade to deterministic fallback when configured."""
    def incomplete_provider(_: str, __: str) -> str:
        return json.dumps({
            "job_family": "keyed_temporal_rule",
            "source_topic": "payments",
            "sink_topic": "alerts",
            "key_by": "account_id",
            "event_time_field": "event_time",
            "input_event_name": "InputEvent",
            "output_event_name": "AlertEvent",
            "rule_type": "two_events_within_window",
            "rule_condition": "second payment within 10 minutes",
            "time_window_minutes": 10,
        })

    config = ExtractorConfig(
        mode="provider",
        fallback="deterministic",
        call_provider=incomplete_provider,
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.actual_path == ("provider", "deterministic")
    assert outcome.fallback_triggered is True
    assert outcome.provider_quality == "unusable"
    assert "quality gate" in outcome.fallback_reason


# --- Deterministic mode (no fallback needed) ---


def test_deterministic_mode_returns_deterministic_outcome() -> None:
    """Deterministic mode should always report extractor_used='deterministic'."""
    config = ExtractorConfig(mode="deterministic")

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.requested_mode == "deterministic"
    assert outcome.extractor_used == "deterministic"
    assert outcome.actual_path == ("deterministic",)
    assert outcome.fallback_triggered is False


def test_deterministic_mode_failure_is_not_caught_as_fallback() -> None:
    """Deterministic extraction errors should propagate normally, not trigger fallback."""
    config = ExtractorConfig(mode="deterministic")

    with pytest.raises(SpecParsingError, match="source_topic"):
        parse_request(
            "key by account_id, emit Alert within 10 minutes",
            extractor_config=config,
        )


def test_unsupported_request_is_distinct_from_invalid_request() -> None:
    """Structurally understandable but out-of-scope requests should be unsupported."""
    config = ExtractorConfig(mode="deterministic")

    with pytest.raises(UnsupportedRequestError, match="joins are not supported"):
        parse_request(
            "Join payments with accounts by account_id within 10 minutes",
            extractor_config=config,
        )


def test_fail_on_ambiguity_is_the_safe_default() -> None:
    """The default ambiguity policy should fail rather than inject assumptions."""
    config = ExtractorConfig(mode="deterministic")

    with pytest.raises(AmbiguousRequestError, match="policy=fail"):
        parse_request(
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
            extractor_config=config,
        )


def test_minor_ambiguity_policy_can_apply_safe_defaults_with_warning() -> None:
    """A minor ambiguity can continue only with an explicit policy and warning."""
    config = ExtractorConfig(
        mode="deterministic",
        ambiguity_policy="minor_defaults",
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
        extractor_config=config,
    )

    assert spec.sink_topic == "inferred-events"
    assert outcome.ambiguity_status == "minor"
    assert outcome.ambiguity_policy == "minor_defaults"
    assert outcome.ambiguity_policy_result == "used_safe_defaults"
    assert outcome.injected_defaults == ("sink_topic",)
    assert outcome.warnings == ("Applied safe default for sink_topic: inferred-events",)


def test_major_ambiguity_still_fails_under_minor_defaults_policy() -> None:
    """Major ambiguity should still fail even under the relaxed policy."""
    config = ExtractorConfig(
        mode="deterministic",
        ambiguity_policy="minor_defaults",
    )

    with pytest.raises(AmbiguousRequestError, match="failed_major"):
        parse_request(
            "Read from Kafka payments, emit Alert within 10 minutes",
            extractor_config=config,
        )


# --- Outcome in report ---


def test_fallback_outcome_records_reason() -> None:
    """The fallback reason should contain enough detail to diagnose the provider failure."""
    config = ExtractorConfig(
        mode="provider", fallback="deterministic", call_provider=_failing_provider,
    )

    _, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
        extractor_config=config,
    )

    assert outcome.fallback_triggered is True
    assert "ProviderExtractionError" in outcome.fallback_reason
    assert "provider unreachable" in outcome.fallback_reason


def test_provider_output_with_unsupported_family_raises_unsupported_request() -> None:
    """Provider-backed output outside the supported families should use the same taxonomy."""
    def unsupported_provider(_: str, __: str) -> str:
        return json.dumps(
            {
                "job_family": "stream_join",
                "job_name": "join-job",
                "source_topic": "payments",
                "sink_topic": "alerts",
                "key_by": "account_id",
                "event_time_field": "event_time",
                "input_event_name": "InputEvent",
                "output_event_name": "JoinedEvent",
                "rule_type": "join_streams",
                "rule_condition": "join streams by account_id",
                "time_window_minutes": 10,
            }
        )

    config = ExtractorConfig(
        mode="provider",
        fallback="fail",
        call_provider=unsupported_provider,
    )

    with pytest.raises(UnsupportedRequestError, match="outside the current supported feature scope"):
        parse_request("any request", extractor_config=config)
