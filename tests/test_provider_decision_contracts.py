"""Regression tests for provider quality and fallback decision contracts."""

from __future__ import annotations

import json

import pytest

from flink_app_agent.ambiguity import AmbiguityAssessment, AmbiguousRequestError
from flink_app_agent.config import ExtractorConfig
from flink_app_agent.main import parse_request
from flink_app_agent.provider_quality import (
    PROVIDER_QUALITY_ACCEPTABLE,
    ProviderPayloadQualityGate,
)


def _provider_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "job_family": "keyed_temporal_rule",
        "job_name": "fraud-alert-job",
        "source_topic": "payments",
        "sink_topic": "alerts",
        "key_by": "account_id",
        "event_time_field": "event_time",
        "input_event_name": "InputEvent",
        "output_event_name": "AlertEvent",
        "rule_type": "two_events_within_window",
        "rule_condition": "second account event within 10 minutes",
        "time_window_minutes": 10,
    }
    payload.update(overrides)
    return payload


def test_quality_gate_ignores_incidental_count_wording_for_keyed_rule() -> None:
    """Human-readable condition wording must not override normalized rule metadata."""
    payload = _provider_payload(
        rule_condition="emit when the account event count changes quickly",
    )

    assessment = ProviderPayloadQualityGate().assess(
        payload=payload,
        ambiguity=AmbiguityAssessment(),
    )

    assert assessment.category == PROVIDER_QUALITY_ACCEPTABLE
    assert assessment.findings == ()


def test_quality_gate_accepts_aggregation_without_literal_count_word() -> None:
    """A valid aggregation rule type should not require a literal word in prose."""
    payload = _provider_payload(
        job_family="windowed_aggregation",
        job_name="sensor-summary-job",
        output_event_name="SensorSummary",
        rule_type="count_by_key_window",
        rule_condition="summarize device events over five minutes",
        time_window_minutes=5,
    )

    assessment = ProviderPayloadQualityGate().assess(
        payload=payload,
        ambiguity=AmbiguityAssessment(),
    )

    assert assessment.category == PROVIDER_QUALITY_ACCEPTABLE
    assert assessment.findings == ()


def test_provider_ambiguity_can_fall_back_to_clear_deterministic_request() -> None:
    """Configured fallback should recover when only provider interpretation is ambiguous."""
    def ambiguous_provider(_: str, __: str) -> str:
        payload = _provider_payload()
        payload.pop("sink_topic")
        return json.dumps(payload)

    config = ExtractorConfig(
        mode="provider",
        fallback="deterministic",
        ambiguity_policy="fail",
        call_provider=ambiguous_provider,
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
        extractor_config=config,
    )

    assert spec.sink_topic == "alerts"
    assert outcome.actual_path == ("provider", "deterministic")
    assert outcome.fallback_triggered is True
    assert "AmbiguousRequestError" in outcome.fallback_reason


def test_deterministic_retry_still_fails_when_request_itself_is_ambiguous() -> None:
    """Fallback must not suppress ambiguity produced by the deterministic retry itself."""
    def ambiguous_provider(_: str, __: str) -> str:
        payload = _provider_payload()
        payload.pop("sink_topic")
        return json.dumps(payload)

    config = ExtractorConfig(
        mode="provider",
        fallback="deterministic",
        ambiguity_policy="fail",
        call_provider=ambiguous_provider,
    )

    with pytest.raises(AmbiguousRequestError):
        parse_request(
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
            extractor_config=config,
        )
