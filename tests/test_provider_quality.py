"""Tests for provider-backed quality gating."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.ambiguity import AmbiguityAssessment, AmbiguityIssue
from flink_app_agent.provider_quality import (
    PROVIDER_QUALITY_ACCEPTABLE,
    PROVIDER_QUALITY_AMBIGUOUS,
    PROVIDER_QUALITY_UNUSABLE,
    ProviderPayloadQualityGate,
)


def test_provider_quality_gate_accepts_complete_consistent_payload() -> None:
    """Complete coherent provider output should pass the quality gate."""
    assessment = ProviderPayloadQualityGate().assess(
        payload={
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
        },
        ambiguity=AmbiguityAssessment(),
    )

    assert assessment.category == PROVIDER_QUALITY_ACCEPTABLE
    assert assessment.findings == ()


def test_provider_quality_gate_marks_ambiguous_when_ambiguity_remains() -> None:
    """Minor ambiguity should lower provider quality without making it unusable."""
    assessment = ProviderPayloadQualityGate().assess(
        payload={
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
        },
        ambiguity=AmbiguityAssessment(
            issues=(
                AmbiguityIssue(
                    code="missing_sink_topic",
                    message="The request does not identify a sink topic.",
                    fields=("sink_topic",),
                ),
            )
        ),
    )

    assert assessment.category == PROVIDER_QUALITY_AMBIGUOUS
    assert assessment.findings[0].code == "ambiguity_present"


def test_provider_quality_gate_rejects_missing_essential_fields() -> None:
    """Missing essential provider fields should make the payload unusable."""
    assessment = ProviderPayloadQualityGate().assess(
        payload={
            "job_family": "keyed_temporal_rule",
            "source_topic": "payments",
        },
        ambiguity=AmbiguityAssessment(),
    )

    assert assessment.category == PROVIDER_QUALITY_UNUSABLE
    assert assessment.findings[0].code == "missing_essential_fields"


def test_provider_quality_gate_rejects_family_condition_mismatch() -> None:
    """Family-specific rule-condition mismatches should be unusable."""
    assessment = ProviderPayloadQualityGate().assess(
        payload={
            "job_family": "windowed_aggregation",
            "job_name": "sensor-events-count-job",
            "source_topic": "sensor-events",
            "sink_topic": "aggregated-events",
            "key_by": "device_id",
            "event_time_field": "event_time",
            "input_event_name": "InputEvent",
            "output_event_name": "SensorEventsCount",
            "rule_type": "count_by_key_window",
            "rule_condition": "emit alerts when values spike",
            "time_window_minutes": 5,
        },
        ambiguity=AmbiguityAssessment(),
    )

    assert assessment.category == PROVIDER_QUALITY_UNUSABLE
    assert assessment.findings[0].code == "family_condition_mismatch"
