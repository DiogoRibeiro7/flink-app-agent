"""Tests for provider extraction fallback behavior."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.config import ExtractorConfig
from flink_app_agent.constants import ProviderExtractionError
from flink_app_agent.llm import SpecParsingError
from flink_app_agent.main import parse_request


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
    assert outcome.extractor_used == "provider"
    assert outcome.fallback_triggered is False
    assert outcome.fallback_reason is None


# --- Provider failure with fallback=fail ---


def test_provider_failure_without_fallback_raises() -> None:
    """When fallback='fail', provider errors should propagate."""
    config = ExtractorConfig(
        mode="provider", fallback="fail", call_provider=_failing_provider,
    )

    with pytest.raises(ProviderExtractionError):
        parse_request(
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
            extractor_config=config,
        )


def test_provider_bad_json_without_fallback_raises() -> None:
    """When fallback='fail', invalid JSON from provider should propagate."""
    config = ExtractorConfig(
        mode="provider", fallback="fail", call_provider=_bad_json_provider,
    )

    with pytest.raises(ProviderExtractionError):
        parse_request(
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
            extractor_config=config,
        )


# --- Provider failure with fallback=deterministic ---


def test_provider_failure_with_deterministic_fallback_succeeds() -> None:
    """When fallback='deterministic', provider failure should fall back to deterministic."""
    config = ExtractorConfig(
        mode="provider", fallback="deterministic", call_provider=_failing_provider,
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.extractor_used == "deterministic"
    assert outcome.fallback_triggered is True
    assert "provider unreachable" in outcome.fallback_reason


def test_provider_bad_json_with_deterministic_fallback_succeeds() -> None:
    """When fallback='deterministic', bad JSON should fall back to deterministic."""
    config = ExtractorConfig(
        mode="provider", fallback="deterministic", call_provider=_bad_json_provider,
    )

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.extractor_used == "deterministic"
    assert outcome.fallback_triggered is True
    assert "invalid JSON" in outcome.fallback_reason


# --- Deterministic mode (no fallback needed) ---


def test_deterministic_mode_returns_deterministic_outcome() -> None:
    """Deterministic mode should always report extractor_used='deterministic'."""
    config = ExtractorConfig(mode="deterministic")

    spec, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
        extractor_config=config,
    )

    assert spec.source_topic == "payments"
    assert outcome.extractor_used == "deterministic"
    assert outcome.fallback_triggered is False


def test_deterministic_mode_failure_is_not_caught_as_fallback() -> None:
    """Deterministic extraction errors should propagate normally, not trigger fallback."""
    config = ExtractorConfig(mode="deterministic")

    with pytest.raises(SpecParsingError, match="source_topic"):
        parse_request(
            "key by account_id, emit Alert within 10 minutes",
            extractor_config=config,
        )


# --- Outcome in report ---


def test_fallback_outcome_records_reason() -> None:
    """The fallback reason should contain enough detail to diagnose the provider failure."""
    config = ExtractorConfig(
        mode="provider", fallback="deterministic", call_provider=_failing_provider,
    )

    _, outcome = parse_request(
        "Read from Kafka payments, key by account_id, emit Alert within 10 minutes",
        extractor_config=config,
    )

    assert outcome.fallback_triggered is True
    assert "ProviderExtractionError" in outcome.fallback_reason
    assert "provider unreachable" in outcome.fallback_reason
