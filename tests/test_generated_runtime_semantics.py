"""Regression tests for generated keyed-rule runtime semantics."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from flink_app_agent.spec import FlinkJobSpec, MAX_TIME_WINDOW_MINUTES


_TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "templates" / "flink_kafka_rule_job"


def test_generated_job_filters_blank_keys_before_keyby() -> None:
    source = (
        _TEMPLATE_ROOT / "src/main/java/com/example/JobTemplate.java"
    ).read_text(encoding="utf-8")

    filter_snippet = '.filter(event -> !event.getField("{{KEY_BY}}").isBlank())'
    keyby_snippet = '.keyBy(event -> event.getField("{{KEY_BY}}"))'

    assert filter_snippet in source
    assert keyby_snippet in source
    assert source.index(filter_snippet) < source.index(keyby_snippet)


def test_generated_rule_uses_overflow_safe_event_time_window() -> None:
    source = (
        _TEMPLATE_ROOT
        / "src/main/java/com/example/functions/RuleProcessFunction.java"
    ).read_text(encoding="utf-8")

    assert "if (eventTime >= firstSeen && eventTime <= windowEnd(firstSeen))" in source
    assert "registerEventTimeTimer(windowEnd(eventTime))" in source
    assert "windowEnd(firstSeen) <= timestamp" in source
    assert "if (start > Long.MAX_VALUE - window)" in source
    assert "if (timeWindowMinutes > Long.MAX_VALUE / 60_000L)" in source
    assert "return Long.MAX_VALUE;" in source
    assert "long delta = eventTime - firstSeen;" not in source


def test_time_window_must_fit_generated_java_milliseconds() -> None:
    payload = FlinkJobSpec.demo().model_dump()
    payload["time_window_minutes"] = MAX_TIME_WINDOW_MINUTES

    spec = FlinkJobSpec.model_validate(payload)
    assert spec.time_window_minutes == MAX_TIME_WINDOW_MINUTES

    payload["time_window_minutes"] = MAX_TIME_WINDOW_MINUTES + 1
    with pytest.raises(ValidationError):
        FlinkJobSpec.model_validate(payload)
