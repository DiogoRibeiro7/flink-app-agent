"""Regression tests for generated keyed-rule runtime semantics."""

from __future__ import annotations

from pathlib import Path


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


def test_generated_rule_rejects_negative_event_time_delta() -> None:
    source = (
        _TEMPLATE_ROOT
        / "src/main/java/com/example/functions/RuleProcessFunction.java"
    ).read_text(encoding="utf-8")

    assert "long delta = eventTime - firstSeen;" in source
    assert "delta >= 0L && delta <= windowMillis()" in source
    assert "if (eventTime >= firstSeen)" in source
