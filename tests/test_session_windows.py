"""Regression tests for keyed event-time session-window aggregation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator, build_main_class_name
from flink_app_agent.llm import build_default_spec_extractor
from flink_app_agent.main import resolve_template
from flink_app_agent.spec import (
    SESSION_WINDOW_AGGREGATION_RULE_TYPE,
    WINDOWED_AGGREGATION_RULE_TYPE,
)


def test_session_window_request_resolves_session_template(tmp_path: Path) -> None:
    """Explicit session wording should generate a session-window Flink scaffold."""
    request = (
        "Read from Kafka sensor-events, group by device_id, count events in "
        "5-minute session windows, write to Kafka session-counts"
    )

    spec = build_default_spec_extractor().extract_spec(request)
    template = resolve_template(spec)

    assert spec.rule_type == SESSION_WINDOW_AGGREGATION_RULE_TYPE
    assert spec.time_window_minutes == 5
    assert template.template_id == "flink_session_window_aggregation_job"

    output_dir = tmp_path / "session-count-job"
    ProjectGenerator(template_dir=template.template_path).generate(
        spec=spec,
        output_dir=output_dir,
    )

    job_path = (
        output_dir
        / "src"
        / "main"
        / "java"
        / "com"
        / "example"
        / f"{build_main_class_name(spec.job_name)}.java"
    )
    job_text = job_path.read_text(encoding="utf-8")

    assert "EventTimeSessionWindows.withGap(Time.minutes(5))" in job_text
    assert "TumblingEventTimeWindows" not in job_text


def test_standard_window_count_stays_tumbling() -> None:
    """Existing count wording should keep the tumbling-window template."""
    request = (
        "Read from Kafka sensor-events, group by device_id, "
        "count events within 5 minutes, write to Kafka aggregated-events"
    )

    spec = build_default_spec_extractor().extract_spec(request)
    template = resolve_template(spec)

    assert spec.rule_type == WINDOWED_AGGREGATION_RULE_TYPE
    assert template.template_id == "flink_windowed_aggregation_job"
