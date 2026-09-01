"""Regression coverage for PR #20 reviewer findings."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.llm import build_default_spec_extractor
from flink_app_agent.main import resolve_template
from flink_app_agent.provider_normalizer import normalize_provider_payload
from flink_app_agent.spec import SESSION_WINDOW_AGGREGATION_RULE_TYPE, FlinkJobSpec


def test_provider_session_rule_normalizes() -> None:
    payload = {
        "job_family": "windowed_aggregation",
        "job_name": "session-count-job",
        "source_topic": "events",
        "sink_topic": "counts",
        "key_by": "device_id",
        "event_time_field": "ts",
        "input_event_name": "InputEvent",
        "output_event_name": "SessionCount",
        "rule_type": SESSION_WINDOW_AGGREGATION_RULE_TYPE,
        "rule_condition": "count events by device_id using a 5-minute session gap",
        "time_window_minutes": 5,
    }

    assert normalize_provider_payload(payload)["rule_type"] == SESSION_WINDOW_AGGREGATION_RULE_TYPE


def test_session_topic_name_does_not_select_session_window() -> None:
    request = (
        "Read from Kafka session-counts, group by device_id, count events within 5 minutes, "
        "write to Kafka aggregated-events"
    )

    spec = build_default_spec_extractor().extract_spec(request)
    template = resolve_template(spec)

    assert template.template_id == "flink_windowed_aggregation_job"


def test_five_minute_session_gap_is_parsed() -> None:
    request = (
        "Read from Kafka sensor-events, group by device_id, count events with a 5-minute "
        "session gap, write to Kafka session-counts"
    )

    spec = build_default_spec_extractor().extract_spec(request)

    assert spec.rule_type == SESSION_WINDOW_AGGREGATION_RULE_TYPE
    assert spec.time_window_minutes == 5


def test_java_string_placeholders_are_escaped(tmp_path: Path) -> None:
    template = resolve_template(FlinkJobSpec.demo_windowed_aggregation())
    payload = FlinkJobSpec.demo_windowed_aggregation().model_dump()
    payload["rule_condition"] = 'count values named "quoted"\\path\nnext line'
    spec = FlinkJobSpec.model_validate(payload)
    output_dir = tmp_path / "generated"

    ProjectGenerator(template_dir=template.template_path).generate(spec, output_dir)
    job_path = next((output_dir / "src" / "main" / "java" / "com" / "example").glob("*Job.java"))
    job_text = job_path.read_text(encoding="utf-8")

    assert '\\"quoted\\"' in job_text
    assert '\\\\path' in job_text
    assert '\\nnext line' in job_text


def test_all_output_models_escape_arbitrary_json_control_characters() -> None:
    """Every generated output model must encode controls below U+0020 as JSON escapes."""
    repository_root = Path(__file__).resolve().parents[1]
    model_paths = (
        repository_root / "templates/flink_kafka_rule_job/src/main/java/com/example/model/OutputEvent.java",
        repository_root
        / "templates/flink_windowed_aggregation_job/src/main/java/com/example/model/OutputEvent.java",
        repository_root
        / "templates/flink_session_window_aggregation_job/src/main/java/com/example/model/OutputEvent.java",
    )

    for model_path in model_paths:
        model_text = model_path.read_text(encoding="utf-8")
        assert "character < 0x20" in model_text
        assert 'String.format("\\\\u%04x", (int) character)' in model_text
