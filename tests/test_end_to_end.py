"""End-to-end test for request parsing and project generation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator, select_template_for_spec
from flink_app_agent.llm import FilePromptRepository, SpecExtractionService, StubSpecExtractor


def test_sensor_job_request_generates_expected_project(tmp_path: Path) -> None:
    """A realistic sensor-event request should produce a coherent generated project."""
    request = (
        "Build a Kafka job named sensor occupancy alerts with source topic sensor-events, "
        "sink topic occupancy-alerts, key by room_id, event time field event_ts, "
        "and emit BED_OUT within 20 minutes."
    )
    extraction_service = SpecExtractionService(
        extractor=StubSpecExtractor(),
        prompt_repository=FilePromptRepository(),
    )
    spec = extraction_service.extract(request)

    assert spec.model_dump() == {
        "job_name": "sensor-occupancy-alerts",
        "source_topic": "sensor-events",
        "sink_topic": "occupancy-alerts",
        "key_by": "room_id",
        "event_time_field": "event_ts",
        "input_event_name": "InputEvent",
        "output_event_name": "BedOut",
        "rule_type": "two_events_within_window",
        "rule_condition": "emit BedOut when two keyed events match within 20 minutes",
        "time_window_minutes": 20,
    }

    repo_root = Path(__file__).resolve().parents[1]
    template = select_template_for_spec(spec, repo_root / "templates")
    generator = ProjectGenerator(template_dir=template.template_path)
    output_dir = tmp_path / "sensor-occupancy-alerts"

    generated_files = generator.generate(spec=spec, output_dir=output_dir)

    expected_files = {
        output_dir / "README.md",
        output_dir / "pom.xml",
        output_dir / "src" / "main" / "java" / "com" / "example" / "SensorOccupancyAlertsJob.java",
        output_dir / "src" / "main" / "java" / "com" / "example" / "model" / "InputEvent.java",
        output_dir / "src" / "main" / "java" / "com" / "example" / "model" / "BedOut.java",
        output_dir / "src" / "main" / "java" / "com" / "example" / "functions" / "RuleProcessFunction.java",
        output_dir / "src" / "test" / "java" / "com" / "example" / "RuleProcessFunctionTest.java",
    }

    assert expected_files.issubset(set(generated_files))

    readme_text = (output_dir / "README.md").read_text(encoding="utf-8")
    job_text = (
        output_dir / "src" / "main" / "java" / "com" / "example" / "SensorOccupancyAlertsJob.java"
    ).read_text(encoding="utf-8")
    output_event_text = (
        output_dir / "src" / "main" / "java" / "com" / "example" / "model" / "BedOut.java"
    ).read_text(encoding="utf-8")

    assert "sensor-occupancy-alerts" in readme_text
    assert "sensor-events" in readme_text
    assert "occupancy-alerts" in readme_text
    assert "{{JOB_NAME}}" not in readme_text
    assert "{{SOURCE_TOPIC}}" not in job_text
    assert "{{SINK_TOPIC}}" not in job_text
    assert "{{KEY_BY}}" not in job_text
    assert "{{EVENT_TIME_FIELD}}" not in job_text
    assert "room_id" in job_text
    assert "event_ts" in job_text
    assert "occupancy-alerts" in job_text
    assert "class BedOut" in output_event_text
