"""End-to-end test for the current narrow local generation flow."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generation_context import GenerationContext
from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.llm import build_default_spec_extractor
from flink_app_agent.main import resolve_template, write_report
from flink_app_agent.report import REPORT_FILENAME
from flink_app_agent.review import StructuralReviewer


def test_sensor_event_request_generates_coherent_project(tmp_path: Path) -> None:
    """A realistic sensor-event request should survive the full local generation flow."""
    request = (
        "Read from Kafka sensor-events, key by user_id, "
        "emit BED_OUT within 20 minutes"
    )

    spec = build_default_spec_extractor().extract_spec(request)
    template = resolve_template(spec)
    output_dir = tmp_path / "sensor-bedout-job"
    context = GenerationContext(
        request_text=request,
        output_dir=output_dir,
        spec=spec,
        template=template,
    )

    generated_files = ProjectGenerator(template_dir=template.template_path).generate(
        spec=spec,
        output_dir=output_dir,
    )
    context.generated_files = generated_files
    context.review_result = StructuralReviewer().review(output_dir=output_dir, spec=spec)
    context.report_path = write_report(context)
    review_result = StructuralReviewer().review(output_dir=output_dir, spec=spec)

    readme_path = output_dir / "README.md"
    pom_path = output_dir / "pom.xml"
    report_path = output_dir / REPORT_FILENAME
    job_path = output_dir / "src" / "main" / "java" / "com" / "example" / "BedoutJob.java"
    output_event_path = (
        output_dir / "src" / "main" / "java" / "com" / "example" / "model" / "BedOut.java"
    )

    assert review_result.success is True
    assert readme_path.exists()
    assert pom_path.exists()
    assert report_path.exists()
    assert job_path.exists()
    assert output_event_path.exists()
    assert readme_path in generated_files
    assert job_path in generated_files

    readme_text = readme_path.read_text(encoding="utf-8")
    job_text = job_path.read_text(encoding="utf-8")
    output_event_text = output_event_path.read_text(encoding="utf-8")

    assert "{{JOB_NAME}}" not in readme_text
    assert "{{SOURCE_TOPIC}}" not in job_text
    assert "{{SINK_TOPIC}}" not in job_text
    assert "{{OUTPUT_EVENT_NAME}}" not in output_event_text

    assert "sensor-events" in readme_text
    assert "inferred-events" in readme_text
    assert "sensor-events" in job_text
    assert "inferred-events" in job_text
    assert "BedOut" in readme_text
    assert "class BedOut" in output_event_text
