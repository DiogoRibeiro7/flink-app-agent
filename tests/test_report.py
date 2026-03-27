"""Tests for the machine-readable generation report artifact."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generation_context import GenerationContext
from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.llm import build_default_spec_extractor
from flink_app_agent.report import REPORT_FILENAME, write_generation_report, GenerationReport
from flink_app_agent.review import ReviewResult, StructuralReviewer
from flink_app_agent.template_registry import TemplateRegistry


def test_generation_report_file_is_created_with_key_fields(tmp_path: Path) -> None:
    """The generation report should be written into the generated project directory."""
    request = "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes"
    spec = build_default_spec_extractor().extract_spec(request)
    template = TemplateRegistry.from_root(Path(__file__).resolve().parents[1] / "templates").resolve_for_spec(spec)
    output_dir = tmp_path / "generated"
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
    write_generation_report(
        output_dir,
        GenerationReport.from_run(
            request_text=request,
            spec=spec,
            selected_template=template.template_id,
            output_directory=output_dir,
            generated_files=generated_files,
            repair_result=None,
            review_result=ReviewResult().finalize(),
            verification_result=None,
        ),
    )
    review_result = StructuralReviewer().review(output_dir=output_dir, spec=spec)
    context.review_result = review_result
    report = GenerationReport.from_context(context)
    report_path = write_generation_report(output_dir, report)
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert report_path == output_dir / REPORT_FILENAME
    assert payload["request_text"] == request
    assert payload["job_family"] == "keyed_temporal_rule"
    assert payload["selected_template"] == "flink_kafka_rule_job"
    assert payload["parsed_spec_summary"]["source_topic"] == "sensor-events"
    assert payload["generated_files_count"] == len(generated_files)
    assert any(path.endswith("README.md") for path in payload["generated_files"])
    assert payload["structural_check"]["success"] is True
    assert payload["pipeline_status"] == "passed"
    assert payload["repair_pass"]["passes_run"] == 0
    assert payload["compile_verification"] is None
