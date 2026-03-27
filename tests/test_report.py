"""Tests for the machine-readable generation report artifact."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generation_context import GenerationContext
from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.llm import build_default_spec_extractor
from flink_app_agent.main import parse_request
from flink_app_agent.config import ExtractorConfig
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
            extraction_outcome=None,
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
    assert payload["extraction_outcome"]["selected_mode"] == "deterministic"
    assert payload["extraction_outcome"]["fallback_policy"] == "fail"
    assert payload["extraction_outcome"]["actual_path"] == ["deterministic"]
    assert payload["extraction_outcome"]["fallback_occurred"] is False
    assert payload["extraction_outcome"]["warnings"] == []
    assert payload["extraction_outcome"]["errors"] == []
    assert "provider_status" not in payload["extraction_outcome"]
    assert payload["pipeline_status"] == "passed"
    assert payload["repair_pass"]["passes_run"] == 0
    assert payload["compile_verification"] is None


def test_deterministic_extraction_report_content_is_compact_and_stable() -> None:
    """Deterministic extraction provenance should be compact and deterministic."""
    request = "Read from Kafka payments, key by account_id, emit Alert within 10 minutes"
    spec, extraction_outcome = parse_request(
        request,
        extractor_config=ExtractorConfig(mode="deterministic"),
    )
    report = GenerationReport.from_run(
        request_text=request,
        spec=spec,
        selected_template="flink_kafka_rule_job",
        output_directory=Path("out"),
        generated_files=[],
        extraction_outcome=extraction_outcome,
        repair_result=None,
        review_result=ReviewResult().finalize(),
        verification_result=None,
    )

    extraction_payload = report.to_dict()["extraction_outcome"]

    assert extraction_payload == {
        "selected_mode": "deterministic",
        "fallback_policy": "fail",
        "actual_path": ["deterministic"],
        "fallback_occurred": False,
        "warnings": [],
        "errors": [],
    }
    assert json.dumps(extraction_payload) == (
        '{"selected_mode": "deterministic", "fallback_policy": "fail", '
        '"actual_path": ["deterministic"], "fallback_occurred": false, '
        '"warnings": [], "errors": []}'
    )


def test_provider_backed_extraction_report_content() -> None:
    """Provider-backed extraction should record provider provenance without fallback."""
    request = "any request"

    def provider(_: str, __: str) -> str:
        return json.dumps(
            {
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
            }
        )

    spec, extraction_outcome = parse_request(
        request,
        extractor_config=ExtractorConfig(
            mode="provider",
            fallback="fail",
            call_provider=provider,
        ),
    )
    report = GenerationReport.from_run(
        request_text=request,
        spec=spec,
        selected_template="flink_kafka_rule_job",
        output_directory=Path("out"),
        generated_files=[],
        extraction_outcome=extraction_outcome,
        repair_result=None,
        review_result=ReviewResult().finalize(),
        verification_result=None,
    )

    assert report.to_dict()["extraction_outcome"] == {
        "selected_mode": "provider",
        "fallback_policy": "fail",
        "actual_path": ["provider"],
        "fallback_occurred": False,
        "provider_status": "available",
        "warnings": [],
        "errors": [],
    }


def test_fallback_extraction_report_content() -> None:
    """Fallback provenance should retain the attempted provider path and error summary."""
    request = "Read from Kafka payments, key by account_id, emit Alert within 10 minutes"

    def failing_provider(_: str, __: str) -> str:
        raise ConnectionError("provider unreachable")

    spec, extraction_outcome = parse_request(
        request,
        extractor_config=ExtractorConfig(
            mode="provider",
            fallback="deterministic",
            call_provider=failing_provider,
        ),
    )
    report = GenerationReport.from_run(
        request_text=request,
        spec=spec,
        selected_template="flink_kafka_rule_job",
        output_directory=Path("out"),
        generated_files=[],
        extraction_outcome=extraction_outcome,
        repair_result=None,
        review_result=ReviewResult().finalize(),
        verification_result=None,
    )

    extraction_payload = report.to_dict()["extraction_outcome"]

    assert extraction_payload["selected_mode"] == "provider"
    assert extraction_payload["fallback_policy"] == "deterministic"
    assert extraction_payload["actual_path"] == ["provider", "deterministic"]
    assert extraction_payload["fallback_occurred"] is True
    assert extraction_payload["provider_status"] == "unavailable"
    assert extraction_payload["warnings"] == [
        "Provider extraction failed; deterministic fallback was used.",
    ]
    assert len(extraction_payload["errors"]) == 1
    assert "ProviderExtractionError: Provider call failed: provider unreachable" in extraction_payload["errors"][0]
