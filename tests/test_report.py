"""Tests for the machine-readable generation report artifact."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.ambiguity import AmbiguityIssue, AmbiguityAssessment
from flink_app_agent.config import (
    AmbiguityFinding,
    DefaultInjection,
    ExtractionOutcome,
    ProviderQualityFindingRecord,
)
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
    request = "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes, write to Kafka inferred-events"
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
    assert payload["extraction_outcome"]["interpretation_risk"] == "low"
    assert payload["extraction_outcome"]["ambiguity_status"] == "clear"
    assert payload["extraction_outcome"]["ambiguity_policy"] == "fail"
    assert payload["extraction_outcome"]["ambiguity_policy_result"] == "clear"
    assert payload["extraction_outcome"]["ambiguity_findings"] == []
    assert payload["extraction_outcome"]["defaults_injected"] == []
    assert payload["extraction_outcome"]["warnings"] == []
    assert payload["extraction_outcome"]["errors"] == []
    assert "provider_status" not in payload["extraction_outcome"]
    assert "provider_quality" not in payload["extraction_outcome"]
    assert payload["pipeline_status"] == "passed"
    assert payload["failure_stage"] is None
    assert payload["failure_reason"] is None
    assert payload["repair_pass"]["passes_run"] == 0
    assert payload["compile_verification"] is None


def test_deterministic_extraction_report_content_is_compact_and_stable() -> None:
    """Deterministic extraction provenance should be compact and deterministic."""
    request = "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts"
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
        "interpretation_risk": "low",
        "ambiguity_status": "clear",
        "ambiguity_policy": "fail",
        "ambiguity_policy_result": "clear",
        "ambiguity_findings": [],
        "defaults_injected": [],
        "warnings": [],
        "errors": [],
    }
    assert json.dumps(extraction_payload) == (
        '{"selected_mode": "deterministic", "fallback_policy": "fail", '
        '"actual_path": ["deterministic"], "fallback_occurred": false, '
        '"interpretation_risk": "low", '
        '"ambiguity_status": "clear", "ambiguity_policy": "fail", '
        '"ambiguity_policy_result": "clear", "ambiguity_findings": [], '
        '"defaults_injected": [], "warnings": [], "errors": []}'
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
        "provider_quality": "acceptable",
        "provider_quality_summary": "acceptable",
        "provider_quality_findings": [],
        "interpretation_risk": "low",
        "ambiguity_status": "clear",
        "ambiguity_policy": "fail",
        "ambiguity_policy_result": "clear",
        "ambiguity_findings": [],
        "defaults_injected": [],
        "provider_quality_findings": [],
        "warnings": [],
        "errors": [],
    }


def test_fallback_extraction_report_content() -> None:
    """Fallback provenance should retain the attempted provider path and error summary."""
    request = "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts"

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
    assert extraction_payload["fallback_reason"] == "ProviderExtractionError: Provider call failed: provider unreachable"
    assert extraction_payload["provider_status"] == "unavailable"
    assert extraction_payload["provider_quality"] == "unusable"
    assert extraction_payload["provider_quality_summary"] == "Provider call failed: provider unreachable"
    assert extraction_payload["provider_quality_findings"] == []
    assert extraction_payload["interpretation_risk"] == "elevated"
    assert extraction_payload["ambiguity_status"] == "clear"
    assert extraction_payload["ambiguity_policy"] == "fail"
    assert extraction_payload["ambiguity_policy_result"] == "clear"
    assert extraction_payload["ambiguity_findings"] == []
    assert extraction_payload["defaults_injected"] == []
    assert extraction_payload["warnings"] == [
        "Provider extraction failed; deterministic fallback was used.",
    ]
    assert len(extraction_payload["errors"]) == 1
    assert "ProviderExtractionError: Provider call failed: provider unreachable" in extraction_payload["errors"][0]


def test_minor_ambiguity_report_content_reflects_policy_and_defaults() -> None:
    """A report should show when the ambiguity policy injected a safe default."""
    request = "Read from Kafka payments, key by account_id, emit Alert within 10 minutes"
    spec, extraction_outcome = parse_request(
        request,
        extractor_config=ExtractorConfig(
            mode="deterministic",
            ambiguity_policy="minor_defaults",
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
        "selected_mode": "deterministic",
        "fallback_policy": "fail",
        "actual_path": ["deterministic"],
        "fallback_occurred": False,
        "interpretation_risk": "elevated",
        "ambiguity_status": "minor",
        "ambiguity_policy": "minor_defaults",
        "ambiguity_policy_result": "used_safe_defaults",
        "ambiguity_findings": [
            {
                "code": "missing_sink_topic",
                "severity": "minor",
                "message": "The request does not identify a sink topic.",
                "fields": ["sink_topic"],
            }
        ],
        "defaults_injected": [
            {
                "field": "sink_topic",
                "value": "inferred-events",
                "reason": "safe default applied under the active ambiguity policy",
            }
        ],
        "warnings": ["Applied safe default for sink_topic: inferred-events"],
        "errors": [],
    }


def test_failure_report_content_for_ambiguity_related_failure() -> None:
    """A failure report should retain trust/provenance details when generation stops early."""
    extraction_outcome = ExtractionOutcome(
        requested_mode="deterministic",
        fallback_policy="fail",
        extractor_used="deterministic",
        actual_path=("deterministic",),
        ambiguity_status="major",
        ambiguity_policy="fail",
        ambiguity_policy_result="failed",
        ambiguity_findings=(
            AmbiguityFinding(
                code="missing_key_field",
                severity="major",
                message="The request does not identify a key field.",
                fields=("key_by",),
            ),
        ),
        interpretation_risk="elevated",
        errors=("Ambiguous request: missing_key_field",),
    )

    report = GenerationReport.from_failure(
        request_text="Read from Kafka payments, emit Alert within 10 minutes, write to Kafka alerts",
        output_directory=Path("out"),
        extraction_outcome=extraction_outcome,
        failure_stage="extraction",
        failure_reason="Ambiguous request: missing_key_field",
    )

    assert report.to_dict() == {
        "request_text": "Read from Kafka payments, emit Alert within 10 minutes, write to Kafka alerts",
        "job_family": None,
        "parsed_spec_summary": None,
        "selected_template": None,
        "output_directory": "out",
        "generated_files_count": 0,
        "generated_files": [],
        "extraction_outcome": {
            "selected_mode": "deterministic",
            "fallback_policy": "fail",
            "actual_path": ["deterministic"],
            "fallback_occurred": False,
            "interpretation_risk": "elevated",
            "ambiguity_status": "major",
            "ambiguity_policy": "fail",
            "ambiguity_policy_result": "failed",
            "ambiguity_findings": [
                {
                    "code": "missing_key_field",
                    "severity": "major",
                    "message": "The request does not identify a key field.",
                    "fields": ["key_by"],
                }
            ],
            "defaults_injected": [],
            "warnings": [],
            "errors": ["Ambiguous request: missing_key_field"],
        },
        "pipeline_status": "failed_before_generation",
        "failure_stage": "extraction",
        "failure_reason": "Ambiguous request: missing_key_field",
        "repair_pass": {"repairs": [], "passes_run": 0, "any_repairs": False},
        "structural_check": None,
        "compile_verification": None,
        "warnings": [],
    }


def test_report_content_can_combine_fallback_and_injected_defaults() -> None:
    """The provenance block should show fallback and safe defaults together."""
    request = "Read from Kafka payments, key by account_id, emit Alert within 10 minutes"

    def failing_provider(_: str, __: str) -> str:
        raise ConnectionError("provider unreachable")

    spec, extraction_outcome = parse_request(
        request,
        extractor_config=ExtractorConfig(
            mode="provider",
            fallback="deterministic",
            ambiguity_policy="minor_defaults",
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

    assert extraction_payload["actual_path"] == ["provider", "deterministic"]
    assert extraction_payload["fallback_occurred"] is True
    assert extraction_payload["provider_quality"] == "unusable"
    assert extraction_payload["defaults_injected"] == [
        {
            "field": "sink_topic",
            "value": "inferred-events",
            "reason": "safe default applied under the active ambiguity policy",
        }
    ]
    assert extraction_payload["warnings"] == [
        "Provider extraction failed; deterministic fallback was used.",
        "Applied safe default for sink_topic: inferred-events",
    ]


def test_provider_quality_details_appear_in_report() -> None:
    """Provider quality details should be machine-readable when the provider path succeeds."""
    extraction_outcome = ExtractionOutcome(
        requested_mode="provider",
        fallback_policy="fail",
        extractor_used="provider",
        actual_path=("provider",),
        provider_status="available",
        provider_quality="ambiguous",
        provider_quality_summary="Provider output remains ambiguous after normalization: missing_sink_topic",
        provider_quality_findings=(
            ProviderQualityFindingRecord(
                code="ambiguity_present",
                message="Provider output remains ambiguous after normalization: missing_sink_topic",
            ),
        ),
        ambiguity_status="minor",
        ambiguity_policy="minor_defaults",
        ambiguity_policy_result="used_safe_defaults",
        ambiguity_findings=(
            AmbiguityFinding(
                code="missing_sink_topic",
                severity="minor",
                message="The request does not identify a sink topic.",
                fields=("sink_topic",),
            ),
        ),
        default_injections=(
            DefaultInjection(
                field="sink_topic",
                value="inferred-events",
                reason="safe default applied under the active ambiguity policy",
            ),
        ),
        interpretation_risk="elevated",
        warnings=("Applied safe default for sink_topic: inferred-events",),
    )

    report = GenerationReport.from_failure(
        request_text="any request",
        output_directory=Path("out"),
        extraction_outcome=extraction_outcome,
        failure_stage="extraction",
        failure_reason="stopped for test coverage",
    )

    provenance = report.to_dict()["extraction_outcome"]

    assert provenance["provider_quality"] == "ambiguous"
    assert provenance["provider_quality_summary"] == (
        "Provider output remains ambiguous after normalization: missing_sink_topic"
    )
    assert provenance["provider_quality_findings"] == [
        {
            "code": "ambiguity_present",
            "message": "Provider output remains ambiguous after normalization: missing_sink_topic",
            "fields": [],
        }
    ]
