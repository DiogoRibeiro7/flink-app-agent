"""Regression coverage for v0.8 release-blocking review findings."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from flink_app_agent.ambiguity import AmbiguityAssessment, AmbiguityIssue, CandidateAmbiguityAssessor
from flink_app_agent.clarification import build_clarification_questions
from flink_app_agent.generator import ProjectGenerator, TemplateRenderingError
from flink_app_agent.spec import FlinkJobSpec, JOB_FAMILY_KEYED_RULE, JOB_FAMILY_WINDOWED_AGGREGATION


def test_request_family_mismatch_is_reported_for_both_supported_families() -> None:
    assessor = CandidateAmbiguityAssessor()

    keyed = assessor.assess(
        "Read from Kafka events, group by user_id, count events within 5 minutes, write to Kafka out",
        {"job_family": JOB_FAMILY_KEYED_RULE, "key_by": "user_id", "sink_topic": "out", "time_window_minutes": 5},
    )
    aggregated = assessor.assess(
        "Read from Kafka events, key by user_id, emit AlertEvent within 5 minutes, write to Kafka out",
        {"job_family": JOB_FAMILY_WINDOWED_AGGREGATION, "key_by": "user_id", "sink_topic": "out", "time_window_minutes": 5},
    )

    assert any(issue.code == "request_family_mismatch" for issue in keyed.issues)
    assert any(issue.code == "request_family_mismatch" for issue in aggregated.issues)


def test_alternative_key_wording_is_ambiguous_before_generation() -> None:
    assessment = CandidateAmbiguityAssessor().assess(
        "Read from Kafka events, key by user_id or device_id, emit AlertEvent within 5 minutes, write to Kafka out",
        {"job_family": JOB_FAMILY_KEYED_RULE, "key_by": "user_id", "sink_topic": "out", "time_window_minutes": 5},
    )

    assert any(issue.code == "unclear_key_field" for issue in assessment.issues)


def test_request_family_mismatch_has_an_actionable_clarification() -> None:
    assessment = AmbiguityAssessment(
        issues=(AmbiguityIssue(code="request_family_mismatch", message="mismatch"),)
    )

    questions = build_clarification_questions(assessment, payload={})

    assert len(questions) == 1
    assert questions[0].fields == ("job_family", "rule_type")


def test_event_model_names_reject_invalid_java_names_and_collisions() -> None:
    payload = FlinkJobSpec.demo().model_dump()
    payload["input_event_name"] = "123 event"
    with pytest.raises(ValidationError, match="valid non-keyword Java identifier"):
        FlinkJobSpec.model_validate(payload)

    payload = FlinkJobSpec.demo().model_dump()
    payload["input_event_name"] = "class"
    with pytest.raises(ValidationError, match="valid non-keyword Java identifier"):
        FlinkJobSpec.model_validate(payload)

    payload = FlinkJobSpec.demo().model_dump()
    payload["output_event_name"] = "input event"
    with pytest.raises(ValidationError, match="must be different Java class names"):
        FlinkJobSpec.model_validate(payload)


def test_generator_rewrites_public_main_class_name(tmp_path: Path) -> None:
    template = tmp_path / "template"
    source = template / "JobTemplate.java"
    source.parent.mkdir(parents=True)
    source.write_text("public class JobTemplate {}\n", encoding="utf-8")

    output = tmp_path / "generated"
    ProjectGenerator(template_dir=template).generate(FlinkJobSpec.demo(), output)

    generated = (output / "FraudAlertJob.java").read_text(encoding="utf-8")
    assert "public class FraudAlertJob" in generated


def test_generator_rejects_unrecognized_residual_constructor(tmp_path: Path) -> None:
    template = tmp_path / "template"
    source = template / "JobTemplate.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        "public class JobTemplate {\n  JobTemplate /* comment */ () {}\n}\n",
        encoding="utf-8",
    )

    with pytest.raises(TemplateRenderingError, match="unrecognized 'JobTemplate' constructor"):
        ProjectGenerator(template_dir=template).generate(FlinkJobSpec.demo(), tmp_path / "generated")


def test_generator_rejects_unrecognized_main_class_declaration(tmp_path: Path) -> None:
    template = tmp_path / "template"
    source = template / "JobTemplate.java"
    source.parent.mkdir(parents=True)
    source.write_text("public interface JobTemplate {}\n", encoding="utf-8")

    with pytest.raises(TemplateRenderingError, match="class JobTemplate"):
        ProjectGenerator(template_dir=template).generate(FlinkJobSpec.demo(), tmp_path / "generated")
