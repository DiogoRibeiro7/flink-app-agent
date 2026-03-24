"""Tests for deterministic structural review of generated projects."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.review import StructuralReviewer
from flink_app_agent.spec import FlinkJobSpec


def test_structural_review_passes_for_generated_project(tmp_path: Path) -> None:
    """A normally generated project should pass the structural review."""
    template_dir = Path(__file__).resolve().parents[1] / "templates" / "flink_kafka_rule_job"
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"

    ProjectGenerator(template_dir=template_dir).generate(spec=spec, output_dir=output_dir)
    result = StructuralReviewer().review(output_dir, spec)

    assert result.success is True
    assert result.failed_checks == []
    assert "Output directory exists." in result.passed_checks
    assert "README exists." in result.passed_checks
    assert "Main Flink job file exists." in result.passed_checks


def test_structural_review_fails_when_main_job_file_is_missing(tmp_path: Path) -> None:
    """Missing key generated files should fail the structural review clearly."""
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"
    main_job_path = output_dir / "src" / "main" / "java" / "com" / "example" / "FraudAlertJob.java"
    main_job_path.parent.mkdir(parents=True, exist_ok=True)
    (output_dir / "README.md").write_text("# fraud-alert-job\n", encoding="utf-8")
    main_job_path.unlink(missing_ok=True)

    result = StructuralReviewer().review(output_dir, spec)

    assert result.success is False
    assert any("Main Flink job file is missing" in item for item in result.failed_checks)
