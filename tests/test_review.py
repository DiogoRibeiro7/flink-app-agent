"""Tests for post-generation project review."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator, select_template_for_spec
from flink_app_agent.review import PostGenerationReviewer
from flink_app_agent.spec import FlinkJobSpec


def test_review_passes_for_generated_project(tmp_path: Path) -> None:
    """The reviewer should pass a normally generated project."""
    spec = FlinkJobSpec.demo()
    template = select_template_for_spec(spec, Path(__file__).resolve().parents[1] / "templates")
    output_dir = tmp_path / "generated"
    ProjectGenerator(template_dir=template.template_path).generate(spec, output_dir)

    result = PostGenerationReviewer().review(output_dir, spec)

    assert result.success is True
    assert result.failed_checks == []
    assert any("README exists." == item for item in result.passed_checks)
    assert any("Main Flink job file exists." == item for item in result.passed_checks)


def test_review_fails_when_main_job_file_is_missing(tmp_path: Path) -> None:
    """The reviewer should fail clearly when a key generated file is missing."""
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"
    main_job_path = output_dir / "src" / "main" / "java" / "com" / "example" / "FraudAlertJob.java"
    main_job_path.parent.mkdir(parents=True)
    (output_dir / "README.md").write_text("# fraud-alert-job\n", encoding="utf-8")

    result = PostGenerationReviewer().review(output_dir, spec, repair=False)

    assert result.success is False
    assert any("Main Flink job file is missing" in item for item in result.failed_checks)


def test_review_repairs_trailing_placeholder_only_lines(tmp_path: Path) -> None:
    """The reviewer should repair simple trailing placeholder marker lines."""
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"
    main_job_path = output_dir / "src" / "main" / "java" / "com" / "example" / "FraudAlertJob.java"
    main_job_path.parent.mkdir(parents=True, exist_ok=True)
    (output_dir / "README.md").write_text(
        "# fraud-alert-job\nsource=payments\nsink=alerts\n{{UNUSED_PLACEHOLDER}}\n",
        encoding="utf-8",
    )
    main_job_path.write_text(
        "public class FraudAlertJob { String source = \"payments\"; String sink = \"alerts\"; }\n",
        encoding="utf-8",
    )

    result = PostGenerationReviewer().review(output_dir, spec, repair=True)

    assert result.success is True
    assert any("Removed trailing placeholder-only lines" in item for item in result.repairs)
    assert "{{UNUSED_PLACEHOLDER}}" not in (output_dir / "README.md").read_text(encoding="utf-8")
