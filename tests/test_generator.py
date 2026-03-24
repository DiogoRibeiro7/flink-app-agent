"""Tests for template generation."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.spec import FlinkJobSpec


def test_generator_creates_project_from_template(tmp_path: Path) -> None:
    """The generator should copy the template and replace placeholders."""
    repo_root = Path(__file__).resolve().parents[1]
    generator = ProjectGenerator(template_root=repo_root / "templates")
    spec = FlinkJobSpec(
        job_name="Fraud Detector",
        job_class_name="FraudDetector",
        package_name="com.example",
        input_topic="payments",
        output_topic="alerts",
        consumer_group="fraud-group",
        key_field="account_id",
        rule_expression="amount > 5000",
        bootstrap_servers="localhost:9092",
    )

    project_dir = generator.generate(spec=spec, output_dir=tmp_path / "fraud-detector")

    readme_path = project_dir / "README.md"
    job_file_path = project_dir / "src" / "main" / "java" / "com" / "example" / "FraudDetector.java"
    spec_path = project_dir / "job_spec.json"

    assert project_dir.exists()
    assert "Fraud Detector" in readme_path.read_text(encoding="utf-8")
    assert "{{JOB_NAME}}" not in job_file_path.read_text(encoding="utf-8")

    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    assert payload["output_topic"] == "alerts"
