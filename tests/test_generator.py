"""Tests for the local template generator."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator, TemplateRenderingError
from flink_app_agent.spec import FlinkJobSpec


def test_generator_copies_template_files(tmp_path: Path) -> None:
    """The generator should copy the template tree into the requested output directory."""
    template_dir = _create_template(tmp_path / "template")
    output_dir = tmp_path / "generated" / "fraud-alert-job"

    generated_files = ProjectGenerator(template_dir=template_dir).generate(
        spec=FlinkJobSpec.demo(),
        output_dir=output_dir,
    )

    assert output_dir.exists()
    assert (output_dir / "README.md") in generated_files
    assert (output_dir / "pom.xml") in generated_files


def test_generator_replaces_placeholders(tmp_path: Path) -> None:
    """The generator should replace supported placeholders in text files."""
    template_dir = _create_template(tmp_path / "template")
    output_dir = tmp_path / "generated" / "fraud-alert-job"

    generated_files = ProjectGenerator(template_dir=template_dir).generate(
        spec=FlinkJobSpec.demo(),
        output_dir=output_dir,
    )

    readme_text = (output_dir / "README.md").read_text(encoding="utf-8")
    job_text = (
        output_dir / "src" / "main" / "java" / "com" / "example" / "FraudAlertJob.java"
    ).read_text(encoding="utf-8")
    output_event_text = (
        output_dir / "src" / "main" / "java" / "com" / "example" / "model" / "AlertEvent.java"
    ).read_text(encoding="utf-8")
    binary_content = (output_dir / "assets" / "logo.bin").read_bytes()

    assert "{{JOB_NAME}}" not in readme_text
    assert "fraud-alert-job" in readme_text
    assert "payments" in job_text
    assert "alerts" in job_text
    assert "account_id" in job_text
    assert "second payment occurs within 10 minutes" in job_text
    assert "class AlertEvent" in output_event_text
    assert b"{{JOB_NAME}}" in binary_content
    assert (output_dir / "src" / "main" / "java" / "com" / "example" / "FraudAlertJob.java") in generated_files


def test_generator_fails_clearly_when_template_path_is_missing(tmp_path: Path) -> None:
    """A missing template directory should raise a clear error."""
    generator = ProjectGenerator(template_dir=tmp_path / "missing-template")

    with pytest.raises(FileNotFoundError, match="Template directory not found"):
        generator.generate(spec=FlinkJobSpec.demo(), output_dir=tmp_path / "generated")


def test_generator_fails_when_unresolved_placeholders_remain(tmp_path: Path) -> None:
    """Unresolved placeholders in text files should fail generation clearly."""
    template_dir = _create_template(tmp_path / "template")
    (template_dir / "README.md").write_text(
        "# {{JOB_NAME}}\nextra={{UNKNOWN_PLACEHOLDER}}\n",
        encoding="utf-8",
    )

    generator = ProjectGenerator(template_dir=template_dir)

    with pytest.raises(TemplateRenderingError, match="UNKNOWN_PLACEHOLDER"):
        generator.generate(spec=FlinkJobSpec.demo(), output_dir=tmp_path / "generated")


def test_generator_rejects_invalid_output_parent(tmp_path: Path) -> None:
    """A file used as the parent output path should fail clearly."""
    template_dir = _create_template(tmp_path / "template")
    invalid_parent = tmp_path / "not-a-directory"
    invalid_parent.write_text("content", encoding="utf-8")

    generator = ProjectGenerator(template_dir=template_dir)

    with pytest.raises(NotADirectoryError, match="Output parent is not a directory"):
        generator.generate(spec=FlinkJobSpec.demo(), output_dir=invalid_parent / "generated")


def _create_template(template_dir: Path) -> Path:
    """Create a small local template tree for generator tests."""
    (template_dir / "src" / "main" / "java" / "com" / "example" / "model").mkdir(parents=True)
    (template_dir / "assets").mkdir(parents=True)

    (template_dir / "README.md").write_text(
        "# {{JOB_NAME}}\nsource={{SOURCE_TOPIC}}\nsink={{SINK_TOPIC}}\n",
        encoding="utf-8",
    )
    (template_dir / "pom.xml").write_text("<artifactId>{{JOB_NAME}}</artifactId>\n", encoding="utf-8")
    (template_dir / "src" / "main" / "java" / "com" / "example" / "JobTemplate.java").write_text(
        (
            "public class JobTemplate {\n"
            "  String sourceTopic = \"{{SOURCE_TOPIC}}\";\n"
            "  String sinkTopic = \"{{SINK_TOPIC}}\";\n"
            "  String keyBy = \"{{KEY_BY}}\";\n"
            "  String eventTimeField = \"{{EVENT_TIME_FIELD}}\";\n"
            "  String ruleType = \"{{RULE_TYPE}}\";\n"
            "  String ruleCondition = \"{{RULE_CONDITION}}\";\n"
            "  int windowMinutes = {{TIME_WINDOW_MINUTES}};\n"
            "}\n"
        ),
        encoding="utf-8",
    )
    (template_dir / "src" / "main" / "java" / "com" / "example" / "model" / "InputEvent.java").write_text(
        "public class {{INPUT_EVENT_NAME}} {}\n",
        encoding="utf-8",
    )
    (template_dir / "src" / "main" / "java" / "com" / "example" / "model" / "OutputEvent.java").write_text(
        "public class {{OUTPUT_EVENT_NAME}} {}\n",
        encoding="utf-8",
    )
    (template_dir / "assets" / "logo.bin").write_bytes(b"\x00{{JOB_NAME}}\x01")
    return template_dir
