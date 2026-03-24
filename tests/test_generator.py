"""Tests for local template project generation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.spec import FlinkJobSpec


def test_generator_copies_template_replaces_text_and_renames_classes(tmp_path: Path) -> None:
    """The generator should copy files, fill placeholders, and rename template classes."""
    template_dir = _create_template(tmp_path / "template")
    output_dir = tmp_path / "generated" / "fraud-alert-job"
    generator = ProjectGenerator(template_dir=template_dir)

    generated_files = generator.generate(FlinkJobSpec.demo(), output_dir)

    readme_path = output_dir / "README.md"
    job_file_path = output_dir / "src" / "main" / "java" / "FraudAlertJob.java"
    input_file_path = output_dir / "src" / "main" / "java" / "PaymentEvent.java"
    output_file_path = output_dir / "src" / "main" / "java" / "AlertEvent.java"
    binary_file_path = output_dir / "assets" / "logo.bin"

    assert readme_path in generated_files
    assert job_file_path in generated_files
    assert input_file_path in generated_files
    assert output_file_path in generated_files
    assert "fraud-alert-job" in readme_path.read_text(encoding="utf-8")
    assert "{{JOB_NAME}}" not in readme_path.read_text(encoding="utf-8")
    assert "PaymentEvent" in input_file_path.read_text(encoding="utf-8")
    assert "AlertEvent" in output_file_path.read_text(encoding="utf-8")
    assert binary_file_path.read_bytes() == b"\x00{{JOB_NAME}}\x01"


def test_generator_rejects_missing_template_directory(tmp_path: Path) -> None:
    """A missing template directory should fail clearly."""
    generator = ProjectGenerator(template_dir=tmp_path / "missing-template")

    with pytest.raises(FileNotFoundError, match="Template directory not found"):
        generator.generate(FlinkJobSpec.demo(), tmp_path / "generated")


def test_generator_rejects_existing_output_directory(tmp_path: Path) -> None:
    """An existing output path should not be overwritten."""
    template_dir = _create_template(tmp_path / "template")
    output_dir = tmp_path / "generated"
    output_dir.mkdir()
    generator = ProjectGenerator(template_dir=template_dir)

    with pytest.raises(FileExistsError, match="Output path already exists"):
        generator.generate(FlinkJobSpec.demo(), output_dir)


def test_generator_rejects_output_path_with_file_parent(tmp_path: Path) -> None:
    """A file used as the parent output path should fail validation."""
    template_dir = _create_template(tmp_path / "template")
    invalid_parent = tmp_path / "not-a-directory"
    invalid_parent.write_text("content", encoding="utf-8")
    generator = ProjectGenerator(template_dir=template_dir)

    with pytest.raises(NotADirectoryError, match="Output parent is not a directory"):
        generator.generate(FlinkJobSpec.demo(), invalid_parent / "generated")


def _create_template(template_dir: Path) -> Path:
    """Create a small local template tree for generator tests."""
    (template_dir / "src" / "main" / "java").mkdir(parents=True)
    (template_dir / "assets").mkdir(parents=True)

    (template_dir / "README.md").write_text(
        "# {{JOB_NAME}}\nsource={{SOURCE_TOPIC}}\nsink={{SINK_TOPIC}}\n",
        encoding="utf-8",
    )
    (template_dir / "src" / "main" / "java" / "JobTemplate.java").write_text(
        (
            "public class JobTemplate {\n"
            "  String ruleType = \"{{RULE_TYPE}}\";\n"
            "  String ruleCondition = \"{{RULE_CONDITION}}\";\n"
            "  String keyBy = \"{{KEY_BY}}\";\n"
            "  String eventTimeField = \"{{EVENT_TIME_FIELD}}\";\n"
            "  int windowMinutes = {{TIME_WINDOW_MINUTES}};\n"
            "}\n"
        ),
        encoding="utf-8",
    )
    (template_dir / "src" / "main" / "java" / "InputEvent.java").write_text(
        "public class {{INPUT_EVENT_NAME}} {}\n",
        encoding="utf-8",
    )
    (template_dir / "src" / "main" / "java" / "OutputEvent.java").write_text(
        "public class {{OUTPUT_EVENT_NAME}} {}\n",
        encoding="utf-8",
    )
    (template_dir / "assets" / "logo.bin").write_bytes(b"\x00{{JOB_NAME}}\x01")
    return template_dir
