"""Tests for local template project generation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import (
    PlaceholderMapping,
    ProjectGenerator,
    TemplateMetadata,
    TemplateCatalog,
    TemplateRenderer,
    TemplateRenderingError,
    TemplateSelectionError,
    select_template_for_spec,
)
from flink_app_agent.spec import ALLOWED_RULE_TYPE, FlinkJobSpec


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


def test_placeholder_mapping_uses_validated_spec_values() -> None:
    """Placeholder mapping should be derived from the spec template dictionary."""
    mapping = PlaceholderMapping(FlinkJobSpec.demo()).as_dict()

    assert mapping["{{JOB_NAME}}"] == "fraud-alert-job"
    assert mapping["{{SOURCE_TOPIC}}"] == "payments"
    assert mapping["{{OUTPUT_EVENT_NAME}}"] == "AlertEvent"


def test_renderer_detects_unresolved_placeholders(tmp_path: Path) -> None:
    """Rendering should fail clearly if a text file still contains placeholders."""
    renderer = TemplateRenderer()
    template_file = tmp_path / "template.txt"
    template_file.write_text("{{JOB_NAME}} {{MISSING_PLACEHOLDER}}", encoding="utf-8")

    with pytest.raises(TemplateRenderingError, match="MISSING_PLACEHOLDER"):
        renderer.render_file(template_file, {"{{JOB_NAME}}": "demo-job"})


def test_generator_rejects_template_with_unresolved_placeholders(tmp_path: Path) -> None:
    """Generation should fail if the copied template still contains unresolved placeholders."""
    template_dir = _create_template(tmp_path / "template")
    (template_dir / "README.md").write_text(
        "# {{JOB_NAME}}\nextra={{UNKNOWN_PLACEHOLDER}}\n",
        encoding="utf-8",
    )
    generator = ProjectGenerator(template_dir=template_dir)

    with pytest.raises(TemplateRenderingError, match="UNKNOWN_PLACEHOLDER"):
        generator.generate(FlinkJobSpec.demo(), tmp_path / "generated")


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


def test_template_catalog_builds_registered_template_metadata(tmp_path: Path) -> None:
    """Template registration should be explicit even with one real template."""
    catalog = TemplateCatalog.from_root(tmp_path)
    template = catalog.get("flink_kafka_rule_job")

    assert template.name == "flink_kafka_rule_job"
    assert template.template_path == tmp_path / "flink_kafka_rule_job"
    assert template.supported_rule_types == frozenset({ALLOWED_RULE_TYPE})


def test_select_template_for_spec_returns_registered_template(tmp_path: Path) -> None:
    """Specs should select the matching registered template explicitly."""
    template = select_template_for_spec(FlinkJobSpec.demo(), tmp_path)

    assert template.name == "flink_kafka_rule_job"
    assert template.template_path == tmp_path / "flink_kafka_rule_job"


def test_template_catalog_rejects_unknown_template_name(tmp_path: Path) -> None:
    """Unknown template names should fail with a clear selection error."""
    catalog = TemplateCatalog.from_root(tmp_path)

    with pytest.raises(TemplateSelectionError, match="Unknown template"):
        catalog.get("missing-template")


def test_template_catalog_rejects_unsupported_rule_type(tmp_path: Path) -> None:
    """Rule types with no matching template should fail selection."""
    unsupported_spec = FlinkJobSpec.model_construct(
        job_name="unsupported-job",
        source_topic="payments",
        sink_topic="alerts",
        key_by="account_id",
        event_time_field="event_time",
        input_event_name="InputEvent",
        output_event_name="AlertEvent",
        rule_type="unsupported_rule",
        rule_condition="demo",
        time_window_minutes=5,
    )
    catalog = TemplateCatalog(
        templates=(
            TemplateMetadata(
                name="flink_kafka_rule_job",
                template_path=tmp_path / "flink_kafka_rule_job",
                supported_rule_types=frozenset({ALLOWED_RULE_TYPE}),
            ),
        )
    )

    with pytest.raises(TemplateSelectionError, match="supports rule_type"):
        catalog.select_for_spec(unsupported_spec)


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
