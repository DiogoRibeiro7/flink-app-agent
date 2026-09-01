"""Tests for the deterministic repair loop."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import ProjectGenerator
from flink_app_agent.repair import DeterministicRepairer
from flink_app_agent.spec import FlinkJobSpec


def test_repair_removes_trailing_placeholder_lines(tmp_path: Path) -> None:
    """Trailing placeholder-only lines should be removed by the repair loop."""
    template_dir = Path(__file__).resolve().parents[1] / "templates" / "flink_kafka_rule_job"
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"
    readme_path = output_dir / "README.md"

    ProjectGenerator(template_dir=template_dir).generate(spec=spec, output_dir=output_dir)
    readme_path.write_text(
        readme_path.read_text(encoding="utf-8") + "\n{{TRAILING_PLACEHOLDER}}\n",
        encoding="utf-8",
    )

    result = DeterministicRepairer().repair(output_dir)

    assert result.any_repairs is True
    assert result.passes_run >= 1
    assert any("Removed trailing placeholder-only lines" in r for r in result.repairs)
    assert "{{TRAILING_PLACEHOLDER}}" not in readme_path.read_text(encoding="utf-8")


def test_repair_adds_missing_final_newline(tmp_path: Path) -> None:
    """Files missing a final newline should get one added."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    java_file = output_dir / "Example.java"
    java_file.write_text("public class Example {}", encoding="utf-8")

    result = DeterministicRepairer().repair(output_dir)

    assert result.any_repairs is True
    assert any("missing final newline" in r for r in result.repairs)
    assert java_file.read_text(encoding="utf-8").endswith("\n")


def test_repair_removes_trailing_whitespace(tmp_path: Path) -> None:
    """Trailing whitespace on lines should be cleaned up."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    java_file = output_dir / "Example.java"
    java_file.write_text("public class Example {}   \n", encoding="utf-8")

    result = DeterministicRepairer().repair(output_dir)

    assert result.any_repairs is True
    assert any("trailing whitespace" in r for r in result.repairs)
    text = java_file.read_text(encoding="utf-8")
    assert "{}   \n" not in text
    assert "{}\n" in text


def test_repair_removes_excess_trailing_blank_lines(tmp_path: Path) -> None:
    """Excess blank lines at the end of files should be collapsed safely."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    java_file = output_dir / "Example.java"
    java_file.write_text("public class Example {}\n\n\n", encoding="utf-8")

    result = DeterministicRepairer().repair(output_dir)

    assert result.any_repairs is True
    assert any("excess trailing blank lines" in r for r in result.repairs)
    assert java_file.read_text(encoding="utf-8") == "public class Example {}\n"


def test_repair_is_idempotent(tmp_path: Path) -> None:
    """Running the repair loop twice should produce no new repairs on the second run."""
    template_dir = Path(__file__).resolve().parents[1] / "templates" / "flink_kafka_rule_job"
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"

    ProjectGenerator(template_dir=template_dir).generate(spec=spec, output_dir=output_dir)

    first_result = DeterministicRepairer().repair(output_dir)
    second_result = DeterministicRepairer().repair(output_dir)

    assert second_result.any_repairs is False
    assert second_result.passes_run == 1


def test_repair_stops_early_when_no_repairs_found(tmp_path: Path) -> None:
    """The loop should stop after one pass when no repairs are needed."""
    template_dir = Path(__file__).resolve().parents[1] / "templates" / "flink_kafka_rule_job"
    spec = FlinkJobSpec.demo()
    output_dir = tmp_path / "generated"

    ProjectGenerator(template_dir=template_dir).generate(spec=spec, output_dir=output_dir)

    result = DeterministicRepairer(max_passes=5).repair(output_dir)

    assert result.passes_run == 1
