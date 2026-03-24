"""CLI-oriented tests for the local command-line entry point."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.main import main
from flink_app_agent.report import REPORT_FILENAME


def test_main_generates_project_and_prints_summary(
    tmp_path: Path,
    capsys,
) -> None:
    """The CLI should parse a request, generate a project, and print a summary."""
    output_dir = tmp_path / "generated"

    exit_code = main(
        [
            "--request",
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes",
            "--output",
            str(output_dir),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Parsed spec summary:" in captured.out
    assert "Chosen template: flink_kafka_rule_job" in captured.out
    assert f"Generation target: {output_dir}" in captured.out
    assert "Generated files count:" in captured.out
    assert f"Generation report: {output_dir / REPORT_FILENAME}" in captured.out
    assert "Generated files:" in captured.out
    assert "Structural review summary:" in captured.out
    assert "0 failed" in captured.out
    assert str(output_dir / "README.md") in captured.out
    assert (output_dir / REPORT_FILENAME).exists()
    assert output_dir.exists()


def test_main_print_spec_only_exits_before_generation(
    tmp_path: Path,
    capsys,
) -> None:
    """The CLI should support parsing-only mode without generating files."""
    output_dir = tmp_path / "generated"

    exit_code = main(
        [
            "--request",
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes",
            "--print-spec-only",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Parsed spec summary:" in captured.out
    assert "Chosen template:" not in captured.out
    assert "Generated files count:" not in captured.out
    assert not output_dir.exists()


def test_main_print_template_info_exits_before_generation(capsys) -> None:
    """The CLI should print resolved template metadata without generating files."""
    exit_code = main(
        [
            "--request",
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes",
            "--print-template-info",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Parsed spec summary:" in captured.out
    assert "Template info:" in captured.out
    assert "Identifier: flink_kafka_rule_job" in captured.out
    assert "Runtime:" in captured.out
    assert "Generated files count:" not in captured.out


def test_main_requires_output_for_generation(capsys) -> None:
    """The CLI should fail clearly when generation is requested without an output."""
    exit_code = main(
        [
            "--request",
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "--output is required" in captured.err


def test_main_returns_non_zero_on_invalid_request(capsys) -> None:
    """The CLI should return a non-zero code and print an error on parse failure."""
    exit_code = main(
        [
            "--request",
            "Consume sensor-events, key by user_id, emit BED_OUT within 20 minutes",
            "--output",
            "./out",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Error:" in captured.err
    assert "source_topic" in captured.err
