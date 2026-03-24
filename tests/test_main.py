"""CLI-oriented tests for the v0.1 command-line entry point."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.main import main


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
    assert "Parsed spec:" in captured.out
    assert "Generated files:" in captured.out
    assert str(output_dir / "README.md") in captured.out
    assert output_dir.exists()


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
