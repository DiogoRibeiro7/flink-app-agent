"""CLI-oriented tests for the local command-line entry point."""

from __future__ import annotations

import textwrap
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.config import PROVIDER_ENTRY_POINT_ENV_VAR
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
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes, write to Kafka inferred-events",
            "--output",
            str(output_dir),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Parsed spec summary:" in captured.out
    assert "Requested extractor: deterministic" in captured.out
    assert "Extraction path: deterministic" in captured.out
    assert "Fallback occurred: no" in captured.out
    assert "Job family: keyed_temporal_rule" in captured.out
    assert "Chosen template: flink_kafka_rule_job" in captured.out
    assert f"Generation target: {output_dir}" in captured.out
    assert "Generated files count:" in captured.out
    assert f"Generation report: {output_dir / REPORT_FILENAME}" in captured.out
    assert "Generated files:" in captured.out
    assert "Repair pass:" in captured.out
    assert "Structural review summary:" in captured.out
    assert "0 failed" in captured.out
    assert str(output_dir / "README.md") in captured.out
    assert (output_dir / REPORT_FILENAME).exists()
    assert output_dir.exists()


def test_main_generates_windowed_aggregation_project(tmp_path: Path, capsys) -> None:
    """The CLI should support the second registered template family."""
    output_dir = tmp_path / "aggregation-generated"

    exit_code = main(
        [
            "--request",
            "Read from Kafka sensor-events, group by device_id, count events within 5 minutes, write to Kafka aggregated-events",
            "--output",
            str(output_dir),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Job family: windowed_aggregation" in captured.out
    assert "Chosen template: flink_windowed_aggregation_job" in captured.out
    assert "Requested extractor: deterministic" in captured.out
    assert "Extraction path: deterministic" in captured.out
    assert "Fallback occurred: no" in captured.out
    assert f"Generation report: {output_dir / REPORT_FILENAME}" in captured.out
    assert (output_dir / REPORT_FILENAME).exists()


def test_main_print_spec_only_exits_before_generation(
    tmp_path: Path,
    capsys,
) -> None:
    """The CLI should support parsing-only mode without generating files."""
    output_dir = tmp_path / "generated"

    exit_code = main(
        [
            "--request",
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes, write to Kafka inferred-events",
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
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes, write to Kafka inferred-events",
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
            "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes, write to Kafka inferred-events",
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


def test_main_provider_mode_prints_provider_path(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI should report provider mode clearly when provider extraction succeeds."""
    provider_module = tmp_path / "cli_provider_success.py"
    provider_module.write_text(
        textwrap.dedent("""\
            import json

            def call_provider(request: str, prompt: str) -> str:
                return json.dumps({
                    "job_family": "keyed_temporal_rule",
                    "job_name": "fraud-alert-job",
                    "source_topic": "payments",
                    "sink_topic": "alerts",
                    "key_by": "account_id",
                    "event_time_field": "event_time",
                    "input_event_name": "InputEvent",
                    "output_event_name": "AlertEvent",
                    "rule_type": "two_events_within_window",
                    "rule_condition": "second payment within 10 minutes",
                    "time_window_minutes": 10,
                })
        """),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "cli_provider_success:call_provider")

    output_dir = tmp_path / "provider-generated"
    exit_code = main(
        [
            "--request",
            "any request",
            "--output",
            str(output_dir),
            "--extractor",
            "provider",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Requested extractor: provider" in captured.out
    assert "Extraction path: provider" in captured.out
    assert "Fallback occurred: no" in captured.out
    assert "Fallback reason:" not in captured.out


def test_main_provider_fallback_prints_fallback_summary(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI should show the actual fallback path when provider extraction degrades."""
    provider_module = tmp_path / "cli_provider_fallback.py"
    provider_module.write_text(
        textwrap.dedent("""\
            def call_provider(request: str, prompt: str) -> str:
                raise ConnectionError("provider unreachable")
        """),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "cli_provider_fallback:call_provider")

    output_dir = tmp_path / "provider-fallback-generated"
    exit_code = main(
        [
            "--request",
            "Read from Kafka payments, key by account_id, emit Alert within 10 minutes, write to Kafka alerts",
            "--output",
            str(output_dir),
            "--extractor",
            "provider",
            "--fallback",
            "deterministic",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Requested extractor: provider" in captured.out
    assert "Extraction path: provider -> deterministic" in captured.out
    assert "Fallback occurred: yes" in captured.out
    assert "Fallback reason: ProviderExtractionError: Provider call failed: provider unreachable" in captured.out
    assert "Provider extraction failed, falling back to deterministic:" in captured.err
