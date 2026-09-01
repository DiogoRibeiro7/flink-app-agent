"""Tests for the optional compile-only verification module."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.verification import CompileVerifier, VerificationResult


def test_verification_skips_when_no_pom_xml(tmp_path: Path) -> None:
    """Verification should skip gracefully when no pom.xml exists."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()

    result = CompileVerifier().verify(output_dir)

    assert result.attempted is False
    assert result.success is False
    assert result.overall_status == "skipped"
    assert result.skipped_reason is not None
    assert "pom.xml" in result.skipped_reason


def test_verification_result_status_values() -> None:
    """VerificationResult should report correct status strings."""
    skipped = VerificationResult()
    assert skipped.overall_status == "skipped"

    passed = VerificationResult(attempted=True, success=True)
    assert passed.overall_status == "passed"

    failed = VerificationResult(attempted=True, success=False)
    assert failed.overall_status == "failed"


def test_verification_result_defaults() -> None:
    """Default VerificationResult should be unattempted."""
    result = VerificationResult()

    assert result.attempted is False
    assert result.success is False
    assert result.skipped_reason is None
    assert result.exit_code is None
    assert result.stdout == ""
    assert result.stderr == ""


def test_verification_runs_maven_compile_successfully(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verification should capture a successful Maven compile result."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    (output_dir / "pom.xml").write_text("<project/>", encoding="utf-8")

    monkeypatch.setattr(CompileVerifier, "_find_mvn", lambda self: "mvn")

    def fake_run(*args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="compile ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = CompileVerifier(timeout_seconds=5).verify(output_dir)

    assert result.attempted is True
    assert result.success is True
    assert result.exit_code == 0
    assert result.stdout == "compile ok"
    assert result.stderr == ""
    assert result.overall_status == "passed"


def test_verification_records_maven_compile_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verification should surface a failing Maven compile cleanly."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    (output_dir / "pom.xml").write_text("<project/>", encoding="utf-8")

    monkeypatch.setattr(CompileVerifier, "_find_mvn", lambda self: "mvn")

    def fake_run(*args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="compile failed")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = CompileVerifier().verify(output_dir)

    assert result.attempted is True
    assert result.success is False
    assert result.exit_code == 1
    assert result.stderr == "compile failed"
    assert result.overall_status == "failed"


def test_verification_handles_maven_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verification should return a failed result when Maven times out."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    (output_dir / "pom.xml").write_text("<project/>", encoding="utf-8")

    monkeypatch.setattr(CompileVerifier, "_find_mvn", lambda self: "mvn")

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["mvn", "compile"], timeout=7)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = CompileVerifier(timeout_seconds=7).verify(output_dir)

    assert result.attempted is True
    assert result.success is False
    assert result.exit_code == -1
    assert result.stderr == "Maven compile timed out after 7 seconds."


def test_verification_handles_maven_os_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verification should return a failed result when Maven cannot be executed."""
    output_dir = tmp_path / "project"
    output_dir.mkdir()
    (output_dir / "pom.xml").write_text("<project/>", encoding="utf-8")

    monkeypatch.setattr(CompileVerifier, "_find_mvn", lambda self: "mvn")

    def fake_run(*args, **kwargs):
        raise OSError("access denied")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = CompileVerifier().verify(output_dir)

    assert result.attempted is True
    assert result.success is False
    assert result.exit_code == -1
    assert result.stderr == "Failed to run Maven: access denied"
