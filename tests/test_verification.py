"""Tests for the optional compile-only verification module."""

from __future__ import annotations

import sys
from pathlib import Path

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
