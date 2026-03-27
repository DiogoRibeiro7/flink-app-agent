"""Optional compile-only verification for generated Flink projects."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class VerificationResult:
    """Structured result for one compile-only verification attempt."""

    attempted: bool = False
    success: bool = False
    skipped_reason: str | None = None
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""

    @property
    def overall_status(self) -> str:
        """Return a short status string for reporting."""
        if not self.attempted:
            return "skipped"
        if self.success:
            return "passed"
        return "failed"


@dataclass(frozen=True)
class CompileVerifier:
    """Run an optional Maven compile-only check on generated output.

    This verifier is intentionally limited: it only runs ``mvn compile``
    and captures the result. It does not run tests, package, or deploy.
    """

    timeout_seconds: int = 120

    def verify(self, output_dir: Path) -> VerificationResult:
        """Attempt a compile-only Maven build in the generated project."""
        result = VerificationResult()

        mvn_path = self._find_mvn()
        if mvn_path is None:
            result.skipped_reason = (
                "Maven (mvn) is not available on PATH. "
                "Install Maven to enable compile verification."
            )
            return result

        pom_path = output_dir / "pom.xml"
        if not pom_path.exists():
            result.skipped_reason = (
                f"No pom.xml found in {output_dir}. "
                "Compile verification requires a Maven project."
            )
            return result

        result.attempted = True
        try:
            completed = subprocess.run(
                [mvn_path, "compile", "-B", "-q"],
                cwd=str(output_dir),
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
            result.exit_code = completed.returncode
            result.stdout = completed.stdout
            result.stderr = completed.stderr
            result.success = completed.returncode == 0
        except subprocess.TimeoutExpired:
            result.exit_code = -1
            result.stderr = (
                f"Maven compile timed out after {self.timeout_seconds} seconds."
            )
        except OSError as exc:
            result.exit_code = -1
            result.stderr = f"Failed to run Maven: {exc}"

        return result

    def _find_mvn(self) -> str | None:
        """Return the mvn executable path if available, or None."""
        return shutil.which("mvn")
