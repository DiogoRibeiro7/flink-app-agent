"""Deterministic structural checks for generated Flink projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .generator import PLACEHOLDER_PATTERN, SAFE_TEXT_EXTENSIONS, build_main_class_name
from .spec import FlinkJobSpec


@dataclass
class ReviewResult:
    """Structured result for post-generation structural checks."""

    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return whether all structural checks passed."""
        return not self.failed_checks


@dataclass(frozen=True)
class StructuralReviewer:
    """Run a small deterministic structural review on generated output."""

    text_extensions: frozenset[str] = SAFE_TEXT_EXTENSIONS

    def review(self, output_dir: Path, spec: FlinkJobSpec) -> ReviewResult:
        """Review the generated project for a few obvious structural issues."""
        result = ReviewResult()

        if not output_dir.exists():
            result.failed_checks.append(f"Output directory does not exist: {output_dir}")
            return result

        if not output_dir.is_dir():
            result.failed_checks.append(f"Output path is not a directory: {output_dir}")
            return result

        result.passed_checks.append("Output directory exists.")

        expected_paths = self._expected_paths(output_dir, spec)
        for label, path in expected_paths.items():
            if path.exists():
                result.passed_checks.append(f"{label} exists.")
            else:
                result.failed_checks.append(f"{label} is missing: {path}")

        unresolved_files = self._find_unresolved_placeholder_files(output_dir)
        if unresolved_files:
            unresolved_text = ", ".join(str(path) for path in unresolved_files)
            result.failed_checks.append(
                f"Unresolved placeholders remain in generated text files: {unresolved_text}"
            )
        else:
            result.passed_checks.append("No unresolved placeholders remain in generated text files.")

        if not expected_paths["README"].exists():
            result.warnings.append("Generated project is missing README context.")

        return result

    def _expected_paths(self, output_dir: Path, spec: FlinkJobSpec) -> dict[str, Path]:
        """Return key file paths expected in generated output."""
        main_class_name = build_main_class_name(spec.job_name)
        return {
            "README": output_dir / "README.md",
            "Main Flink job file": (
                output_dir
                / "src"
                / "main"
                / "java"
                / "com"
                / "example"
                / f"{main_class_name}.java"
            ),
        }

    def _find_unresolved_placeholder_files(self, output_dir: Path) -> list[Path]:
        """Return generated text files that still contain unresolved placeholders."""
        unresolved_files: list[Path] = []
        for path in self._iter_text_files(output_dir):
            text = path.read_text(encoding="utf-8")
            if PLACEHOLDER_PATTERN.search(text):
                unresolved_files.append(path)
        return unresolved_files

    def _iter_text_files(self, output_dir: Path) -> list[Path]:
        """Yield generated files that are safe to inspect as text."""
        return sorted(
            path
            for path in output_dir.rglob("*")
            if path.is_file() and path.suffix in self.text_extensions
        )
