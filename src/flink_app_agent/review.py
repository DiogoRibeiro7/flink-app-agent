"""Lightweight deterministic review for generated Flink projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .generator import PLACEHOLDER_PATTERN, SAFE_TEXT_EXTENSIONS, build_main_class_name
from .spec import FlinkJobSpec


@dataclass
class ReviewResult:
    """Structured review outcome for a generated project."""

    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    repairs: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return whether the review completed without failed checks."""
        return not self.failed_checks


@dataclass(frozen=True)
class PostGenerationReviewer:
    """Review generated output for obvious structural issues."""

    safe_text_extensions: frozenset[str] = SAFE_TEXT_EXTENSIONS

    def review(self, output_dir: Path, spec: FlinkJobSpec, repair: bool = True) -> ReviewResult:
        """Review generated output and optionally repair small issues."""
        result = ReviewResult()

        if not output_dir.exists():
            if repair:
                output_dir.mkdir(parents=True, exist_ok=True)
                result.repairs.append(f"Created missing output directory: {output_dir}")
                result.warnings.append("Output directory was missing and has been recreated.")
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

        unresolved = self._repair_or_collect_unresolved(output_dir, repair, result)
        if unresolved:
            unresolved_text = ", ".join(str(path) for path in unresolved)
            result.failed_checks.append(
                f"Unresolved placeholders remain in generated text files: {unresolved_text}"
            )
        else:
            result.passed_checks.append("No unresolved placeholders remain in rendered text files.")

        self._check_expected_topics(expected_paths, spec, result)
        return result

    def _expected_paths(self, output_dir: Path, spec: FlinkJobSpec) -> dict[str, Path]:
        """Build the expected key file paths for the generated output."""
        main_class = build_main_class_name(spec.job_name)
        return {
            "README": output_dir / "README.md",
            "Main Flink job file": output_dir / "src" / "main" / "java" / "com" / "example" / f"{main_class}.java",
        }

    def _repair_or_collect_unresolved(
        self,
        output_dir: Path,
        repair: bool,
        result: ReviewResult,
    ) -> list[Path]:
        """Collect unresolved placeholders and repair trivial trailing marker lines when possible."""
        unresolved_files: list[Path] = []
        for path in self._iter_text_files(output_dir):
            text = path.read_text(encoding="utf-8")
            repaired_text = self._strip_trailing_placeholder_lines(text) if repair else text
            if repaired_text != text:
                path.write_text(repaired_text, encoding="utf-8")
                result.repairs.append(f"Removed trailing placeholder-only lines from: {path}")

            if PLACEHOLDER_PATTERN.search(repaired_text):
                unresolved_files.append(path)

        return unresolved_files

    def _iter_text_files(self, output_dir: Path) -> list[Path]:
        """Return generated text files that are safe to inspect."""
        return sorted(
            path
            for path in output_dir.rglob("*")
            if path.is_file() and path.suffix in self.safe_text_extensions
        )

    def _strip_trailing_placeholder_lines(self, text: str) -> str:
        """Remove trailing lines that only contain unresolved placeholder markers."""
        lines = text.splitlines()
        while lines:
            stripped = lines[-1].strip()
            if stripped and PLACEHOLDER_PATTERN.fullmatch(stripped):
                lines.pop()
                continue
            break

        if not lines:
            return ""
        return "\n".join(lines) + "\n"

    def _check_expected_topics(
        self,
        expected_paths: dict[str, Path],
        spec: FlinkJobSpec,
        result: ReviewResult,
    ) -> None:
        """Check that configured topics appear in the expected generated files."""
        readme_path = expected_paths["README"]
        main_job_path = expected_paths["Main Flink job file"]

        if readme_path.exists():
            readme_text = readme_path.read_text(encoding="utf-8")
            self._assert_contains(readme_text, spec.source_topic, "README contains source topic.", result)
            self._assert_contains(readme_text, spec.sink_topic, "README contains sink topic.", result)

        if main_job_path.exists():
            job_text = main_job_path.read_text(encoding="utf-8")
            self._assert_contains(job_text, spec.source_topic, "Main job file contains source topic.", result)
            self._assert_contains(job_text, spec.sink_topic, "Main job file contains sink topic.", result)

    def _assert_contains(self, text: str, needle: str, label: str, result: ReviewResult) -> None:
        """Record a passed or failed check for expected generated content."""
        if needle in text:
            result.passed_checks.append(label)
        else:
            result.failed_checks.append(f"{label[:-1]} is missing expected value '{needle}'.")
