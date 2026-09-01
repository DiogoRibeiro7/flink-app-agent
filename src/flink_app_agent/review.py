"""Deterministic review module for generated Flink projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .constants import GENERATION_REPORT_FILENAME
from .generator import PLACEHOLDER_PATTERN, SAFE_TEXT_EXTENSIONS, build_main_class_name
from .spec import FlinkJobSpec, JOB_FAMILY_WINDOWED_AGGREGATION


REPORT_FILENAME = GENERATION_REPORT_FILENAME


@dataclass
class ReviewResult:
    """Structured result for deterministic post-generation review."""

    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    overall_status: str = "passed"

    @property
    def success(self) -> bool:
        """Return whether all structural checks passed."""
        return not self.failed_checks

    def finalize(self) -> "ReviewResult":
        """Update the overall status from the current result contents."""
        if self.failed_checks:
            self.overall_status = "failed"
        elif self.warnings:
            self.overall_status = "passed_with_warnings"
        else:
            self.overall_status = "passed"
        return self


@dataclass(frozen=True)
class StructuralReviewer:
    """Run a small deterministic file-based review on generated output."""

    text_extensions: frozenset[str] = SAFE_TEXT_EXTENSIONS

    def review(
        self,
        output_dir: Path,
        spec: FlinkJobSpec,
    ) -> ReviewResult:
        """Review the generated project for obvious structural issues."""
        result = ReviewResult()

        if not output_dir.exists():
            result.failed_checks.append(f"Output directory does not exist: {output_dir}")
            return result.finalize()

        if not output_dir.is_dir():
            result.failed_checks.append(f"Output path is not a directory: {output_dir}")
            return result.finalize()

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

        self._check_expected_topics(expected_paths, spec, result)
        self._check_family_function_file(output_dir, spec, result)
        self._check_job_family_in_readme(expected_paths, spec, result)
        self._check_optional_test_scaffold(output_dir, spec, result)

        return result.finalize()

    def _expected_paths(self, output_dir: Path, spec: FlinkJobSpec) -> dict[str, Path]:
        """Return key file paths expected in generated output."""
        main_class_name = build_main_class_name(spec.job_name)
        return {
            "README": output_dir / "README.md",
            "Generation report": output_dir / REPORT_FILENAME,
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

    def _check_expected_topics(
        self,
        expected_paths: dict[str, Path],
        spec: FlinkJobSpec,
        result: ReviewResult,
    ) -> None:
        """Check that configured topics appear in key generated files."""
        readme_path = expected_paths["README"]
        main_job_path = expected_paths["Main Flink job file"]

        if readme_path.exists():
            readme_text = readme_path.read_text(encoding="utf-8")
            self._assert_contains(readme_text, spec.source_topic, "README contains source topic.", result)
            self._assert_contains(readme_text, spec.sink_topic, "README contains sink topic.", result)

        if main_job_path.exists():
            job_text = main_job_path.read_text(encoding="utf-8")
            self._assert_contains(job_text, spec.source_topic, "Main Flink job file contains source topic.", result)
            self._assert_contains(job_text, spec.sink_topic, "Main Flink job file contains sink topic.", result)

    def _check_family_function_file(
        self,
        output_dir: Path,
        spec: FlinkJobSpec,
        result: ReviewResult,
    ) -> None:
        """Check that the expected family-specific function file exists."""
        functions_dir = (
            output_dir / "src" / "main" / "java" / "com" / "example" / "functions"
        )
        if spec.job_family == JOB_FAMILY_WINDOWED_AGGREGATION:
            expected_name = "WindowedCountProcessWindowFunction.java"
        else:
            expected_name = "RuleProcessFunction.java"
        function_file = functions_dir / expected_name
        if function_file.exists():
            result.passed_checks.append("Family-specific function file exists.")
        else:
            result.failed_checks.append(
                f"Family-specific function file is missing: {function_file}"
            )

    def _check_job_family_in_readme(
        self,
        expected_paths: dict[str, Path],
        spec: FlinkJobSpec,
        result: ReviewResult,
    ) -> None:
        """Check that the job family value appears in the generated README."""
        readme_path = expected_paths["README"]
        if readme_path.exists():
            readme_text = readme_path.read_text(encoding="utf-8")
            self._assert_contains(
                readme_text,
                spec.job_family,
                "README contains job family.",
                result,
            )

    def _check_optional_test_scaffold(
        self,
        output_dir: Path,
        spec: FlinkJobSpec,
        result: ReviewResult,
    ) -> None:
        """Record a warning if the generated test scaffold is missing."""
        if spec.job_family == JOB_FAMILY_WINDOWED_AGGREGATION:
            test_file_name = "WindowedCountProcessWindowFunctionTest.java"
        else:
            test_file_name = "RuleProcessFunctionTest.java"
        test_file = output_dir / "src" / "test" / "java" / "com" / "example" / test_file_name
        if test_file.exists():
            result.passed_checks.append("Generated test scaffold exists.")
        else:
            result.warnings.append(f"Generated test scaffold is missing: {test_file}")

    def _assert_contains(self, text: str, needle: str, label: str, result: ReviewResult) -> None:
        """Record a pass or failure for expected generated content."""
        if needle in text:
            result.passed_checks.append(label)
        else:
            result.failed_checks.append(f"{label[:-1]} is missing expected value '{needle}'.")
