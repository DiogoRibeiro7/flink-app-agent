"""Machine-readable report artifact for one generation run."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .constants import GENERATION_REPORT_FILENAME
from .generation_context import GenerationContext
from .review import ReviewResult
from .spec import FlinkJobSpec


REPORT_FILENAME = GENERATION_REPORT_FILENAME


@dataclass(frozen=True)
class StructuralCheckReport:
    """Serializable structural check summary."""

    overall_status: str
    success: bool
    passed_checks: list[str]
    failed_checks: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class GenerationReport:
    """Serializable report for one local generation run."""

    request_text: str
    parsed_spec_summary: dict[str, Any]
    selected_template: str
    output_directory: str
    generated_files_count: int
    generated_files: list[str]
    structural_check: StructuralCheckReport
    warnings: list[str]

    @classmethod
    def from_run(
        cls,
        request_text: str,
        spec: FlinkJobSpec,
        selected_template: str,
        output_directory: Path,
        generated_files: list[Path],
        review_result: ReviewResult,
    ) -> "GenerationReport":
        """Build a deterministic report from one generation run."""
        return cls(
            request_text=request_text,
            parsed_spec_summary=spec.model_dump(),
            selected_template=selected_template,
            output_directory=str(output_directory),
            generated_files_count=len(generated_files),
            generated_files=[str(path) for path in generated_files],
            structural_check=StructuralCheckReport(
                overall_status=review_result.overall_status,
                success=review_result.success,
                passed_checks=list(review_result.passed_checks),
                failed_checks=list(review_result.failed_checks),
                warnings=list(review_result.warnings),
            ),
            warnings=list(review_result.warnings),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the report as a plain JSON-serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_context(cls, context: GenerationContext) -> "GenerationReport":
        """Build a deterministic report from the shared generation context."""
        if context.review_result is None:
            raise ValueError("Generation context does not contain a review result.")

        return cls.from_run(
            request_text=context.request_text,
            spec=context.spec,
            selected_template=context.template.template_id,
            output_directory=context.output_dir,
            generated_files=context.generated_files,
            review_result=context.review_result,
        )


def write_generation_report(output_dir: Path, report: GenerationReport) -> Path:
    """Write the generation report JSON inside the generated project directory."""
    report_path = output_dir / REPORT_FILENAME
    report_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return report_path
