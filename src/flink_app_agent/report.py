"""Machine-readable report artifact for one generation run."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import ExtractionOutcome
from .constants import GENERATION_REPORT_FILENAME
from .generation_context import GenerationContext
from .repair import RepairResult
from .review import ReviewResult
from .spec import FlinkJobSpec
from .verification import VerificationResult


REPORT_FILENAME = GENERATION_REPORT_FILENAME


@dataclass(frozen=True)
class ExtractionOutcomeReport:
    """Serializable extraction provenance summary."""

    selected_mode: str
    fallback_policy: str
    actual_path: list[str]
    fallback_occurred: bool
    provider_status: str | None
    warnings: list[str]
    errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Return a compact deterministic extraction provenance dictionary."""
        payload: dict[str, Any] = {
            "selected_mode": self.selected_mode,
            "fallback_policy": self.fallback_policy,
            "actual_path": list(self.actual_path),
            "fallback_occurred": self.fallback_occurred,
        }
        if self.provider_status is not None:
            payload["provider_status"] = self.provider_status
        payload["warnings"] = list(self.warnings)
        payload["errors"] = list(self.errors)
        return payload


@dataclass(frozen=True)
class RepairPassReport:
    """Serializable repair pass summary."""

    repairs: list[str]
    passes_run: int
    any_repairs: bool


@dataclass(frozen=True)
class CompileVerificationReport:
    """Serializable compile verification summary."""

    overall_status: str
    attempted: bool
    success: bool
    skipped_reason: str | None


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
    job_family: str
    parsed_spec_summary: dict[str, Any]
    selected_template: str
    output_directory: str
    generated_files_count: int
    generated_files: list[str]
    extraction_outcome: ExtractionOutcomeReport
    pipeline_status: str
    repair_pass: RepairPassReport
    structural_check: StructuralCheckReport
    compile_verification: CompileVerificationReport | None
    warnings: list[str]

    @classmethod
    def from_run(
        cls,
        request_text: str,
        spec: FlinkJobSpec,
        selected_template: str,
        output_directory: Path,
        generated_files: list[Path],
        extraction_outcome: ExtractionOutcome | None,
        repair_result: RepairResult | None,
        review_result: ReviewResult,
        verification_result: VerificationResult | None,
    ) -> "GenerationReport":
        """Build a deterministic report from one generation run."""
        eo = extraction_outcome or ExtractionOutcome(
            requested_mode="deterministic",
            fallback_policy="fail",
            extractor_used="deterministic",
            actual_path=("deterministic",),
        )
        extraction_report = ExtractionOutcomeReport(
            selected_mode=eo.requested_mode,
            fallback_policy=eo.fallback_policy,
            actual_path=list(eo.actual_path or (eo.extractor_used,)),
            fallback_occurred=eo.fallback_triggered,
            provider_status=eo.provider_status,
            warnings=list(_summarize_extraction_warnings(eo)),
            errors=list(_summarize_extraction_errors(eo)),
        )
        rr = repair_result or RepairResult()
        repair_report = RepairPassReport(
            repairs=list(rr.repairs),
            passes_run=rr.passes_run,
            any_repairs=rr.any_repairs,
        )
        structural_report = StructuralCheckReport(
            overall_status=review_result.overall_status,
            success=review_result.success,
            passed_checks=list(review_result.passed_checks),
            failed_checks=list(review_result.failed_checks),
            warnings=list(review_result.warnings),
        )
        compile_report: CompileVerificationReport | None = None
        if verification_result is not None:
            compile_report = CompileVerificationReport(
                overall_status=verification_result.overall_status,
                attempted=verification_result.attempted,
                success=verification_result.success,
                skipped_reason=verification_result.skipped_reason,
            )
        pipeline_status = _compute_pipeline_status(
            review_result, verification_result,
        )
        return cls(
            request_text=request_text,
            job_family=spec.job_family,
            parsed_spec_summary=spec.model_dump(),
            selected_template=selected_template,
            output_directory=str(output_directory),
            generated_files_count=len(generated_files),
            generated_files=[str(path) for path in generated_files],
            extraction_outcome=extraction_report,
            pipeline_status=pipeline_status,
            repair_pass=repair_report,
            structural_check=structural_report,
            compile_verification=compile_report,
            warnings=list(review_result.warnings),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the report as a plain JSON-serializable dictionary."""
        payload: dict[str, Any] = {
            "request_text": self.request_text,
            "job_family": self.job_family,
            "parsed_spec_summary": self.parsed_spec_summary,
            "selected_template": self.selected_template,
            "output_directory": self.output_directory,
            "generated_files_count": self.generated_files_count,
            "generated_files": list(self.generated_files),
            "extraction_outcome": self.extraction_outcome.to_dict(),
            "pipeline_status": self.pipeline_status,
            "repair_pass": {
                "repairs": list(self.repair_pass.repairs),
                "passes_run": self.repair_pass.passes_run,
                "any_repairs": self.repair_pass.any_repairs,
            },
            "structural_check": {
                "overall_status": self.structural_check.overall_status,
                "success": self.structural_check.success,
                "passed_checks": list(self.structural_check.passed_checks),
                "failed_checks": list(self.structural_check.failed_checks),
                "warnings": list(self.structural_check.warnings),
            },
            "compile_verification": None,
            "warnings": list(self.warnings),
        }
        if self.compile_verification is not None:
            payload["compile_verification"] = {
                "overall_status": self.compile_verification.overall_status,
                "attempted": self.compile_verification.attempted,
                "success": self.compile_verification.success,
                "skipped_reason": self.compile_verification.skipped_reason,
            }
        return payload

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
            extraction_outcome=context.extraction_outcome,
            repair_result=context.repair_result,
            review_result=context.review_result,
            verification_result=context.verification_result,
        )


def _compute_pipeline_status(
    review_result: ReviewResult,
    verification_result: VerificationResult | None,
) -> str:
    """Derive the overall pipeline status from review and verification."""
    if not review_result.success:
        return "failed"
    if verification_result is not None and verification_result.attempted:
        if not verification_result.success:
            return "review_passed_compile_failed"
        return "passed"
    if review_result.warnings:
        return "passed_with_warnings"
    return "passed"


def write_generation_report(output_dir: Path, report: GenerationReport) -> Path:
    """Write the generation report JSON inside the generated project directory."""
    report_path = output_dir / REPORT_FILENAME
    report_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return report_path


def _summarize_extraction_warnings(outcome: ExtractionOutcome) -> tuple[str, ...]:
    """Build a stable warning summary for extraction provenance."""
    if outcome.warnings:
        return outcome.warnings
    if outcome.fallback_triggered and outcome.fallback_reason is not None:
        return (outcome.fallback_reason,)
    return ()


def _summarize_extraction_errors(outcome: ExtractionOutcome) -> tuple[str, ...]:
    """Build a stable error summary for extraction provenance."""
    if outcome.errors:
        return outcome.errors
    if outcome.provider_error is not None:
        return (outcome.provider_error,)
    return ()
