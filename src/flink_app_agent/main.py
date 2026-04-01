"""Command-line entry point for the local flink-app-agent flow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pydantic import ValidationError

from .ambiguity import AmbiguousRequestError
from .ambiguity_policy import AmbiguityPolicy
from .config import (
    AmbiguityFinding,
    ConfigurationError,
    DefaultInjection,
    ExtractionOutcome,
    ExtractorConfig,
    ProviderQualityFindingRecord,
    resolve_extractor_config,
)
from .constants import GENERATION_REPORT_FILENAME, ProviderExtractionError
from .generation_context import GenerationContext
from .generator import ProjectGenerator
from .llm import (
    SpecParsingError,
    ExtractionAnalysis,
    SpecExtractionService,
    build_default_extraction_service,
    build_provider_extraction_service,
)
from .provider_quality import (
    PROVIDER_QUALITY_AMBIGUOUS,
    PROVIDER_QUALITY_UNUSABLE,
    ProviderPayloadQualityGate,
)
from .request_taxonomy import (
    REQUEST_CATEGORY_AMBIGUOUS,
    REQUEST_CATEGORY_INVALID,
    REQUEST_CATEGORY_SUPPORTED,
    REQUEST_CATEGORY_UNSUPPORTED,
    UnsupportedRequestError,
)
from .repair import DeterministicRepairer, RepairResult
from .report import GenerationReport, write_generation_report
from .review import ReviewResult, StructuralReviewer
from .spec import FlinkJobSpec
from .template_registry import TemplateDefinition, TemplateRegistry
from .verification import CompileVerifier, VerificationResult


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Parse a Flink job request and generate a local Flink project.",
    )
    parser.add_argument(
        "--request",
        required=True,
        help="Plain-English Flink job request to parse.",
    )
    parser.add_argument(
        "--output",
        help="Directory where the generated project should be written.",
    )
    parser.add_argument(
        "--print-spec-only",
        action="store_true",
        help="Parse and validate the request, print the spec summary, and exit.",
    )
    parser.add_argument(
        "--print-template-info",
        action="store_true",
        help="Resolve the template for the request, print template metadata, and exit.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run optional Maven compile verification on the generated project.",
    )
    parser.add_argument(
        "--extractor",
        choices=["deterministic", "provider"],
        default="deterministic",
        help="Extraction strategy: 'deterministic' (default) or 'provider'.",
    )
    parser.add_argument(
        "--fallback",
        choices=["fail", "deterministic"],
        default=None,
        help=(
            "Fallback policy when provider extraction fails: "
            "'fail' (default, abort on error) or "
            "'deterministic' (fall back to deterministic extractor)."
        ),
    )
    parser.add_argument(
        "--ambiguity-policy",
        choices=["fail", "minor_defaults"],
        default=None,
        help=(
            "How to handle ambiguity: "
            "'fail' (default, abort on ambiguity) or "
            "'minor_defaults' (allow documented safe defaults for minor ambiguity only)."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI flow and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        should_generate = not args.print_spec_only and not args.print_template_info
        if should_generate and args.output is None:
            raise ValueError(
                "--output is required unless using --print-spec-only or --print-template-info."
            )

        output_dir = Path(args.output) if args.output is not None else Path(".")
        extractor_config = resolve_extractor_config(
            cli_extractor=args.extractor,
            cli_fallback=args.fallback,
            cli_ambiguity_policy=args.ambiguity_policy,
        )
        context = build_generation_context(
            args.request, output_dir, extractor_config=extractor_config,
        )

        print_parsed_spec_summary(context.spec)
        if should_print_template_info(args):
            print_template_summary(context.template)
        if not should_generate:
            return 0

        context.generated_files = generate_project(context)
        finalize_generated_project(context, verify=args.verify)

        print_generation_summary(context)
        if context.review_result is not None and not context.review_result.success:
            return 1
        return 0
    except (
        FileNotFoundError,
        NotADirectoryError,
        FileExistsError,
        SpecParsingError,
        AmbiguousRequestError,
        UnsupportedRequestError,
        ProviderExtractionError,
        ConfigurationError,
        ValidationError,
        ValueError,
    ) as exc:
        print(_format_cli_error(exc), file=sys.stderr)
        _print_clarification_questions(exc)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


def parse_request(
    request: str,
    extractor_config: ExtractorConfig | None = None,
) -> tuple[FlinkJobSpec, ExtractionOutcome]:
    """Parse a natural-language request into a validated ``FlinkJobSpec``.

    Returns a tuple of (spec, outcome) so the caller always knows which
    extractor produced the result and whether fallback was triggered.
    """
    config = extractor_config or resolve_extractor_config()
    policy = AmbiguityPolicy(name=config.ambiguity_policy)

    if config.mode != "provider":
        service = build_default_extraction_service()
        return _extract_with_policy(
            request=request,
            extraction_outcome=ExtractionOutcome(
                requested_mode=config.mode,
                fallback_policy=config.fallback,
                extractor_used="deterministic",
                request_category=REQUEST_CATEGORY_SUPPORTED,
                actual_path=("deterministic",),
                ambiguity_policy=config.ambiguity_policy,
            ),
            service=service,
            policy=policy,
        )

    if config.call_provider is None:
        raise ConfigurationError(
            "Provider mode is selected but no call_provider callable was resolved."
        )

    try:
        service = build_provider_extraction_service(config.call_provider)
        analysis = service.analyze(request)
        quality = ProviderPayloadQualityGate().assess(
            payload=analysis.payload,
            ambiguity=analysis.ambiguity,
        )
        if quality.category == PROVIDER_QUALITY_UNUSABLE:
            raise ProviderExtractionError(
                f"Provider output failed quality gate: {quality.summary}"
            )
        return _finalize_analysis_with_policy(
            analysis=analysis,
            validator=service.validator,
            extraction_outcome=ExtractionOutcome(
                requested_mode="provider",
                fallback_policy=config.fallback,
                extractor_used="provider",
                request_category=REQUEST_CATEGORY_SUPPORTED,
                actual_path=("provider",),
                provider_status="available",
                provider_quality=quality.category,
                provider_quality_summary=quality.summary,
                provider_quality_codes=tuple(
                    finding.code for finding in quality.findings
                ),
                provider_quality_findings=tuple(
                    ProviderQualityFindingRecord(
                        code=finding.code,
                        message=finding.message,
                        fields=finding.fields,
                    )
                    for finding in quality.findings
                ),
                ambiguity_policy=config.ambiguity_policy,
            ),
            policy=policy,
        )
    except AmbiguousRequestError:
        raise
    except (ProviderExtractionError, Exception) as exc:
        if config.fallback != "deterministic":
            raise
        error_type = type(exc).__name__
        error_message = str(exc)
        print(
            f"Provider extraction failed, falling back to deterministic: "
            f"{error_type}: {error_message}",
            file=sys.stderr,
        )
        return _extract_with_policy(
            request=request,
            extraction_outcome=ExtractionOutcome(
                requested_mode="provider",
                fallback_policy="deterministic",
                extractor_used="deterministic",
                request_category=REQUEST_CATEGORY_SUPPORTED,
                actual_path=("provider", "deterministic"),
                fallback_triggered=True,
                fallback_reason=f"{error_type}: {error_message}",
                provider_error=error_message,
                provider_status="unavailable",
                provider_quality=PROVIDER_QUALITY_UNUSABLE,
                provider_quality_summary=error_message,
                ambiguity_policy=config.ambiguity_policy,
                warnings=(
                    "Provider extraction failed; deterministic fallback was used.",
                ),
                errors=(f"{error_type}: {error_message}",),
            ),
            service=build_default_extraction_service(),
            policy=policy,
        )


def should_print_template_info(args: argparse.Namespace) -> bool:
    """Return whether the CLI should print resolved template metadata."""
    return args.print_template_info


def _extract_with_policy(
    request: str,
    extraction_outcome: ExtractionOutcome,
    service: SpecExtractionService,
    policy: AmbiguityPolicy,
) -> tuple[FlinkJobSpec, ExtractionOutcome]:
    """Run extraction analysis, apply ambiguity policy, then validate."""
    analysis = service.analyze(request)
    return _finalize_analysis_with_policy(
        analysis=analysis,
        validator=service.validator,
        extraction_outcome=extraction_outcome,
        policy=policy,
    )


def _finalize_analysis_with_policy(
    analysis: ExtractionAnalysis,
    validator,
    extraction_outcome: ExtractionOutcome,
    policy: AmbiguityPolicy,
) -> tuple[FlinkJobSpec, ExtractionOutcome]:
    """Apply policy to an existing extraction analysis and validate it."""
    policy_result = policy.apply(analysis.ambiguity, analysis.payload)
    payload = dict(analysis.payload)
    payload.update(policy_result.applied_defaults)
    spec = validator.validate(payload)
    issue_codes = tuple(item.issue.code for item in policy_result.classified_issues)
    warnings = extraction_outcome.warnings + policy_result.warnings
    ambiguity_warning = "; ".join(policy_result.warnings) if policy_result.warnings else None
    ambiguity_findings = tuple(
        AmbiguityFinding(
            code=item.issue.code,
            severity=item.severity,
            message=item.issue.message,
            fields=item.issue.fields,
        )
        for item in policy_result.classified_issues
    )
    default_injections = tuple(
        DefaultInjection(
            field=field,
            value=value,
            reason="safe default applied under the active ambiguity policy",
        )
        for field, value in sorted(policy_result.applied_defaults.items())
    )
    return spec, ExtractionOutcome(
        requested_mode=extraction_outcome.requested_mode,
        fallback_policy=extraction_outcome.fallback_policy,
        extractor_used=extraction_outcome.extractor_used,
        request_category=extraction_outcome.request_category,
        actual_path=extraction_outcome.actual_path,
        fallback_triggered=extraction_outcome.fallback_triggered,
        fallback_reason=extraction_outcome.fallback_reason,
        provider_error=extraction_outcome.provider_error,
        provider_status=extraction_outcome.provider_status,
        provider_quality=extraction_outcome.provider_quality,
        provider_quality_summary=extraction_outcome.provider_quality_summary,
        provider_quality_codes=extraction_outcome.provider_quality_codes,
        provider_quality_findings=extraction_outcome.provider_quality_findings,
        ambiguity_status=policy_result.classification,
        ambiguity_policy=policy_result.policy_name,
        ambiguity_policy_result=policy_result.status,
        ambiguity_issue_codes=issue_codes,
        ambiguity_warning=ambiguity_warning,
        injected_defaults=tuple(sorted(policy_result.applied_defaults)),
        ambiguity_findings=ambiguity_findings,
        default_injections=default_injections,
        interpretation_risk=_derive_interpretation_risk(
            extraction_outcome=extraction_outcome,
            policy_result_status=policy_result.status,
        ),
        warnings=warnings,
        errors=extraction_outcome.errors,
    )


def _derive_interpretation_risk(
    extraction_outcome: ExtractionOutcome,
    policy_result_status: str,
) -> str:
    """Return a compact trust/risk label for one extraction outcome."""
    if extraction_outcome.provider_quality == PROVIDER_QUALITY_AMBIGUOUS:
        return "elevated"
    if extraction_outcome.fallback_triggered:
        return "elevated"
    if policy_result_status == "used_safe_defaults":
        return "elevated"
    return "low"


def build_generation_context(
    request_text: str,
    output_dir: Path,
    extractor_config: ExtractorConfig | None = None,
) -> GenerationContext:
    """Build the small shared context used across the generation pipeline."""
    spec, extraction_outcome = parse_request(
        request_text,
        extractor_config=extractor_config,
    )
    template = resolve_template(spec)
    return GenerationContext(
        request_text=request_text,
        output_dir=output_dir,
        spec=spec,
        template=template,
        extraction_outcome=extraction_outcome,
    )


def build_template_registry() -> TemplateRegistry:
    """Build the local template registry used by the current CLI."""
    templates_root = Path(__file__).resolve().parents[2] / "templates"
    return TemplateRegistry.from_root(templates_root)


def resolve_template(spec: FlinkJobSpec) -> TemplateDefinition:
    """Resolve the registered template for a validated spec."""
    return build_template_registry().resolve_for_spec(spec)


def generate_project(context: GenerationContext) -> list[Path]:
    """Generate the project from the template resolved in the shared context."""
    generator = ProjectGenerator(template_dir=context.template.template_path)
    return generator.generate(spec=context.spec, output_dir=context.output_dir)


def finalize_generated_project(
    context: GenerationContext,
    verify: bool = False,
) -> None:
    """Finalize one generation run: repair -> review -> verify -> report.

    The pipeline runs in strict order:
    1. Deterministic repair loop for safe fixups
    2. Structural review (checks report exists after first write)
    3. Optional compile-only verification
    4. Final report write
    """
    context.repair_result = repair_project(context)
    context.review_result = review_project(context)
    context.report_path = write_report(context)
    context.review_result = review_project(context)
    if verify:
        context.verification_result = verify_project(context)
    context.report_path = write_report(context)


def repair_project(context: GenerationContext) -> RepairResult:
    """Run the deterministic repair loop on the generated output."""
    repairer = DeterministicRepairer()
    return repairer.repair(output_dir=context.output_dir)


def review_project(context: GenerationContext) -> ReviewResult:
    """Run the deterministic structural review for the current generation context."""
    reviewer = StructuralReviewer()
    return reviewer.review(
        output_dir=context.output_dir,
        spec=context.spec,
    )


def verify_project(context: GenerationContext) -> VerificationResult:
    """Run the optional compile-only verification on the generated output."""
    verifier = CompileVerifier()
    return verifier.verify(output_dir=context.output_dir)


def write_report(context: GenerationContext) -> Path:
    """Write the machine-readable generation report for the current context."""
    report = GenerationReport.from_context(context)
    return write_generation_report(context.output_dir, report)


def print_parsed_spec_summary(spec: FlinkJobSpec) -> None:
    """Print the validated spec in a stable, readable form."""
    print("Parsed spec summary:")
    print(json.dumps(spec.model_dump(), indent=2))
    print()


def print_template_summary(template: TemplateDefinition) -> None:
    """Print a concise summary of the resolved generation template."""
    print("Template info:")
    print(f"- Identifier: {template.template_id}")
    print(f"- Job family: {template.job_family}")
    print(f"- Description: {template.description}")
    print(f"- Runtime: {template.runtime}")
    print(f"- Supported rule types: {', '.join(sorted(template.supported_rule_types))}")
    print(f"- Path: {template.template_path}")
    print()


def print_generation_summary(context: GenerationContext) -> None:
    """Print a concise stable summary for the generation run."""
    review_result = context.review_result
    if review_result is None:
        raise ValueError("Generation context does not contain a review result.")
    if context.report_path is None:
        raise ValueError("Generation context does not contain a report path.")

    outcome = context.extraction_outcome
    print(f"Requested extractor: {outcome.requested_mode}")
    print(f"Request category: {outcome.request_category}")
    print(f"Extraction path: {_format_extraction_path(outcome.actual_path, outcome.extractor_used)}")
    print(f"Fallback occurred: {_format_bool(outcome.fallback_triggered)}")
    if outcome.fallback_triggered and outcome.fallback_reason is not None:
        print(f"Fallback reason: {outcome.fallback_reason}")
    if outcome.provider_quality is not None:
        print(f"Provider quality: {outcome.provider_quality}")
    if outcome.provider_quality_summary is not None:
        print(f"Provider quality summary: {outcome.provider_quality_summary}")
    print(f"Ambiguity status: {outcome.ambiguity_status}")
    print(f"Ambiguity policy: {outcome.ambiguity_policy}")
    print(f"Ambiguity result: {outcome.ambiguity_policy_result}")
    print(f"Injected defaults: {_format_defaults(outcome.injected_defaults)}")
    for item in outcome.warnings:
        print(f"- EXTRACTION WARN: {item}")
    print(f"Job family: {context.spec.job_family}")
    print(f"Chosen template: {context.template.template_id}")
    print(f"Generation target: {context.output_dir}")
    print(f"Generated files count: {len(context.generated_files)}")
    report_name = context.report_path.name if context.report_path is not None else GENERATION_REPORT_FILENAME
    print(f"Generation report: {context.output_dir / report_name}")
    print("Generated files:")
    for path in context.generated_files:
        print(f"- {path}")

    repair_result = context.repair_result
    if repair_result is not None:
        repair_count = len(repair_result.repairs)
        print(
            f"Repair pass: {repair_result.passes_run} passes, "
            f"{repair_count} repairs"
        )
        for item in repair_result.repairs:
            print(f"- REPAIR: {item}")

    print(
        "Structural review summary: "
        f"{review_result.overall_status}, "
        f"{len(review_result.passed_checks)} passed, "
        f"{len(review_result.failed_checks)} failed, "
        f"{len(review_result.warnings)} warnings"
    )
    for item in review_result.passed_checks:
        print(f"- PASS: {item}")
    for item in review_result.failed_checks:
        print(f"- FAIL: {item}")
    for item in review_result.warnings:
        print(f"- WARN: {item}")

    verification_result = context.verification_result
    if verification_result is not None:
        print(f"Compile verification: {verification_result.overall_status}")
        if verification_result.skipped_reason:
            print(f"  Reason: {verification_result.skipped_reason}")
        if verification_result.attempted and not verification_result.success:
            if verification_result.stderr:
                stderr_preview = verification_result.stderr[:500]
                print(f"  Stderr: {stderr_preview}")


def _format_extraction_path(actual_path: tuple[str, ...], extractor_used: str) -> str:
    """Render the extraction path as a short stable arrow-separated string."""
    path = actual_path or (extractor_used,)
    return " -> ".join(path)


def _format_bool(value: bool) -> str:
    """Render booleans in a short scripting-friendly form."""
    return "yes" if value else "no"


def _format_defaults(defaults: tuple[str, ...]) -> str:
    """Render injected defaults in a stable, scripting-friendly form."""
    if not defaults:
        return "no"
    return ", ".join(defaults)


def _format_cli_error(exc: Exception) -> str:
    """Return a stable CLI error prefix for the shared request taxonomy."""
    category = _classify_request_error(exc)
    if category == REQUEST_CATEGORY_AMBIGUOUS:
        return str(exc)
    if category == REQUEST_CATEGORY_UNSUPPORTED:
        return f"Unsupported request: {exc}"
    if category == REQUEST_CATEGORY_INVALID:
        return f"Invalid request: {exc}"
    return f"Error: {exc}"


def _print_clarification_questions(exc: Exception) -> None:
    """Print clarification questions for ambiguous failures when available."""
    questions = getattr(exc, "clarification_questions", ())
    if not questions:
        return
    print("Clarifications needed:", file=sys.stderr)
    for item in questions:
        print(f"- {item.question}", file=sys.stderr)


def _classify_request_error(exc: Exception) -> str | None:
    """Return the shared request category for a known extraction/validation error."""
    category = getattr(exc, "request_category", None)
    if category in {
        REQUEST_CATEGORY_INVALID,
        REQUEST_CATEGORY_AMBIGUOUS,
        REQUEST_CATEGORY_UNSUPPORTED,
    }:
        return category
    if isinstance(exc, ValidationError):
        return REQUEST_CATEGORY_INVALID
    return None


if __name__ == "__main__":
    raise SystemExit(main())
