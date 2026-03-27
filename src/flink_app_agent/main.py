"""Command-line entry point for the local flink-app-agent flow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .constants import GENERATION_REPORT_FILENAME
from .generation_context import GenerationContext
from .generator import ProjectGenerator
from .llm import SpecParsingError, build_default_spec_extractor
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
        context = build_generation_context(args.request, output_dir)

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
    except (FileNotFoundError, NotADirectoryError, FileExistsError, SpecParsingError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


def parse_request(request: str) -> FlinkJobSpec:
    """Parse a natural-language request into a validated ``FlinkJobSpec``."""
    extractor = build_default_spec_extractor()
    return extractor.extract_spec(request)


def should_print_template_info(args: argparse.Namespace) -> bool:
    """Return whether the CLI should print resolved template metadata."""
    return args.print_template_info


def build_generation_context(request_text: str, output_dir: Path) -> GenerationContext:
    """Build the small shared context used across the generation pipeline."""
    spec = parse_request(request_text)
    template = resolve_template(spec)
    return GenerationContext(
        request_text=request_text,
        output_dir=output_dir,
        spec=spec,
        template=template,
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


if __name__ == "__main__":
    raise SystemExit(main())
