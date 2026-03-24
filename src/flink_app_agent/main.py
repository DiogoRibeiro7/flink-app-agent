"""Command-line entry point for the v0.1 flink-app-agent flow."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from .generator import ProjectGenerator
from .llm import SpecParsingError, build_default_spec_extractor
from .review import ReviewResult, StructuralReviewer
from .spec import FlinkJobSpec


@dataclass(frozen=True)
class GenerationSummary:
    """Small stable summary for one generation run."""

    template_name: str
    output_dir: Path
    generated_files: list[Path]
    review_result: ReviewResult


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
        required=True,
        help="Directory where the generated project should be written.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the v0.2 CLI flow and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        spec = parse_request(args.request)
        template_dir = get_template_dir()
        print_parsed_spec_summary(spec)

        generated_files = generate_project(spec, Path(args.output), template_dir)
        review_result = review_project(spec, Path(args.output))
        summary = GenerationSummary(
            template_name=template_dir.name,
            output_dir=Path(args.output),
            generated_files=generated_files,
            review_result=review_result,
        )
        print_generation_summary(summary)
        if not summary.review_result.success:
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


def get_template_dir() -> Path:
    """Return the single local template directory used by the current CLI."""
    return Path(__file__).resolve().parents[2] / "templates" / "flink_kafka_rule_job"


def generate_project(spec: FlinkJobSpec, output_dir: Path, template_dir: Path | None = None) -> list[Path]:
    """Generate the project from the single local v0.2 template directory."""
    resolved_template_dir = template_dir or get_template_dir()
    generator = ProjectGenerator(template_dir=resolved_template_dir)
    return generator.generate(spec=spec, output_dir=output_dir)


def review_project(spec: FlinkJobSpec, output_dir: Path) -> ReviewResult:
    """Run the deterministic structural review for the generated project."""
    reviewer = StructuralReviewer()
    return reviewer.review(output_dir=output_dir, spec=spec)


def print_parsed_spec_summary(spec: FlinkJobSpec) -> None:
    """Print the validated spec in a stable, readable form."""
    print("Parsed spec summary:")
    print(json.dumps(spec.model_dump(), indent=2))
    print()


def print_generation_summary(summary: GenerationSummary) -> None:
    """Print a concise stable summary for the generation run."""
    print(f"Chosen template: {summary.template_name}")
    print(f"Generation target: {summary.output_dir}")
    print(f"Generated files count: {len(summary.generated_files)}")
    print("Generated files:")
    for path in summary.generated_files:
        print(f"- {path}")
    print(
        "Structural review summary: "
        f"{len(summary.review_result.passed_checks)} passed, "
        f"{len(summary.review_result.failed_checks)} failed, "
        f"{len(summary.review_result.warnings)} warnings"
    )
    for item in summary.review_result.failed_checks:
        print(f"- FAIL: {item}")
    for item in summary.review_result.warnings:
        print(f"- WARN: {item}")


if __name__ == "__main__":
    raise SystemExit(main())
