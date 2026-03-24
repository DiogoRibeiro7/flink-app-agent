"""Command-line entry point for the first version of flink-app-agent."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .generator import ProjectGenerator, select_template_for_spec
from .llm import SpecExtractor, SpecParsingError, build_default_spec_extractor
from .review import PostGenerationReviewer
from .spec import FlinkJobSpec


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
        "--output-dir",
        dest="output",
        required=True,
        help="Directory where the generated project should be written.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        spec = parse_request(args.request)
        print_parsed_spec(spec)

        generated_files = generate_project(spec, Path(args.output))
        review_result = review_project(spec, Path(args.output))
        print_success_summary(Path(args.output), generated_files, review_result)
        if not review_result.success:
            print("Error: post-generation review failed.", file=sys.stderr)
            return 1
    except (FileNotFoundError, NotADirectoryError, FileExistsError, SpecParsingError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1

    return 0


def parse_request(request: str) -> FlinkJobSpec:
    """Parse a natural-language request into a validated ``FlinkJobSpec``."""
    extractor: SpecExtractor = build_default_spec_extractor()
    return extractor.extract_spec(request)


def generate_project(spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
    """Generate the Flink project from the local template directory."""
    templates_root = Path(__file__).resolve().parents[2] / "templates"
    template = select_template_for_spec(spec, templates_root)
    generator = ProjectGenerator(template_dir=template.template_path)
    return generator.generate(spec=spec, output_dir=output_dir)


def review_project(spec: FlinkJobSpec, output_dir: Path):
    """Review the generated project for obvious structural issues."""
    reviewer = PostGenerationReviewer()
    return reviewer.review(output_dir=output_dir, spec=spec, repair=True)


def print_parsed_spec(spec: FlinkJobSpec) -> None:
    """Print the parsed spec before generation starts."""
    print("Parsed spec:")
    print(json.dumps(spec.model_dump(), indent=2))
    print()


def print_success_summary(output_dir: Path, generated_files: list[Path], review_result) -> None:
    """Print a small success summary including generated files."""
    print(f"Generated project in: {output_dir}")
    print("Generated files:")
    for path in generated_files:
        print(f"- {path}")
    print("Review summary:")
    for item in review_result.passed_checks:
        print(f"- PASS: {item}")
    for item in review_result.warnings:
        print(f"- WARN: {item}")
    for item in review_result.repairs:
        print(f"- REPAIR: {item}")
    for item in review_result.failed_checks:
        print(f"- FAIL: {item}")


if __name__ == "__main__":
    raise SystemExit(main())
