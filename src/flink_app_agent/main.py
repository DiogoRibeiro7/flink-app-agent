"""Command-line entry point for the v0.1 flink-app-agent flow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .generator import ProjectGenerator
from .llm import SpecParsingError, build_default_spec_extractor
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
        required=True,
        help="Directory where the generated project should be written.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the v0.1 CLI flow and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        spec = parse_request(args.request)
        print_parsed_spec(spec)

        generated_files = generate_project(spec, Path(args.output))
        print_generated_files(generated_files)
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


def generate_project(spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
    """Generate the project from the single local v0.1 template directory."""
    template_dir = Path(__file__).resolve().parents[2] / "templates" / "flink_kafka_rule_job"
    generator = ProjectGenerator(template_dir=template_dir)
    return generator.generate(spec=spec, output_dir=output_dir)


def print_parsed_spec(spec: FlinkJobSpec) -> None:
    """Print the validated spec in a readable form."""
    print("Parsed spec:")
    print(json.dumps(spec.model_dump(), indent=2))
    print()


def print_generated_files(generated_files: list[Path]) -> None:
    """Print the generated file list after successful project creation."""
    print("Generated files:")
    for path in generated_files:
        print(f"- {path}")


if __name__ == "__main__":
    raise SystemExit(main())
