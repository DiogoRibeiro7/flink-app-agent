"""Command-line entry point for the first version of flink-app-agent."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .generator import ProjectGenerator
from .llm import SpecParsingError, StubSpecExtractor, load_prompt
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
        print_success_summary(spec, Path(args.output), generated_files)
    except (FileNotFoundError, NotADirectoryError, FileExistsError, SpecParsingError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1

    return 0


def parse_request(request: str) -> FlinkJobSpec:
    """Parse a natural-language request into a validated ``FlinkJobSpec``."""
    extract_prompt = load_prompt("extract_spec.md")
    extractor = StubSpecExtractor()
    return extractor.extract_spec(request=request, prompt=extract_prompt)


def generate_project(spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
    """Generate the Flink project from the local template directory."""
    template_dir = Path(__file__).resolve().parents[2] / "templates" / "flink_kafka_rule_job"
    generator = ProjectGenerator(template_dir=template_dir)
    return generator.generate(spec=spec, output_dir=output_dir)


def print_parsed_spec(spec: FlinkJobSpec) -> None:
    """Print the parsed spec before generation starts."""
    print("Parsed spec:")
    print(json.dumps(spec.model_dump(), indent=2))
    print()


def print_success_summary(spec: FlinkJobSpec, output_dir: Path, generated_files: list[Path]) -> None:
    """Print a small success summary including generated files."""
    del spec
    print(f"Generated project in: {output_dir}")
    print("Generated files:")
    for path in generated_files:
        print(f"- {path}")


if __name__ == "__main__":
    raise SystemExit(main())
