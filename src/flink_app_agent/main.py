"""Command-line entry point for the first version of flink-app-agent."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .generator import ProjectGenerator, select_template_for_spec
from .llm import FilePromptRepository, SpecExtractionService, SpecParsingError, StubSpecExtractor
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
        print_success_summary(Path(args.output), generated_files)
    except (FileNotFoundError, NotADirectoryError, FileExistsError, SpecParsingError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1

    return 0


def parse_request(request: str) -> FlinkJobSpec:
    """Parse a natural-language request into a validated ``FlinkJobSpec``."""
    extraction_service = SpecExtractionService(
        extractor=StubSpecExtractor(),
        prompt_repository=FilePromptRepository(),
    )
    return extraction_service.extract(request)


def generate_project(spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
    """Generate the Flink project from the local template directory."""
    templates_root = Path(__file__).resolve().parents[2] / "templates"
    template = select_template_for_spec(spec, templates_root)
    generator = ProjectGenerator(template_dir=template.template_path)
    return generator.generate(spec=spec, output_dir=output_dir)


def print_parsed_spec(spec: FlinkJobSpec) -> None:
    """Print the parsed spec before generation starts."""
    print("Parsed spec:")
    print(json.dumps(spec.model_dump(), indent=2))
    print()


def print_success_summary(output_dir: Path, generated_files: list[Path]) -> None:
    """Print a small success summary including generated files."""
    print(f"Generated project in: {output_dir}")
    print("Generated files:")
    for path in generated_files:
        print(f"- {path}")


if __name__ == "__main__":
    raise SystemExit(main())
