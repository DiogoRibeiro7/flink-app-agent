"""CLI entry point for flink-app-agent."""

from __future__ import annotations

import argparse
from pathlib import Path

from .generator import ProjectGenerator
from .llm import StubLLMClient
from .spec import FlinkJobSpec
from .utils import load_prompt


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Generate a small Flink project from a natural-language request."
    )
    parser.add_argument(
        "--request",
        required=True,
        help="Plain-English description of the Flink job to generate.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where the generated project should be created.",
    )
    return parser


def main() -> None:
    """Run the CLI flow."""
    parser = build_parser()
    args = parser.parse_args()

    package_root = Path(__file__).resolve().parents[2]
    prompts_dir = package_root / "src" / "flink_app_agent" / "prompts"
    templates_dir = package_root / "templates"

    extract_prompt = load_prompt(prompts_dir / "extract_spec.md")
    llm_client = StubLLMClient()
    raw_payload = llm_client.extract_spec(prompt=extract_prompt, request=args.request)
    spec = FlinkJobSpec.from_llm_payload(raw_payload)

    generator = ProjectGenerator(template_root=templates_dir)
    project_dir = generator.generate(spec=spec, output_dir=Path(args.output_dir))

    print(f"Generated project at: {project_dir}")


if __name__ == "__main__":
    main()
