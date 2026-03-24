"""Small typed context object for one local generation run."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .review import ReviewResult
from .spec import FlinkJobSpec
from .template_registry import TemplateDefinition


@dataclass
class GenerationContext:
    """Carry the small set of state shared across the generation pipeline."""

    request_text: str
    output_dir: Path
    spec: FlinkJobSpec
    template: TemplateDefinition
    generated_files: list[Path] = field(default_factory=list)
    review_result: ReviewResult | None = None
    report_path: Path | None = None
