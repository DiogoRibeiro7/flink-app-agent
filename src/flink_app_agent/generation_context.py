"""Small typed context object for one local generation run."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .config import ExtractionOutcome
from .repair import RepairResult
from .review import ReviewResult
from .spec import FlinkJobSpec
from .template_registry import TemplateDefinition
from .verification import VerificationResult


@dataclass
class GenerationContext:
    """Carry the small set of state shared across the generation pipeline."""

    request_text: str
    output_dir: Path
    spec: FlinkJobSpec
    template: TemplateDefinition
    extraction_outcome: ExtractionOutcome = field(
        default_factory=lambda: ExtractionOutcome(
            requested_mode="deterministic",
            fallback_policy="fail",
            extractor_used="deterministic",
            actual_path=("deterministic",),
        ),
    )
    generated_files: list[Path] = field(default_factory=list)
    repair_result: RepairResult | None = None
    review_result: ReviewResult | None = None
    verification_result: VerificationResult | None = None
    report_path: Path | None = None
