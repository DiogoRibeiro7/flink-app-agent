"""Deterministic repair loop for generated Flink projects.

The repair loop applies a small set of safe, auditable fixes to generated
output. Each repair pass is deterministic and idempotent. The loop runs
at most ``max_passes`` times and stops early when no new repairs are found.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from .generator import PLACEHOLDER_PATTERN, SAFE_TEXT_EXTENSIONS


@dataclass
class RepairResult:
    """Structured result for one complete repair loop."""

    repairs: list[str] = field(default_factory=list)
    passes_run: int = 0

    @property
    def any_repairs(self) -> bool:
        """Return whether any repairs were applied."""
        return len(self.repairs) > 0


@dataclass(frozen=True)
class DeterministicRepairer:
    """Apply a small set of safe deterministic repairs to generated output.

    Repairs are intentionally narrow. Each repair must be:
    - deterministic (same input → same output)
    - safe (only fixes obviously broken output)
    - auditable (every repair is recorded)
    """

    text_extensions: frozenset[str] = SAFE_TEXT_EXTENSIONS
    max_passes: int = 3

    def repair(self, output_dir: Path) -> RepairResult:
        """Run the repair loop up to ``max_passes`` times."""
        result = RepairResult()
        for _ in range(self.max_passes):
            result.passes_run += 1
            pass_repairs = self._run_one_pass(output_dir)
            result.repairs.extend(pass_repairs)
            if not pass_repairs:
                break
        return result

    def _run_one_pass(self, output_dir: Path) -> list[str]:
        """Run all repair strategies once and return applied repairs."""
        repairs: list[str] = []
        for path in self._iter_text_files(output_dir):
            repairs.extend(self._repair_trailing_placeholder_lines(path))
            repairs.extend(self._repair_excess_trailing_blank_lines(path))
            repairs.extend(self._repair_missing_final_newline(path))
            repairs.extend(self._repair_trailing_whitespace(path))
        return repairs

    def _repair_trailing_placeholder_lines(self, path: Path) -> list[str]:
        """Remove trailing lines that consist only of an unresolved placeholder."""
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        trimmed = list(lines)
        while trimmed and PLACEHOLDER_PATTERN.fullmatch(trimmed[-1].strip()):
            trimmed.pop()
        if len(trimmed) == len(lines):
            return []
        repaired = "\n".join(trimmed)
        if text.endswith("\n") and repaired:
            repaired += "\n"
        path.write_text(repaired, encoding="utf-8")
        return [f"Removed trailing placeholder-only lines from {path}"]

    def _repair_excess_trailing_blank_lines(self, path: Path) -> list[str]:
        """Collapse multiple trailing blank lines to a single final newline."""
        text = path.read_text(encoding="utf-8")
        if not text:
            return []
        normalized = text.rstrip("\n")
        if normalized == text:
            return []
        repaired = normalized + "\n"
        if repaired == text:
            return []
        path.write_text(repaired, encoding="utf-8")
        return [f"Removed excess trailing blank lines from {path}"]

    def _repair_missing_final_newline(self, path: Path) -> list[str]:
        """Add a final newline to text files that are missing one."""
        text = path.read_text(encoding="utf-8")
        if text and not text.endswith("\n"):
            path.write_text(text + "\n", encoding="utf-8")
            return [f"Added missing final newline to {path}"]
        return []

    def _repair_trailing_whitespace(self, path: Path) -> list[str]:
        """Remove trailing whitespace from lines in text files."""
        text = path.read_text(encoding="utf-8")
        cleaned = re.sub(r"[ \t]+\n", "\n", text)
        if cleaned != text:
            path.write_text(cleaned, encoding="utf-8")
            return [f"Removed trailing whitespace from {path}"]
        return []

    def _iter_text_files(self, output_dir: Path) -> list[Path]:
        """Return files that are safe to repair."""
        return sorted(
            path
            for path in output_dir.rglob("*")
            if path.is_file() and path.suffix in self.text_extensions
        )
