"""Utility helpers shared across the project."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def slugify(value: str) -> str:
    """Convert free text into a lowercase hyphenated identifier."""
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return normalized.strip("-") or "flink-job"


def to_pascal_case(value: str) -> str:
    """Convert free text into a Java-friendly PascalCase class name."""
    parts = re.findall(r"[A-Za-z0-9]+", value)
    return "".join(part.capitalize() for part in parts) or "GeneratedFlinkJob"


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON content with stable formatting."""
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_prompt(path: Path) -> str:
    """Load a markdown prompt file from disk."""
    return path.read_text(encoding="utf-8")
