"""Utility helpers shared across the project."""

from __future__ import annotations

import re


def slugify(value: str) -> str:
    """Convert free text into a lowercase hyphenated identifier."""
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return normalized.strip("-") or "flink-job"


def to_pascal_case(value: str) -> str:
    """Convert free text into a Java-friendly PascalCase class name."""
    parts = re.findall(r"[A-Za-z0-9]+", value)
    return "".join(part.capitalize() for part in parts) or "GeneratedFlinkJob"
