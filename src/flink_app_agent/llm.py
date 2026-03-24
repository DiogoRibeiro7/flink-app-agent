"""Deterministic parsing support for future LLM-backed spec extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .spec import ALLOWED_RULE_TYPE, FlinkJobSpec
from .utils import slugify, to_pascal_case


PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


class SpecParsingError(ValueError):
    """Raised when a natural-language request cannot be parsed into a valid spec."""


class SpecExtractor(Protocol):
    """Interface for turning a natural-language request into a job spec."""

    def extract_spec(self, request: str, prompt: str | None = None) -> FlinkJobSpec:
        """Parse a user request into a validated ``FlinkJobSpec``."""


def load_prompt(prompt_name: str) -> str:
    """Load a prompt file from the package prompt directory."""
    prompt_path = PROMPTS_DIR / prompt_name
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    return prompt_path.read_text(encoding="utf-8")


@dataclass(frozen=True)
class StubSpecExtractor:
    """Deterministic extractor used until a real LLM integration is added."""

    default_source_topic: str = "input-topic"
    default_sink_topic: str = "output-topic"
    default_event_time_field: str = "event_time"
    default_input_event_name: str = "InputEvent"

    def extract_spec(self, request: str, prompt: str | None = None) -> FlinkJobSpec:
        """Parse a narrow request pattern into a validated spec."""
        del prompt
        self._validate_required_clues(request)

        # TODO: Replace this deterministic parser with a real prompt-driven LLM call.
        # TODO: Pass the contents of extract_spec.md into the future model request.
        key_by = self._extract_match(
            request,
            r"key by ([A-Za-z0-9_.-]+)",
            "Request must include 'key by <field>'.",
        )
        output_event_name = self._normalize_event_name(
            self._extract_match(
                request,
                r"emit ([A-Za-z0-9 _-]+?)(?: within| from| to| on| when| if|\.|,|$)",
                "Request must include 'emit <EVENT>'.",
            )
        )
        window_minutes = int(
            self._extract_match(
                request,
                r"within (\d+) minutes",
                "Request must include 'within <N> minutes'.",
            )
        )

        source_topic = self._extract_optional_match(
            request,
            r"(?:source topic|from topic) ([A-Za-z0-9._-]+)",
            self.default_source_topic,
        )
        sink_topic = self._extract_optional_match(
            request,
            r"(?:sink topic|to topic) ([A-Za-z0-9._-]+)",
            self.default_sink_topic,
        )
        event_time_field = self._extract_optional_match(
            request,
            r"event time field ([A-Za-z0-9_.-]+)",
            self.default_event_time_field,
        )
        raw_job_name = self._extract_optional_match(
            request,
            r"(?:job name|job called|job named) ([A-Za-z0-9 _.-]+?)(?: with| using| that|,|\.|$)",
            f"{output_event_name} job",
        )
        job_name = slugify(raw_job_name)

        rule_condition = (
            f"emit {output_event_name} when two keyed events match within {window_minutes} minutes"
        )

        return FlinkJobSpec(
            job_name=job_name,
            source_topic=source_topic,
            sink_topic=sink_topic,
            key_by=key_by,
            event_time_field=event_time_field,
            input_event_name=self.default_input_event_name,
            output_event_name=output_event_name,
            rule_type=ALLOWED_RULE_TYPE,
            rule_condition=rule_condition,
            time_window_minutes=window_minutes,
        )

    def _validate_required_clues(self, request: str) -> None:
        """Ensure the request contains the minimum supported pattern."""
        missing_parts: list[str] = []
        if "kafka" not in request.lower():
            missing_parts.append("Kafka")
        if re.search(r"key by [A-Za-z0-9_.-]+", request, flags=re.IGNORECASE) is None:
            missing_parts.append("key by <field>")
        if re.search(
            r"emit [A-Za-z0-9 _-]+?(?: within| from| to| on| when| if|\.|,|$)",
            request,
            flags=re.IGNORECASE,
        ) is None:
            missing_parts.append("emit <EVENT>")
        if re.search(r"within \d+ minutes", request, flags=re.IGNORECASE) is None:
            missing_parts.append("within <N> minutes")

        if missing_parts:
            required = ", ".join(missing_parts)
            raise SpecParsingError(
                f"Unable to parse request. Supported requests must mention: {required}."
            )

    def _extract_match(self, text: str, pattern: str, error_message: str) -> str:
        """Extract a required regex group from text."""
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match is None:
            raise SpecParsingError(error_message)
        return match.group(1).strip()

    def _extract_optional_match(self, text: str, pattern: str, default: str) -> str:
        """Extract an optional regex group or return the provided default."""
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match is None:
            return default
        return match.group(1).strip()

    def _normalize_event_name(self, value: str) -> str:
        """Return a stable event class name from free text."""
        if re.fullmatch(r"[A-Z][A-Za-z0-9]*", value):
            return value
        return to_pascal_case(value)
