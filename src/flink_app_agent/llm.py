"""Deterministic v0.1 extraction stub for Flink job specifications."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from .spec import ALLOWED_RULE_TYPE, FlinkJobSpec
from .utils import slugify, to_pascal_case


PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
EXTRACT_SPEC_PROMPT = "extract_spec.md"


class SpecParsingError(ValueError):
    """Raised when a request does not match the narrow supported v0.1 pattern."""


class SpecExtractor(Protocol):
    """Interface for converting a plain-English request into a validated spec."""

    def extract_spec(self, request: str) -> FlinkJobSpec:
        """Parse a request into a validated ``FlinkJobSpec``."""


class ExtractionService(Protocol):
    """Interface for the small extraction service wrapper used by the CLI and tests."""

    def extract(self, request: str) -> FlinkJobSpec:
        """Parse a request into a validated ``FlinkJobSpec``."""


@dataclass(frozen=True)
class FilePromptRepository:
    """Load local prompt files from the package prompt directory."""

    prompts_dir: Path = PROMPTS_DIR

    def load(self, prompt_name: str) -> str:
        """Return the contents of a prompt file."""
        prompt_path = self.prompts_dir / prompt_name
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt file not found: {prompt_name}")
        return prompt_path.read_text(encoding="utf-8")


def load_prompt(prompt_name: str) -> str:
    """Load a prompt file using the default local repository."""
    return FilePromptRepository().load(prompt_name)


@dataclass(frozen=True)
class StubSpecExtractor:
    """Deterministic v0.1 extractor for a single narrow Flink request pattern."""

    default_sink_topic: str = "inferred-events"
    default_event_time_field: str = "ts"
    default_input_event_name: str = "InputEvent"

    def extract_spec(self, request: str, prompt: str | None = None) -> FlinkJobSpec:
        """Convert a supported v0.1 request into a validated ``FlinkJobSpec``."""
        del prompt

        normalized_request = self._normalize_request(request)

        # TODO: Replace this regex-based parser with a real model-backed extractor.
        # TODO: Pass the contents of extract_spec.md to the future provider call.
        source_topic = self._extract_required(
            normalized_request,
            r"read from kafka ([A-Za-z0-9._-]+)",
            "Unable to parse source_topic. Supported v0.1 wording starts with 'Read from Kafka <topic>'.",
        )
        key_by = self._extract_required(
            normalized_request,
            r"key by ([A-Za-z0-9_.-]+)",
            "Unable to parse key_by. Supported v0.1 wording includes 'key by <field>'.",
        )
        output_event_raw = self._extract_required(
            normalized_request,
            r"emit ([A-Za-z0-9 _-]+?) within",
            "Unable to parse output_event_name. Supported v0.1 wording includes 'emit <EVENT> within <N> minutes'.",
        )
        time_window_raw = self._extract_required(
            normalized_request,
            r"within (\d+) minutes",
            "Unable to parse time_window_minutes. Supported v0.1 wording includes 'within <N> minutes'.",
        )

        output_event_name = to_pascal_case(output_event_raw)
        job_name = slugify(output_event_name) + "-job"
        time_window_minutes = int(time_window_raw)
        rule_condition = (
            f"emit {output_event_name} when two keyed events match within {time_window_minutes} minutes"
        )

        return FlinkJobSpec(
            job_name=job_name,
            source_topic=source_topic,
            sink_topic=self.default_sink_topic,
            key_by=key_by,
            event_time_field=self.default_event_time_field,
            input_event_name=self.default_input_event_name,
            output_event_name=output_event_name,
            rule_type=ALLOWED_RULE_TYPE,
            rule_condition=rule_condition,
            time_window_minutes=time_window_minutes,
        )

    def _normalize_request(self, request: str) -> str:
        """Normalize whitespace without changing the supported wording structure."""
        return re.sub(r"\s+", " ", request).strip()

    def _extract_required(self, text: str, pattern: str, error_message: str) -> str:
        """Extract one required value or raise a clear parsing error."""
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match is None:
            raise SpecParsingError(error_message)
        return match.group(1).strip()


@dataclass(frozen=True)
class DefaultExtractionService:
    """Small service wrapper that keeps extraction wiring explicit."""

    extractor: SpecExtractor

    def extract(self, request: str) -> FlinkJobSpec:
        """Parse a request through the configured extractor."""
        return self.extractor.extract_spec(request)


@dataclass(frozen=True)
class OpenAISpecExtractor:
    """Placeholder adapter for a future provider-backed extractor."""

    model_name: str = "gpt-placeholder"

    def extract_spec(self, request: str) -> FlinkJobSpec:
        """Raise until a real external provider integration is added."""
        del request
        raise NotImplementedError(
            "Real LLM extraction is not implemented yet. Replace this stub with a provider-backed extractor later."
        )


def build_default_spec_extractor() -> SpecExtractor:
    """Return the default deterministic v0.1 extractor."""
    return StubSpecExtractor()


def build_default_extraction_service() -> ExtractionService:
    """Return the default extraction service used by the current application."""
    return DefaultExtractionService(extractor=build_default_spec_extractor())
