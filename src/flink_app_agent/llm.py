"""Extraction layer abstractions and deterministic implementations."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .spec import ALLOWED_RULE_TYPE, FlinkJobSpec
from .utils import slugify, to_pascal_case


PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
EXTRACT_SPEC_PROMPT = "extract_spec.md"


class SpecParsingError(ValueError):
    """Raised when a natural-language request cannot be parsed into a valid spec."""


class PromptRepository(Protocol):
    """Interface for loading named prompt files."""

    def load(self, prompt_name: str) -> str:
        """Return the text content for a named prompt."""


class RequestPreprocessor(Protocol):
    """Interface for normalizing user requests before extraction."""

    def preprocess(self, request: str) -> str:
        """Return the normalized request text used for extraction."""


class SpecPayloadExtractor(Protocol):
    """Interface for turning a request and prompt into a raw spec payload."""

    def extract_payload(self, request: str, prompt: str) -> dict[str, Any]:
        """Return a raw structured payload extracted from the request."""


class SpecValidator(Protocol):
    """Interface for validating and normalizing extracted payloads."""

    def validate(self, payload: dict[str, Any]) -> FlinkJobSpec:
        """Convert a raw payload into a validated ``FlinkJobSpec``."""


class SpecExtractor(Protocol):
    """High-level interface for turning a natural-language request into a job spec."""

    def extract_spec(self, request: str) -> FlinkJobSpec:
        """Parse a user request into a validated ``FlinkJobSpec``."""


@dataclass(frozen=True)
class FilePromptRepository:
    """Load prompt files from a local prompt directory."""

    prompts_dir: Path = PROMPTS_DIR

    def load(self, prompt_name: str) -> str:
        """Load a prompt file from disk."""
        prompt_path = self.prompts_dir / prompt_name
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt file not found in prompts directory: {prompt_name}")
        return prompt_path.read_text(encoding="utf-8")


def load_prompt(prompt_name: str) -> str:
    """Load a prompt file using the default file-backed repository."""
    return FilePromptRepository().load(prompt_name)


@dataclass(frozen=True)
class SimpleRequestPreprocessor:
    """Apply minimal normalization to user input before extraction."""

    def preprocess(self, request: str) -> str:
        """Normalize whitespace while preserving the current parsing behavior."""
        return re.sub(r"\s+", " ", request).strip()


@dataclass(frozen=True)
class PydanticSpecValidator:
    """Validate extracted payloads with the project Pydantic model."""

    def validate(self, payload: dict[str, Any]) -> FlinkJobSpec:
        """Convert a raw payload into a validated ``FlinkJobSpec``."""
        return FlinkJobSpec.from_llm_payload(payload)


@dataclass(frozen=True)
class SpecExtractionService:
    """Coordinate preprocessing, prompt loading, extraction, and validation."""

    preprocessor: RequestPreprocessor
    payload_extractor: SpecPayloadExtractor
    validator: SpecValidator
    prompt_repository: PromptRepository
    prompt_name: str = EXTRACT_SPEC_PROMPT

    def extract(self, request: str) -> FlinkJobSpec:
        """Load the extraction prompt and parse the request into a spec."""
        normalized_request = self.preprocessor.preprocess(request)
        prompt = self.prompt_repository.load(self.prompt_name)
        payload = self.payload_extractor.extract_payload(
            request=normalized_request,
            prompt=prompt,
        )
        return self.validator.validate(payload)


@dataclass(frozen=True)
class StubSpecPayloadExtractor:
    """Deterministic payload extractor used until a real LLM integration is added."""

    default_sink_topic: str = "output-topic"
    default_event_time_field: str = "event_time"
    default_input_event_name: str = "InputEvent"

    def extract_payload(self, request: str, prompt: str) -> dict[str, Any]:
        """Parse a narrow request pattern into a raw structured payload."""
        del prompt

        # TODO: Replace this deterministic parser with a real prompt-driven LLM call.
        # TODO: Pass the contents of extract_spec.md into the future model request.
        source_topic = self._extract_source_topic(request)
        key_by = self._extract_key_by(request)
        output_event_name, inferred_sink_topic = self._extract_output_and_sink(request)
        window_minutes = self._extract_window_minutes(request)
        sink_topic = self._extract_optional_match(
            request,
            self._sink_topic_patterns(),
            default=inferred_sink_topic or self.default_sink_topic,
        )
        event_time_field = self._extract_optional_match(
            request,
            self._event_time_patterns(),
            default=self.default_event_time_field,
        )
        raw_job_name = self._extract_optional_match(
            request,
            [
                r"(?:job name|job called|job named) ([A-Za-z0-9 _.-]+?)(?: with| using| that|,|\.|$)",
                r"build a flink job called ([A-Za-z0-9 _.-]+?)(?: with| using| that|,|\.|$)",
            ],
            f"{output_event_name} job",
        )
        job_name = slugify(raw_job_name)

        rule_condition = (
            f"emit {output_event_name} when two keyed events match within {window_minutes} minutes"
        )

        return {
            "job_name": job_name,
            "source_topic": source_topic,
            "sink_topic": sink_topic,
            "key_by": key_by,
            "event_time_field": event_time_field,
            "input_event_name": self.default_input_event_name,
            "output_event_name": output_event_name,
            "rule_type": ALLOWED_RULE_TYPE,
            "rule_condition": rule_condition,
            "time_window_minutes": window_minutes,
        }

    def _extract_source_topic(self, request: str) -> str:
        """Extract the Kafka source topic from a supported wording variant."""
        return self._extract_required_match(
            request,
            self._source_topic_patterns(),
            "Unable to parse source_topic. Supported variants include 'source topic <topic>', 'read(s) <topic>', or 'consume <topic>'.",
        )

    def _extract_key_by(self, request: str) -> str:
        """Extract the key field from a supported wording variant."""
        return self._extract_required_match(
            request,
            [
                r"key by ([A-Za-z0-9_.-]+)",
                r"keyed by ([A-Za-z0-9_.-]+)",
                r"group by ([A-Za-z0-9_.-]+)",
                r"groups by ([A-Za-z0-9_.-]+)",
            ],
            "Unable to parse key_by. Supported variants include 'key by <field>' or 'group(s) by <field>'.",
        )

    def _extract_output_and_sink(self, request: str) -> tuple[str, str | None]:
        """Extract the output event name and optionally infer the sink topic."""
        for pattern in [
            r"publish ([A-Za-z0-9 _-]+?) events? to ([A-Za-z0-9._-]+)",
            r"writes? ([A-Za-z0-9 _-]+?) to ([A-Za-z0-9._-]+)",
            r"emit ([A-Za-z0-9 _-]+?) events? to ([A-Za-z0-9._-]+)",
            r"emit ([A-Za-z0-9 _-]+?) to ([A-Za-z0-9._-]+)",
        ]:
            match = re.search(pattern, request, flags=re.IGNORECASE)
            if match is not None:
                return self._normalize_event_name(match.group(1).strip()), match.group(2).strip()

        output_event_name = self._normalize_event_name(
            self._extract_required_match(
                request,
                [r"emit ([A-Za-z0-9 _-]+?)(?: within| from| to| on| when| if|\.|,|$)"],
                "Unable to parse output_event_name. Supported variants include 'emit <EVENT>' or 'publish <EVENT> events to <topic>'.",
            )
        )
        return output_event_name, None

    def _extract_window_minutes(self, request: str) -> int:
        """Extract the rule window in minutes from a supported wording variant."""
        value = self._extract_required_match(
            request,
            [
                r"within (\d+) minutes",
                r"within a (\d+) minute window",
                r"over a (\d+) minute window",
            ],
            "Unable to parse time_window_minutes. Supported variants include 'within <N> minutes'.",
        )
        return int(value)

    def _extract_required_match(
        self,
        text: str,
        patterns: list[str],
        error_message: str,
    ) -> str:
        """Extract the first matching required regex group from text."""
        value = self._extract_optional_match(text, patterns, default=None)
        if value is None:
            raise SpecParsingError(error_message)
        return value

    def _extract_optional_match(
        self,
        text: str,
        patterns: list[str],
        default: str | None,
    ) -> str | None:
        """Extract the first matching regex group or return the provided default."""
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match is not None:
                return match.group(1).strip()
        return default

    def _source_topic_patterns(self) -> list[str]:
        """Return the supported source-topic extraction patterns."""
        return [
            r"source topic ([A-Za-z0-9._-]+)",
            r"from kafka topic ([A-Za-z0-9._-]+)",
            r"from topic ([A-Za-z0-9._-]+)",
            r"consume ([A-Za-z0-9._-]+)",
            r"reads? from ([A-Za-z0-9._-]+)",
            r"reads? ([A-Za-z0-9._-]+)",
        ]

    def _sink_topic_patterns(self) -> list[str]:
        """Return the supported sink-topic extraction patterns."""
        return [
            r"sink topic ([A-Za-z0-9._-]+)",
            r"to inferred topic ([A-Za-z0-9._-]+)",
            r"to ([A-Za-z0-9._-]+)",
        ]

    def _event_time_patterns(self) -> list[str]:
        """Return the supported event-time field extraction patterns."""
        return [
            r"event time field ([A-Za-z0-9_.-]+)",
            r"event-time field ([A-Za-z0-9_.-]+)",
            r"use ([A-Za-z0-9_.-]+) as event time",
        ]

    def _normalize_event_name(self, value: str) -> str:
        """Return a stable event class name from free text."""
        if re.fullmatch(r"[A-Z][A-Za-z0-9]*", value):
            return value
        return to_pascal_case(value)


@dataclass(frozen=True)
class OpenAISpecPayloadExtractor:
    """Placeholder adapter for a future OpenAI-style extraction implementation."""

    model_name: str = "gpt-placeholder"

    def extract_payload(self, request: str, prompt: str) -> dict[str, Any]:
        """Raise until a real provider integration is implemented."""
        del request
        del prompt
        raise NotImplementedError(
            "OpenAI-style extraction is not implemented yet. Replace this adapter with a real provider call."
        )


@dataclass(frozen=True)
class ServiceBackedSpecExtractor:
    """Adapter that exposes the high-level extractor interface over the service."""

    extraction_service: SpecExtractionService

    def extract_spec(self, request: str) -> FlinkJobSpec:
        """Extract a validated spec using the configured service pipeline."""
        return self.extraction_service.extract(request)


@dataclass(frozen=True)
class StubSpecExtractor:
    """Backward-compatible high-level extractor that preserves current stub behavior."""

    extraction_service: SpecExtractionService = field(init=False)

    def __init__(self) -> None:
        object.__setattr__(self, "extraction_service", build_default_extraction_service())

    def extract_spec(self, request: str, prompt: str | None = None) -> FlinkJobSpec:
        """Extract a validated spec using the default deterministic stub pipeline."""
        del prompt
        return self.extraction_service.extract(request)


def build_default_spec_extractor() -> SpecExtractor:
    """Build the default extractor used by the current application."""
    return StubSpecExtractor()


def build_default_extraction_service() -> SpecExtractionService:
    """Build the default extraction service pipeline."""
    return SpecExtractionService(
        preprocessor=SimpleRequestPreprocessor(),
        payload_extractor=StubSpecPayloadExtractor(),
        validator=PydanticSpecValidator(),
        prompt_repository=FilePromptRepository(),
    )
