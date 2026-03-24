"""Extraction layer boundaries and deterministic implementations for v0.3."""

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
    """Raised when a request does not match the supported deterministic patterns."""


class PromptRepository(Protocol):
    """Interface for loading named prompt text used by extraction."""

    def load(self, prompt_name: str) -> str:
        """Return the prompt text for the given prompt name."""


class RequestPreprocessor(Protocol):
    """Interface for normalizing raw user requests before extraction."""

    def preprocess(self, request: str) -> str:
        """Return the normalized request text."""


class SpecPayloadExtractor(Protocol):
    """Interface for converting prompt-guided text into a raw spec payload."""

    def extract_payload(self, request: str, prompt: str) -> dict[str, Any]:
        """Return a raw payload that can be validated into ``FlinkJobSpec``."""


class SpecValidator(Protocol):
    """Interface for validating raw payloads into the internal spec model."""

    def validate(self, payload: dict[str, Any]) -> FlinkJobSpec:
        """Return a validated ``FlinkJobSpec``."""


class SpecExtractor(Protocol):
    """Top-level interface for converting a request into a validated spec."""

    def extract_spec(self, request: str) -> FlinkJobSpec:
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
class SimpleRequestPreprocessor:
    """Normalize requests without changing their semantic structure."""

    def preprocess(self, request: str) -> str:
        """Collapse repeated whitespace and trim the outer edges."""
        return re.sub(r"\s+", " ", request).strip()


@dataclass(frozen=True)
class PydanticSpecValidator:
    """Validate extracted payloads using the project Pydantic model."""

    def validate(self, payload: dict[str, Any]) -> FlinkJobSpec:
        """Convert a raw payload into a validated ``FlinkJobSpec``."""
        return FlinkJobSpec.from_llm_payload(payload)


@dataclass(frozen=True)
class SpecExtractionService:
    """Coordinate preprocessing, prompt loading, extraction, and validation."""

    preprocessor: RequestPreprocessor
    prompt_repository: PromptRepository
    payload_extractor: SpecPayloadExtractor
    validator: SpecValidator
    prompt_name: str = EXTRACT_SPEC_PROMPT

    def extract(self, request: str) -> FlinkJobSpec:
        """Run the full extraction flow and return a validated spec."""
        normalized_request = self.preprocessor.preprocess(request)
        prompt = self.prompt_repository.load(self.prompt_name)
        payload = self.payload_extractor.extract_payload(normalized_request, prompt)
        return self.validator.validate(payload)


@dataclass(frozen=True)
class DeterministicSpecPayloadExtractor:
    """Deterministic extractor for the current narrow request surface."""

    default_sink_topic: str = "inferred-events"
    default_event_time_field: str = "ts"
    default_input_event_name: str = "InputEvent"

    def extract_payload(self, request: str, prompt: str) -> dict[str, Any]:
        """Convert supported request variants into a raw structured payload."""
        del prompt

        # TODO: Replace this regex-based implementation with a real provider-backed call.
        # TODO: Pass the contents of extract_spec.md into the future provider request.
        source_topic = self._extract_source_topic(request)
        key_by = self._extract_key_by(request)
        output_event_raw = self._extract_output_event_name(request)
        time_window_raw = self._extract_time_window_minutes(request)

        output_event_name = to_pascal_case(output_event_raw)
        time_window_minutes = int(time_window_raw)

        return {
            "job_name": slugify(output_event_name) + "-job",
            "source_topic": source_topic,
            "sink_topic": self.default_sink_topic,
            "key_by": key_by,
            "event_time_field": self.default_event_time_field,
            "input_event_name": self.default_input_event_name,
            "output_event_name": output_event_name,
            "rule_type": ALLOWED_RULE_TYPE,
            "rule_condition": (
                f"emit {output_event_name} when two keyed events match within "
                f"{time_window_minutes} minutes"
            ),
            "time_window_minutes": time_window_minutes,
        }

    def _extract_source_topic(self, request: str) -> str:
        """Extract the Kafka source topic from supported wording variants."""
        return self._extract_required(
            request,
            patterns=[
                r"read from kafka ([A-Za-z0-9._-]+)",
                r"consume ([A-Za-z0-9._-]+) from kafka",
                r"build a flink job reading ([A-Za-z0-9._-]+)",
                r"read topic ([A-Za-z0-9._-]+)",
                r"reading ([A-Za-z0-9._-]+)",
            ],
            error_message=(
                "Unable to parse source_topic. Supported variants include "
                "'Read from Kafka <topic>', 'Consume <topic> from Kafka', or "
                "'Build a Flink job reading <topic>'."
            ),
        )

    def _extract_key_by(self, request: str) -> str:
        """Extract the key field from supported wording variants."""
        return self._extract_required(
            request,
            patterns=[
                r"key by ([A-Za-z0-9_.-]+)",
                r"keyed by ([A-Za-z0-9_.-]+)",
                r"keying by ([A-Za-z0-9_.-]+)",
                r"group by ([A-Za-z0-9_.-]+)",
            ],
            error_message=(
                "Unable to parse key_by. Supported variants include "
                "'key by <field>', 'keyed by <field>', 'keying by <field>', or "
                "'group by <field>'."
            ),
        )

    def _extract_output_event_name(self, request: str) -> str:
        """Extract the output event name from supported wording variants."""
        return self._extract_required(
            request,
            patterns=[
                r"emit ([A-Za-z0-9 _-]+?) events? within",
                r"emit ([A-Za-z0-9 _-]+?) within",
                r"emit ([A-Za-z0-9 _-]+?)(?: events?)?$",
                r"writing ([A-Za-z0-9 _-]+?)(?: within|$)",
                r"write ([A-Za-z0-9 _-]+?)(?: within|$)",
            ],
            error_message=(
                "Unable to parse output_event_name. Supported variants include "
                "'emit <EVENT> within <N> minutes' or 'writing <EVENT> within <N> minutes'."
            ),
        )

    def _extract_time_window_minutes(self, request: str) -> str:
        """Extract the time window from supported minute-based wording variants."""
        return self._extract_required(
            request,
            patterns=[
                r"within (\d+) minutes",
                r"within (\d+) minute",
            ],
            error_message=(
                "Unable to parse time_window_minutes. Supported variants include "
                "'within <N> minutes'."
            ),
        )

    def _extract_required(
        self,
        text: str,
        patterns: list[str],
        error_message: str,
    ) -> str:
        """Extract the first matching value from the supplied pattern list."""
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match is not None:
                return match.group(1).strip()
        raise SpecParsingError(error_message)


@dataclass(frozen=True)
class OpenAISpecPayloadExtractor:
    """Placeholder skeleton for a future model-backed payload extractor."""

    model_name: str = "gpt-placeholder"

    def extract_payload(self, request: str, prompt: str) -> dict[str, Any]:
        """Raise until a real provider-backed implementation is added."""
        del request
        del prompt
        raise NotImplementedError(
            "Real model-backed extraction is not implemented yet. Replace this skeleton with a provider call later."
        )


@dataclass(frozen=True)
class ServiceBackedSpecExtractor:
    """Expose the top-level extraction interface over the extraction service."""

    extraction_service: SpecExtractionService

    def extract_spec(self, request: str) -> FlinkJobSpec:
        """Parse a request through the configured extraction service."""
        return self.extraction_service.extract(request)


@dataclass(frozen=True)
class StubSpecExtractor:
    """Backward-compatible deterministic extractor used as the default implementation."""

    extraction_service: SpecExtractionService = field(init=False)

    def __init__(self) -> None:
        object.__setattr__(self, "extraction_service", build_default_extraction_service())

    def extract_spec(self, request: str, prompt: str | None = None) -> FlinkJobSpec:
        """Parse a request using the default deterministic extraction pipeline."""
        del prompt
        return self.extraction_service.extract(request)


def build_default_extraction_service() -> SpecExtractionService:
    """Build the default deterministic extraction service pipeline."""
    return SpecExtractionService(
        preprocessor=SimpleRequestPreprocessor(),
        prompt_repository=FilePromptRepository(),
        payload_extractor=DeterministicSpecPayloadExtractor(),
        validator=PydanticSpecValidator(),
    )


def build_default_spec_extractor() -> SpecExtractor:
    """Return the default deterministic extractor used by the current CLI."""
    return StubSpecExtractor()
