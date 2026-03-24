"""Deterministic v0.1 extraction stub for Flink job specifications."""

from __future__ import annotations

import re
from dataclasses import dataclass
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
    """Deterministic extractor for the narrow v0.2 request surface."""

    default_sink_topic: str = "inferred-events"
    default_event_time_field: str = "ts"
    default_input_event_name: str = "InputEvent"

    def extract_spec(self, request: str, prompt: str | None = None) -> FlinkJobSpec:
        """Convert a supported request variant into a validated ``FlinkJobSpec``."""
        del prompt

        normalized_request = self._normalize_request(request)

        # TODO: Replace this regex-based parser with a real model-backed extractor.
        # TODO: Pass the contents of extract_spec.md to the future provider call.
        source_topic = self._extract_source_topic(normalized_request)
        key_by = self._extract_key_by(normalized_request)
        output_event_raw = self._extract_output_event_name(normalized_request)
        time_window_raw = self._extract_time_window_minutes(normalized_request)

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
