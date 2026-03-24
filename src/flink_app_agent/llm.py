"""Stub LLM interface used by the first project version."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol

from .utils import slugify, to_pascal_case


class LLMClient(Protocol):
    """Minimal interface for extracting a structured payload from text."""

    def extract_spec(self, prompt: str, request: str) -> dict[str, Any]:
        """Return a structured payload derived from the natural-language request."""


@dataclass
class StubLLMClient:
    """Deterministic placeholder implementation for local development."""

    default_bootstrap_servers: str = "localhost:9092"

    def extract_spec(self, prompt: str, request: str) -> dict[str, Any]:
        """Extract a simple spec using lightweight heuristics instead of an LLM."""
        del prompt
        job_name = self._match(
            request,
            [
                r"job named ([a-zA-Z0-9 _-]+?)(?: that|,|\.|$)",
                r"called ([a-zA-Z0-9 _-]+?)(?: that|,|\.|$)",
            ],
            default="Generated Flink Job",
        )
        input_topic = self._match(
            request,
            [
                r"reads from topic ([a-zA-Z0-9._-]+)",
                r"input topic ([a-zA-Z0-9._-]+)",
                r"consume[s]? from ([a-zA-Z0-9._-]+)",
            ],
            default="input-topic",
        )
        output_topic = self._match(
            request,
            [
                r"writes to topic ([a-zA-Z0-9._-]+)",
                r"output topic ([a-zA-Z0-9._-]+)",
                r"publish(?:es)? to ([a-zA-Z0-9._-]+)",
            ],
            default="output-topic",
        )
        consumer_group = self._match(
            request,
            [
                r"consumer group ([a-zA-Z0-9._-]+)",
                r"group id ([a-zA-Z0-9._-]+)",
                r"uses group ([a-zA-Z0-9._-]+)",
            ],
            default=f"{slugify(job_name)}-group",
        )
        key_field = self._match(
            request,
            [
                r"keys? by ([a-zA-Z0-9._-]+)",
                r"keyed by ([a-zA-Z0-9._-]+)",
                r"use[s]? ([a-zA-Z0-9._-]+) as the key",
            ],
            default="userId",
        )
        rule_expression = self._match(
            request,
            [
                r"(?:emits?|trigger(?:s)?) .* when (.+?)(?:\.|$)",
                r"rule[: ]+(.+?)(?:\.|$)",
                r"if (.+?)(?:\.|$)",
            ],
            default="event.value > 0",
        )

        return {
            "template_id": "flink_kafka_rule_job",
            "job_name": job_name.strip(),
            "job_class_name": to_pascal_case(job_name),
            "package_name": "com.example",
            "input_topic": input_topic.strip(),
            "output_topic": output_topic.strip(),
            "consumer_group": consumer_group.strip(),
            "key_field": key_field.strip(),
            "rule_expression": rule_expression.strip(),
            "bootstrap_servers": self.default_bootstrap_servers,
            "input_schema_class": "InputEvent",
            "output_schema_class": "OutputEvent",
        }

    def _match(self, text: str, patterns: list[str], default: str) -> str:
        """Return the first matching regex group or a fallback value."""
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        return default
