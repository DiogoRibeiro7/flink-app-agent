"""Provider adapter for model-backed extraction.

This module defines the boundary between external provider clients and the
internal extraction pipeline. It handles three concerns:

1. **Message construction** — formatting the extraction prompt and user request
   into the provider's expected message structure.
2. **Client invocation** — calling the provider through a minimal protocol that
   any provider SDK can implement.
3. **Response parsing** — extracting clean JSON from the provider's raw response,
   which may contain markdown fences or surrounding text.

The rest of the codebase never imports a provider SDK. It only sees
``ProviderCallable`` from ``llm.py``. This module is the only place that
knows about message roles, response cleanup, and client protocols.

Concrete integrations implement ``ProviderClient`` and pass it to
``ProviderAdapter``. The adapter's ``call`` method is the
``ProviderCallable`` that plugs into the extraction pipeline.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Protocol

from .constants import ProviderExtractionError
from .llm import ProviderCallable


@dataclass(frozen=True)
class Message:
    """Single message in a provider conversation."""

    role: str
    content: str


class ProviderClient(Protocol):
    """Minimal interface that a provider SDK adapter must implement.

    A concrete implementation wraps the provider's SDK and handles
    authentication, HTTP transport, and model selection internally.
    This protocol only exposes what the adapter needs.
    """

    def complete(self, messages: list[Message]) -> str:
        """Send messages to the provider and return the raw response text."""
        ...


@dataclass(frozen=True)
class ProviderAdapter:
    """Adapt a provider client into the internal extraction callable.

    This adapter is the single place that knows how to:
    - build the system/user message pair from a prompt and request
    - call the provider client
    - parse the raw response into clean JSON

    Usage::

        client = MyProviderClient(api_key="...")
        adapter = ProviderAdapter(client=client)
        # adapter.call is a ProviderCallable
        extractor = build_provider_spec_extractor(adapter.call)
    """

    client: ProviderClient

    def call(self, request: str, prompt: str) -> str:
        """Build messages, call the provider, parse the response.

        This method matches the ``ProviderCallable`` signature so it can
        be passed directly to ``build_provider_spec_extractor``.
        """
        messages = self.build_messages(request, prompt)
        try:
            raw_response = self.client.complete(messages)
        except Exception as exc:
            raise ProviderExtractionError(
                f"Provider client call failed: {exc}"
            ) from exc
        return self.parse_response(raw_response)

    def build_messages(self, request: str, prompt: str) -> list[Message]:
        """Construct the message list for spec extraction.

        The system message carries the extraction prompt (loaded from
        ``extract_spec.md``). The user message carries the plain-English
        Flink job request.
        """
        return [
            Message(role="system", content=prompt),
            Message(role="user", content=request),
        ]

    def parse_response(self, raw_response: str) -> str:
        """Extract exactly one JSON object from the provider response.

        Provider responses may wrap JSON in markdown fences or surround it
        with explanatory prose. Parsing is JSON-aware rather than based on a
        greedy brace regular expression, so nested objects remain intact.
        Multiple top-level JSON values are rejected as ambiguous output.
        """
        cleaned = _strip_markdown_fences(raw_response).strip()
        return _extract_single_json_object(cleaned)


def build_provider_callable(client: ProviderClient) -> ProviderCallable:
    """Create a ``ProviderCallable`` from a ``ProviderClient``.

    This is the recommended way to bridge a concrete provider client
    into the extraction pipeline::

        callable = build_provider_callable(my_client)
        extractor = build_provider_spec_extractor(callable)
    """
    return ProviderAdapter(client=client).call


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences wrapping a JSON block."""
    match = re.search(
        r"```(?:json)?\s*\n(.*?)```",
        text,
        flags=re.DOTALL,
    )
    if match:
        return match.group(1).strip()
    return text


def _extract_single_json_object(text: str) -> str:
    """Return the only top-level JSON object embedded in ``text``."""
    decoder = json.JSONDecoder()
    candidates: list[tuple[object, str]] = []
    index = 0

    while index < len(text):
        object_start = text.find("{", index)
        array_start = text.find("[", index)
        starts = [start for start in (object_start, array_start) if start >= 0]
        if not starts:
            break
        start = min(starts)
        try:
            value, consumed = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            index = start + 1
            continue
        candidates.append((value, text[start : start + consumed].strip()))
        index = start + consumed

    if not candidates:
        raise ProviderExtractionError(
            "Could not extract valid JSON object from provider response."
        )
    if len(candidates) > 1:
        raise ProviderExtractionError(
            "Provider response contains multiple JSON objects / multiple JSON values; expected exactly one object."
        )

    value, candidate = candidates[0]
    if not isinstance(value, dict):
        raise ProviderExtractionError(
            "Provider response JSON must be an object, not a container around one."
        )
    return candidate
