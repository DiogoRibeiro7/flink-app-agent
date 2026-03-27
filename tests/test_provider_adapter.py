"""Tests for the provider adapter module."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import ProviderExtractionError
from flink_app_agent.provider_adapter import (
    Message,
    ProviderAdapter,
    build_provider_callable,
    _strip_markdown_fences,
)


VALID_SPEC_PAYLOAD = {
    "job_family": "keyed_temporal_rule",
    "job_name": "fraud-alert-job",
    "source_topic": "payments",
    "sink_topic": "alerts",
    "key_by": "account_id",
    "event_time_field": "event_time",
    "input_event_name": "InputEvent",
    "output_event_name": "AlertEvent",
    "rule_type": "two_events_within_window",
    "rule_condition": "second payment within 10 minutes",
    "time_window_minutes": 10,
}


class FakeClient:
    """Deterministic fake provider client for tests."""

    def __init__(self, response: str) -> None:
        self.response = response
        self.received_messages: list[Message] = []

    def complete(self, messages: list[Message]) -> str:
        self.received_messages = list(messages)
        return self.response


class FailingClient:
    """Provider client that always raises."""

    def complete(self, messages: list[Message]) -> str:
        raise ConnectionError("provider unreachable")


# --- Message construction ---


def test_build_messages_creates_system_and_user_pair() -> None:
    """The adapter should build a system message from the prompt and a user message from the request."""
    client = FakeClient(json.dumps(VALID_SPEC_PAYLOAD))
    adapter = ProviderAdapter(client=client)

    messages = adapter.build_messages("my request", "my prompt")

    assert len(messages) == 2
    assert messages[0].role == "system"
    assert messages[0].content == "my prompt"
    assert messages[1].role == "user"
    assert messages[1].content == "my request"


def test_call_passes_messages_to_client() -> None:
    """The adapter should pass constructed messages to the client."""
    client = FakeClient(json.dumps(VALID_SPEC_PAYLOAD))
    adapter = ProviderAdapter(client=client)

    adapter.call("the request", "the prompt")

    assert len(client.received_messages) == 2
    assert client.received_messages[0].content == "the prompt"
    assert client.received_messages[1].content == "the request"


# --- Response parsing ---


def test_parse_response_accepts_plain_json() -> None:
    """Plain JSON should parse successfully."""
    client = FakeClient("")
    adapter = ProviderAdapter(client=client)

    result = adapter.parse_response(json.dumps(VALID_SPEC_PAYLOAD))

    assert json.loads(result) == VALID_SPEC_PAYLOAD


def test_parse_response_strips_markdown_json_fence() -> None:
    """JSON wrapped in ```json fences should be extracted."""
    wrapped = f"Here is the result:\n```json\n{json.dumps(VALID_SPEC_PAYLOAD)}\n```\nDone."
    client = FakeClient("")
    adapter = ProviderAdapter(client=client)

    result = adapter.parse_response(wrapped)

    assert json.loads(result) == VALID_SPEC_PAYLOAD


def test_parse_response_strips_plain_markdown_fence() -> None:
    """JSON wrapped in plain ``` fences should be extracted."""
    wrapped = f"```\n{json.dumps(VALID_SPEC_PAYLOAD)}\n```"
    client = FakeClient("")
    adapter = ProviderAdapter(client=client)

    result = adapter.parse_response(wrapped)

    assert json.loads(result) == VALID_SPEC_PAYLOAD


def test_parse_response_rejects_non_json() -> None:
    """Non-JSON responses should fail clearly."""
    client = FakeClient("")
    adapter = ProviderAdapter(client=client)

    with pytest.raises(ProviderExtractionError, match="Could not extract valid JSON"):
        adapter.parse_response("I don't know how to do that.")


def test_parse_response_rejects_empty_response() -> None:
    """Empty responses should fail clearly."""
    client = FakeClient("")
    adapter = ProviderAdapter(client=client)

    with pytest.raises(ProviderExtractionError, match="Could not extract valid JSON"):
        adapter.parse_response("")


# --- Full call flow ---


def test_call_returns_clean_json_from_provider() -> None:
    """A full call should return parseable JSON matching the provider response."""
    payload_json = json.dumps(VALID_SPEC_PAYLOAD)
    client = FakeClient(payload_json)
    adapter = ProviderAdapter(client=client)

    result = adapter.call("request", "prompt")

    assert json.loads(result) == VALID_SPEC_PAYLOAD


def test_call_handles_markdown_wrapped_response() -> None:
    """A full call with markdown-wrapped response should still return clean JSON."""
    wrapped = f"```json\n{json.dumps(VALID_SPEC_PAYLOAD)}\n```"
    client = FakeClient(wrapped)
    adapter = ProviderAdapter(client=client)

    result = adapter.call("request", "prompt")

    assert json.loads(result) == VALID_SPEC_PAYLOAD


def test_call_wraps_client_exceptions() -> None:
    """Client exceptions should be wrapped in ProviderExtractionError."""
    adapter = ProviderAdapter(client=FailingClient())

    with pytest.raises(ProviderExtractionError, match="Provider client call failed"):
        adapter.call("request", "prompt")


def test_call_fails_on_invalid_client_response() -> None:
    """A client returning non-JSON should fail at the parsing stage."""
    client = FakeClient("not json at all")
    adapter = ProviderAdapter(client=client)

    with pytest.raises(ProviderExtractionError, match="Could not extract valid JSON"):
        adapter.call("request", "prompt")


# --- build_provider_callable ---


def test_build_provider_callable_returns_callable() -> None:
    """The helper should return a ProviderCallable from a client."""
    client = FakeClient(json.dumps(VALID_SPEC_PAYLOAD))

    fn = build_provider_callable(client)

    assert callable(fn)
    result = fn("request", "prompt")
    assert json.loads(result) == VALID_SPEC_PAYLOAD


# --- Markdown fence stripping ---


def test_strip_markdown_fences_with_json_tag() -> None:
    """```json fences should be stripped."""
    assert _strip_markdown_fences('```json\n{"a":1}\n```') == '{"a":1}'


def test_strip_markdown_fences_with_no_tag() -> None:
    """Plain ``` fences should be stripped."""
    assert _strip_markdown_fences('```\n{"a":1}\n```') == '{"a":1}'


def test_strip_markdown_fences_passthrough() -> None:
    """Text without fences should pass through unchanged."""
    assert _strip_markdown_fences('{"a":1}') == '{"a":1}'


def test_strip_markdown_fences_with_surrounding_text() -> None:
    """Fences with surrounding explanation should extract only the fenced content."""
    text = 'Here you go:\n```json\n{"a":1}\n```\nHope that helps!'
    assert _strip_markdown_fences(text) == '{"a":1}'
