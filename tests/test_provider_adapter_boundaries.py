"""Boundary regression tests for provider response JSON extraction."""

from __future__ import annotations

import pytest

from flink_app_agent.constants import ProviderExtractionError
from flink_app_agent.provider_adapter import ProviderAdapter


class _UnusedClient:
    def complete(self, messages):  # pragma: no cover - not used in these tests
        raise AssertionError("client should not be called")


def test_array_wrapped_object_is_rejected() -> None:
    adapter = ProviderAdapter(client=_UnusedClient())

    with pytest.raises(ProviderExtractionError, match="must be an object"):
        adapter.parse_response('[{"job_family": "keyed_temporal_rule"}]')


def test_malformed_object_then_valid_object_uses_valid_top_level_object() -> None:
    adapter = ProviderAdapter(client=_UnusedClient())

    result = adapter.parse_response('prefix {not-json} then {"a": 1}')

    assert result == '{"a": 1}'


def test_multiple_top_level_json_values_are_rejected() -> None:
    adapter = ProviderAdapter(client=_UnusedClient())

    with pytest.raises(ProviderExtractionError, match="multiple JSON values"):
        adapter.parse_response('{"a": 1} then [1, 2]')


def test_non_json_text_is_rejected() -> None:
    adapter = ProviderAdapter(client=_UnusedClient())

    with pytest.raises(ProviderExtractionError, match="Could not extract valid JSON object"):
        adapter.parse_response("nothing parseable here")
