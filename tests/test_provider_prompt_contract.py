"""Regression tests for provider extraction prompt contracts."""

from __future__ import annotations

from pathlib import Path


def test_extract_spec_prompt_does_not_invite_provider_defaults() -> None:
    prompt_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "flink_app_agent"
        / "prompts"
        / "extract_spec.md"
    )
    prompt = prompt_path.read_text(encoding="utf-8")

    assert "Missing information must remain missing" in prompt
    assert "do not invent `sink_topic`" in prompt
    assert "omit that field from the JSON object" in prompt
    assert "application ambiguity-policy layer owns" in prompt
