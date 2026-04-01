"""Tests for clarification-question generation on ambiguity failures."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.ambiguity import AmbiguityAssessment, AmbiguityIssue
from flink_app_agent.clarification import build_clarification_questions


def test_clarification_questions_cover_supported_ambiguity_codes() -> None:
    """Known ambiguity codes should map to narrow clarification questions."""
    assessment = AmbiguityAssessment(
        issues=(
            AmbiguityIssue(
                code="missing_key_field",
                message="The request does not identify a key field.",
                fields=("key_by",),
            ),
            AmbiguityIssue(
                code="missing_sink_topic",
                message="The request does not identify a sink topic.",
                fields=("sink_topic",),
            ),
        )
    )

    questions = build_clarification_questions(
        assessment,
        payload={"job_family": "keyed_temporal_rule"},
    )

    assert [item.code for item in questions] == ["missing_key_field", "missing_sink_topic"]
    assert questions[0].question == "Which field should the stream be keyed by?"
    assert questions[1].question == "Which Kafka topic should receive the inferred events?"


def test_clarification_questions_deduplicate_repeated_issue_codes() -> None:
    """Repeated issue codes should not produce duplicate clarification prompts."""
    assessment = AmbiguityAssessment(
        issues=(
            AmbiguityIssue(code="missing_key_field", message="missing"),
            AmbiguityIssue(code="missing_key_field", message="still missing"),
        )
    )

    questions = build_clarification_questions(assessment, payload={})

    assert len(questions) == 1
    assert questions[0].code == "missing_key_field"
