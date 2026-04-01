"""Tests for clarification-question generation on ambiguity failures."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.ambiguity import AmbiguityAssessment, AmbiguityIssue
from flink_app_agent.clarification import ClarificationQuestion, build_clarification_questions


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


def test_clarification_question_to_dict_is_machine_readable() -> None:
    """Clarification questions should serialize cleanly for future consumers."""
    question = ClarificationQuestion(
        code="vague_temporal_language",
        question="What explicit time window should be used, in minutes?",
        fields=("time_window_minutes",),
    )

    assert question.to_dict() == {
        "code": "vague_temporal_language",
        "question": "What explicit time window should be used, in minutes?",
        "fields": ["time_window_minutes"],
    }


def test_clarification_questions_cover_remaining_supported_issue_codes() -> None:
    """The builder should cover the remaining explicit ambiguity issue mappings."""
    assessment = AmbiguityAssessment(
        issues=(
            AmbiguityIssue(code="unclear_job_family", message="unclear family"),
            AmbiguityIssue(code="conflicting_aggregation_intent", message="conflicting intent"),
            AmbiguityIssue(code="vague_temporal_language", message="vague time"),
        )
    )

    questions = build_clarification_questions(assessment, payload={})

    assert [item.code for item in questions] == [
        "unclear_job_family",
        "conflicting_aggregation_intent",
        "vague_temporal_language",
    ]
    assert questions[0].question == (
        "Should this job emit inferred events from keyed rules or produce "
        "windowed aggregations?"
    )
    assert questions[1].question == (
        "Should this request count events by key or emit inferred events "
        "for matching keyed patterns?"
    )
    assert questions[2].question == "What explicit time window should be used, in minutes?"


def test_clarification_questions_use_generic_sink_wording_without_family() -> None:
    """Missing sink-topic questions should fall back to generic wording when family is unknown."""
    assessment = AmbiguityAssessment(
        issues=(AmbiguityIssue(code="missing_sink_topic", message="missing sink"),)
    )

    questions = build_clarification_questions(assessment, payload={})

    assert len(questions) == 1
    assert questions[0].question == "Which Kafka sink topic should this job write to?"
