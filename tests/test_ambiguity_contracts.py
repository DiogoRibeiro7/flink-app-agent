"""Regression tests for ambiguity-policy and clarification contracts."""

from __future__ import annotations

import pytest

from flink_app_agent.ambiguity import AmbiguityAssessment, AmbiguityIssue
from flink_app_agent.ambiguity_policy import AmbiguityPolicy
from flink_app_agent.clarification import build_clarification_questions
from flink_app_agent.spec import SESSION_WINDOW_AGGREGATION_RULE_TYPE


def test_ambiguity_policy_rejects_unknown_policy_name() -> None:
    """Direct policy use must not silently behave like minor_defaults."""
    assessment = AmbiguityAssessment(
        issues=(AmbiguityIssue(code="missing_sink_topic", message="missing"),)
    )

    with pytest.raises(ValueError, match="Invalid ambiguity policy"):
        AmbiguityPolicy("unknown").apply(
            assessment,
            payload={"job_family": "keyed_temporal_rule"},
        )


def test_session_window_clarification_asks_for_inactivity_gap() -> None:
    """Session ambiguity must describe time_window_minutes as an inactivity gap."""
    assessment = AmbiguityAssessment(
        issues=(AmbiguityIssue(code="vague_temporal_language", message="vague"),)
    )

    questions = build_clarification_questions(
        assessment,
        payload={"rule_type": SESSION_WINDOW_AGGREGATION_RULE_TYPE},
    )

    assert questions[0].question == "What session inactivity gap should be used, in minutes?"
