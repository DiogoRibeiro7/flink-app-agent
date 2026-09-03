"""Small clarification-question builder for ambiguity failures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .ambiguity import AmbiguityAssessment
from .spec import (
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
    SESSION_WINDOW_AGGREGATION_RULE_TYPE,
)


@dataclass(frozen=True)
class ClarificationQuestion:
    """One explicit clarification question for an ambiguous request."""

    code: str
    question: str
    fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable clarification question."""
        return {
            "code": self.code,
            "question": self.question,
            "fields": list(self.fields),
        }


def build_clarification_questions(
    assessment: AmbiguityAssessment,
    payload: dict[str, Any],
) -> tuple[ClarificationQuestion, ...]:
    """Return stable clarification questions for the current ambiguity findings."""
    questions: list[ClarificationQuestion] = []
    seen_codes: set[str] = set()

    for issue in assessment.issues:
        question = _question_for_issue(issue.code, payload)
        if question is None or question.code in seen_codes:
            continue
        questions.append(question)
        seen_codes.add(question.code)

    return tuple(questions)


def _question_for_issue(
    issue_code: str,
    payload: dict[str, Any],
) -> ClarificationQuestion | None:
    """Map one ambiguity issue to one narrow clarification question."""
    if issue_code == "missing_sink_topic":
        family = str(payload.get("job_family", "")).strip()
        if family == JOB_FAMILY_WINDOWED_AGGREGATION:
            wording = "Which Kafka topic should receive the aggregation results?"
        elif family == JOB_FAMILY_KEYED_RULE:
            wording = "Which Kafka topic should receive the inferred events?"
        else:
            wording = "Which Kafka sink topic should this job write to?"
        return ClarificationQuestion(
            code=issue_code,
            question=wording,
            fields=("sink_topic",),
        )

    if issue_code in {"missing_key_field", "unclear_key_field"}:
        return ClarificationQuestion(
            code=issue_code,
            question="Which field should the stream be keyed by?",
            fields=("key_by",),
        )

    if issue_code == "unclear_job_family":
        return ClarificationQuestion(
            code=issue_code,
            question=(
                "Should this job emit inferred events from keyed rules or produce "
                "windowed aggregations?"
            ),
            fields=("job_family",),
        )

    if issue_code == "conflicting_aggregation_intent":
        return ClarificationQuestion(
            code=issue_code,
            question=(
                "Should this request count events by key or emit inferred events "
                "for matching keyed patterns?"
            ),
            fields=("job_family", "rule_type"),
        )

    if issue_code == "request_family_mismatch":
        return ClarificationQuestion(
            code=issue_code,
            question=(
                "Should this request use the keyed temporal-rule family or the "
                "windowed-aggregation family, and which rule type should apply?"
            ),
            fields=("job_family", "rule_type"),
        )

    if issue_code == "vague_temporal_language":
        if payload.get("rule_type") == SESSION_WINDOW_AGGREGATION_RULE_TYPE:
            wording = "What session inactivity gap should be used, in minutes?"
        else:
            wording = "What explicit time window should be used, in minutes?"
        return ClarificationQuestion(
            code=issue_code,
            question=wording,
            fields=("time_window_minutes",),
        )

    return None
