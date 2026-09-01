"""Deterministic ambiguity assessment for extracted spec candidates."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from .request_taxonomy import REQUEST_CATEGORY_AMBIGUOUS
from .spec import JOB_FAMILY_KEYED_RULE, JOB_FAMILY_WINDOWED_AGGREGATION


@dataclass(frozen=True)
class AmbiguityIssue:
    """One explicit ambiguity found in an extracted candidate."""

    code: str
    message: str
    fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "code": self.code,
            "message": self.message,
            "fields": list(self.fields),
        }


@dataclass(frozen=True)
class AmbiguityAssessment:
    """Structured ambiguity summary for one extracted candidate."""

    issues: tuple[AmbiguityIssue, ...] = ()

    @property
    def is_ambiguous(self) -> bool:
        """Return whether any ambiguity issues were detected."""
        return bool(self.issues)

    def to_dict(self) -> dict[str, Any]:
        """Return a compact JSON-serializable ambiguity payload."""
        return {
            "is_ambiguous": self.is_ambiguous,
            "issues": [issue.to_dict() for issue in self.issues],
        }


class AmbiguousRequestError(ValueError):
    """Raised when a candidate payload remains ambiguous before validation."""

    def __init__(
        self,
        assessment: AmbiguityAssessment,
        policy_name: str | None = None,
        policy_result: str | None = None,
        clarification_questions: tuple[Any, ...] = (),
    ) -> None:
        """Store the structured ambiguity assessment and policy decision."""
        self.assessment = assessment
        self.policy_name = policy_name
        self.policy_result = policy_result
        self.clarification_questions = clarification_questions
        self.request_category = REQUEST_CATEGORY_AMBIGUOUS
        codes = ", ".join(issue.code for issue in assessment.issues)
        detail = f"Ambiguous request: {codes}"
        if policy_name is not None:
            detail = f"{detail} (policy={policy_name}"
            if policy_result is not None:
                detail = f"{detail}, result={policy_result}"
            detail = f"{detail})"
        super().__init__(detail)


@dataclass(frozen=True)
class CandidateAmbiguityAssessor:
    """Assess ambiguity before a candidate becomes a validated final spec."""

    def assess(
        self,
        request: str,
        payload: Mapping[str, Any],
    ) -> AmbiguityAssessment:
        """Return deterministic ambiguity findings for one candidate payload."""
        issues: list[AmbiguityIssue] = []

        family_issue = self._assess_job_family(request, payload)
        if family_issue is not None:
            issues.append(family_issue)

        if not _string_value(payload, "sink_topic"):
            issues.append(
                AmbiguityIssue(
                    code="missing_sink_topic",
                    message="The request does not identify a sink topic.",
                    fields=("sink_topic",),
                )
            )

        key_by = _string_value(payload, "key_by")
        if not key_by:
            issues.append(
                AmbiguityIssue(
                    code="missing_key_field",
                    message="The request does not identify a key field.",
                    fields=("key_by",),
                )
            )
        elif self._request_has_alternative_keys(request) or re.search(
            r"\b(?:or|and/or)\b", key_by, flags=re.IGNORECASE
        ):
            issues.append(
                AmbiguityIssue(
                    code="unclear_key_field",
                    message="The request contains multiple plausible key fields.",
                    fields=("key_by",),
                )
            )

        if self._has_vague_temporal_language(request, payload):
            issues.append(
                AmbiguityIssue(
                    code="vague_temporal_language",
                    message="The request refers to time imprecisely and does not define a numeric window.",
                    fields=("time_window_minutes",),
                )
            )

        return AmbiguityAssessment(issues=tuple(issues))

    def _assess_job_family(
        self,
        request: str,
        payload: Mapping[str, Any],
    ) -> AmbiguityIssue | None:
        """Return a family-related ambiguity issue when one is present."""
        lowered = request.lower()
        has_aggregation_signal = bool(
            re.search(r"\bcount(?: events?)?\b", lowered)
            or re.search(r"\baggregate count\b", lowered)
        )
        has_emit_signal = bool(re.search(r"\bemit\b", lowered))

        if has_aggregation_signal and has_emit_signal:
            return AmbiguityIssue(
                code="conflicting_aggregation_intent",
                message="The request mixes aggregation wording with emitted-event wording.",
                fields=("job_family", "rule_type", "output_event_name"),
            )

        family = _string_value(payload, "job_family")
        if family == JOB_FAMILY_KEYED_RULE and has_aggregation_signal and not has_emit_signal:
            return AmbiguityIssue(
                code="request_family_mismatch",
                message="The extracted keyed-rule family contradicts aggregation wording in the request.",
                fields=("job_family", "rule_type"),
            )
        if family == JOB_FAMILY_WINDOWED_AGGREGATION and has_emit_signal and not has_aggregation_signal:
            return AmbiguityIssue(
                code="request_family_mismatch",
                message="The extracted aggregation family contradicts emitted-event wording in the request.",
                fields=("job_family", "rule_type"),
            )
        if family in {JOB_FAMILY_KEYED_RULE, JOB_FAMILY_WINDOWED_AGGREGATION}:
            return None

        if has_aggregation_signal or has_emit_signal:
            return AmbiguityIssue(
                code="unclear_job_family",
                message="The request does not resolve to one supported job family with confidence.",
                fields=("job_family",),
            )
        return None

    @staticmethod
    def _request_has_alternative_keys(request: str) -> bool:
        """Return whether the request explicitly offers multiple key-field alternatives."""
        return bool(
            re.search(
                r"\b(?:key(?:ed|ing)? by|group by|partition by)\s+"
                r"[A-Za-z0-9_.-]+\s+(?:or|and/or)\s+[A-Za-z0-9_.-]+\b",
                request,
                flags=re.IGNORECASE,
            )
        )

    def _has_vague_temporal_language(
        self,
        request: str,
        payload: Mapping[str, Any],
    ) -> bool:
        """Return whether the request uses imprecise temporal wording."""
        if payload.get("time_window_minutes") is not None:
            return False
        lowered = request.lower()
        if not re.search(r"\b(?:within|every|over|during|for)\b", lowered):
            return False
        return bool(
            re.search(
                r"\b(?:few|several|some|short|brief|quick|recent|soon|later)\b",
                lowered,
            )
            or re.search(r"\b(?:minutes?|hours?|days?)\b", lowered)
        )


def _string_value(payload: Mapping[str, Any], field: str) -> str:
    """Return a normalized string value from the candidate payload."""
    value = payload.get(field)
    if isinstance(value, str):
        return value.strip()
    return ""
