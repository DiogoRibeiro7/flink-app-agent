"""Explicit policy layer for handling ambiguity findings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .ambiguity import AmbiguityAssessment, AmbiguityIssue, AmbiguousRequestError
from .clarification import build_clarification_questions
from .spec import JOB_FAMILY_KEYED_RULE, JOB_FAMILY_WINDOWED_AGGREGATION

MINOR_AMBIGUITY_POLICY = "minor_defaults"
FAIL_AMBIGUITY_POLICY = "fail"
VALID_AMBIGUITY_POLICIES = (FAIL_AMBIGUITY_POLICY, MINOR_AMBIGUITY_POLICY)
DEFAULT_AMBIGUITY_POLICY = FAIL_AMBIGUITY_POLICY

MINOR_SEVERITY = "minor"
MAJOR_SEVERITY = "major"


@dataclass(frozen=True)
class ClassifiedAmbiguity:
    """One ambiguity issue paired with its severity."""

    issue: AmbiguityIssue
    severity: str


@dataclass(frozen=True)
class AmbiguityPolicyResult:
    """Decision returned by the ambiguity policy layer."""

    policy_name: str
    status: str
    classification: str
    applied_defaults: dict[str, Any]
    warnings: tuple[str, ...]
    classified_issues: tuple[ClassifiedAmbiguity, ...]

    @property
    def should_continue(self) -> bool:
        """Return whether the pipeline may continue."""
        return self.status != "failed"


@dataclass(frozen=True)
class AmbiguityPolicy:
    """Apply a small explicit policy to ambiguity findings."""

    name: str = DEFAULT_AMBIGUITY_POLICY

    def apply(self, assessment: AmbiguityAssessment, payload: dict[str, Any]) -> AmbiguityPolicyResult:
        """Return the policy decision for one ambiguity assessment."""
        classified_issues = tuple(
            ClassifiedAmbiguity(issue=issue, severity=_classify_issue(issue, payload))
            for issue in assessment.issues
        )
        classification = _overall_classification(classified_issues)
        if not classified_issues:
            return AmbiguityPolicyResult(
                policy_name=self.name,
                status="clear",
                classification="clear",
                applied_defaults={},
                warnings=(),
                classified_issues=(),
            )

        if self.name == FAIL_AMBIGUITY_POLICY:
            raise AmbiguousRequestError(
                assessment=assessment,
                policy_name=self.name,
                policy_result="failed",
                clarification_questions=build_clarification_questions(assessment, payload),
            )

        if classification == MAJOR_SEVERITY:
            raise AmbiguousRequestError(
                assessment=assessment,
                policy_name=self.name,
                policy_result="failed_major",
                clarification_questions=build_clarification_questions(assessment, payload),
            )

        applied_defaults = _resolve_minor_defaults(classified_issues, payload)
        warnings = tuple(
            f"Applied safe default for {field}: {value}"
            for field, value in sorted(applied_defaults.items())
        )
        return AmbiguityPolicyResult(
            policy_name=self.name,
            status="used_safe_defaults",
            classification=classification,
            applied_defaults=applied_defaults,
            warnings=warnings,
            classified_issues=classified_issues,
        )


def _classify_issue(issue: AmbiguityIssue, payload: dict[str, Any]) -> str:
    """Return the severity for one ambiguity issue."""
    if issue.code == "missing_sink_topic":
        family = payload.get("job_family")
        if family in {JOB_FAMILY_KEYED_RULE, JOB_FAMILY_WINDOWED_AGGREGATION}:
            return MINOR_SEVERITY
    return MAJOR_SEVERITY


def _overall_classification(classified_issues: tuple[ClassifiedAmbiguity, ...]) -> str:
    """Return the highest-severity classification for the assessment."""
    if not classified_issues:
        return "clear"
    if any(item.severity == MAJOR_SEVERITY for item in classified_issues):
        return MAJOR_SEVERITY
    return MINOR_SEVERITY


def _resolve_minor_defaults(
    classified_issues: tuple[ClassifiedAmbiguity, ...],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Return deterministic defaults for supported minor ambiguity issues."""
    defaults: dict[str, Any] = {}
    for item in classified_issues:
        if item.severity != MINOR_SEVERITY:
            continue
        if item.issue.code == "missing_sink_topic":
            defaults["sink_topic"] = _default_sink_topic_for_family(str(payload.get("job_family", "")))
    return defaults


def _default_sink_topic_for_family(job_family: str) -> str:
    """Return the deterministic sink topic used for a supported family."""
    if job_family == JOB_FAMILY_WINDOWED_AGGREGATION:
        return "aggregated-events"
    if job_family == JOB_FAMILY_KEYED_RULE:
        return "inferred-events"
    raise ValueError(f"No safe sink-topic default is defined for job family '{job_family}'.")
