"""Provider-backed extraction quality gate."""

from __future__ import annotations

from dataclasses import dataclass

from .ambiguity import AmbiguityAssessment
from .spec import (
    ALLOWED_RULE_TYPE,
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
    SESSION_WINDOW_AGGREGATION_RULE_TYPE,
    WINDOWED_AGGREGATION_RULE_TYPE,
)

PROVIDER_QUALITY_ACCEPTABLE = "acceptable"
PROVIDER_QUALITY_AMBIGUOUS = "ambiguous"
PROVIDER_QUALITY_UNUSABLE = "unusable"

_ESSENTIAL_PROVIDER_FIELDS: frozenset[str] = frozenset(
    {
        "job_family",
        "job_name",
        "source_topic",
        "key_by",
        "event_time_field",
        "input_event_name",
        "output_event_name",
        "rule_type",
        "rule_condition",
        "time_window_minutes",
    }
)


@dataclass(frozen=True)
class ProviderQualityFinding:
    """One explicit provider-quality finding."""

    code: str
    message: str
    fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProviderQualityAssessment:
    """Result of provider quality gating for one normalized payload."""

    category: str
    findings: tuple[ProviderQualityFinding, ...] = ()

    @property
    def summary(self) -> str:
        """Return a short stable summary for logs and reports."""
        if not self.findings:
            return self.category
        return "; ".join(finding.message for finding in self.findings)


@dataclass(frozen=True)
class ProviderPayloadQualityGate:
    """Apply a narrow quality gate to provider-backed payloads."""

    def assess(
        self,
        payload: dict[str, object],
        ambiguity: AmbiguityAssessment,
    ) -> ProviderQualityAssessment:
        """Classify provider output as acceptable, ambiguous, or unusable."""
        findings: list[ProviderQualityFinding] = []

        missing_essential_fields = sorted(
            field for field in _ESSENTIAL_PROVIDER_FIELDS if payload.get(field) is None
        )
        if missing_essential_fields:
            findings.append(
                ProviderQualityFinding(
                    code="missing_essential_fields",
                    message=(
                        "Provider output is missing essential fields: "
                        + ", ".join(missing_essential_fields)
                    ),
                    fields=tuple(missing_essential_fields),
                )
            )

        coherence_finding = self._check_family_specific_coherence(payload)
        if coherence_finding is not None:
            findings.append(coherence_finding)

        if findings:
            return ProviderQualityAssessment(
                category=PROVIDER_QUALITY_UNUSABLE,
                findings=tuple(findings),
            )

        if ambiguity.is_ambiguous:
            ambiguity_codes = ", ".join(issue.code for issue in ambiguity.issues)
            return ProviderQualityAssessment(
                category=PROVIDER_QUALITY_AMBIGUOUS,
                findings=(
                    ProviderQualityFinding(
                        code="ambiguity_present",
                        message=(
                            "Provider output remains ambiguous after normalization: "
                            f"{ambiguity_codes}"
                        ),
                    ),
                ),
            )

        return ProviderQualityAssessment(category=PROVIDER_QUALITY_ACCEPTABLE)

    def _check_family_specific_coherence(
        self,
        payload: dict[str, object],
    ) -> ProviderQualityFinding | None:
        """Return a quality finding when normalized family/rule metadata disagree."""
        family = payload.get("job_family")
        rule_type = payload.get("rule_type")
        if (
            family == JOB_FAMILY_KEYED_RULE
            and rule_type is not None
            and rule_type != ALLOWED_RULE_TYPE
        ):
            return ProviderQualityFinding(
                code="family_rule_mismatch",
                message=(
                    "Provider output declares a keyed temporal rule with an "
                    "incompatible normalized rule type."
                ),
                fields=("job_family", "rule_type"),
            )
        if (
            family == JOB_FAMILY_WINDOWED_AGGREGATION
            and rule_type is not None
            and rule_type
            not in {
                WINDOWED_AGGREGATION_RULE_TYPE,
                SESSION_WINDOW_AGGREGATION_RULE_TYPE,
            }
        ):
            return ProviderQualityFinding(
                code="family_rule_mismatch",
                message=(
                    "Provider output declares a windowed aggregation with an "
                    "incompatible normalized rule type."
                ),
                fields=("job_family", "rule_type"),
            )
        return None
