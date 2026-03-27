"""Structured output normalization for provider-backed extraction.

This module sits between the raw provider JSON and the strict spec
validation. It enforces a narrow expected shape, normalizes common
provider variations, and rejects output that cannot be safely mapped
to the internal spec model.

The normalization layer is intentionally separate from ``spec.py``
validation. Spec validation enforces the final contract (field formats,
allowed values). This module handles the messier reality of provider
output: extra keys, type mismatches, camelCase aliases, and
family/rule_type incoherence.

Pipeline position::

    Provider JSON string
        → json.loads()          (ProviderSpecPayloadExtractor)
        → normalize()           (this module)
        → PydanticSpecValidator  (spec.py)
        → FlinkJobSpec
"""

from __future__ import annotations

from typing import Any

from .constants import ProviderExtractionError
from .spec import (
    ALLOWED_RULE_TYPE,
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
    WINDOWED_AGGREGATION_RULE_TYPE,
)

REQUIRED_FIELDS: frozenset[str] = frozenset({
    "job_family",
    "job_name",
    "source_topic",
    "sink_topic",
    "key_by",
    "event_time_field",
    "input_event_name",
    "output_event_name",
    "rule_type",
    "rule_condition",
    "time_window_minutes",
})

# Map camelCase and other common aliases to canonical field names.
FIELD_ALIASES: dict[str, str] = {
    "jobFamily": "job_family",
    "job-family": "job_family",
    "jobName": "job_name",
    "job-name": "job_name",
    "sourceTopic": "source_topic",
    "source-topic": "source_topic",
    "sinkTopic": "sink_topic",
    "sink-topic": "sink_topic",
    "keyBy": "key_by",
    "key-by": "key_by",
    "eventTimeField": "event_time_field",
    "event-time-field": "event_time_field",
    "inputEventName": "input_event_name",
    "outputEventName": "output_event_name",
    "ruleType": "rule_type",
    "rule-type": "rule_type",
    "ruleCondition": "rule_condition",
    "rule-condition": "rule_condition",
    "timeWindowMinutes": "time_window_minutes",
    "time-window-minutes": "time_window_minutes",
    "window_minutes": "time_window_minutes",
    "windowMinutes": "time_window_minutes",
}

VALID_FAMILY_RULE_PAIRS: dict[str, str] = {
    JOB_FAMILY_KEYED_RULE: ALLOWED_RULE_TYPE,
    JOB_FAMILY_WINDOWED_AGGREGATION: WINDOWED_AGGREGATION_RULE_TYPE,
}


def normalize_provider_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw provider payload into a shape ready for spec validation.

    Steps:
    1. Map aliased field names to canonical names
    2. Strip fields not in the required set
    3. Check all required fields are present
    4. Coerce types (string numbers to int)
    5. Validate family/rule_type coherence

    Raises:
        ProviderExtractionError: If the payload is missing required fields,
            has incoherent family/rule_type, or has values that cannot be
            coerced.
    """
    mapped = _apply_aliases(raw)
    filtered = _strip_unknown_fields(mapped)
    _check_required_fields(filtered)
    coerced = _coerce_types(filtered)
    _check_family_rule_coherence(coerced)
    return coerced


def _apply_aliases(raw: dict[str, Any]) -> dict[str, Any]:
    """Map aliased keys to canonical field names."""
    result: dict[str, Any] = {}
    for key, value in raw.items():
        canonical = FIELD_ALIASES.get(key, key)
        if canonical not in result:
            result[canonical] = value
    return result


def _strip_unknown_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove keys that are not in the required field set."""
    return {k: v for k, v in payload.items() if k in REQUIRED_FIELDS}


def _check_required_fields(payload: dict[str, Any]) -> None:
    """Raise if any required fields are missing."""
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ProviderExtractionError(
            f"Provider output is missing required fields: {missing_list}"
        )


def _coerce_types(payload: dict[str, Any]) -> dict[str, Any]:
    """Coerce field values to expected types where safe."""
    result = dict(payload)

    twm = result.get("time_window_minutes")
    if isinstance(twm, str):
        try:
            result["time_window_minutes"] = int(twm)
        except ValueError:
            raise ProviderExtractionError(
                f"time_window_minutes must be an integer, got '{twm}'."
            )
    elif isinstance(twm, float):
        if twm != int(twm):
            raise ProviderExtractionError(
                f"time_window_minutes must be a whole number, got {twm}."
            )
        result["time_window_minutes"] = int(twm)

    for field in ("job_family", "job_name", "source_topic", "sink_topic",
                  "key_by", "event_time_field", "input_event_name",
                  "output_event_name", "rule_type", "rule_condition"):
        val = result.get(field)
        if val is not None and not isinstance(val, str):
            raise ProviderExtractionError(
                f"{field} must be a string, got {type(val).__name__}."
            )

    return result


def _check_family_rule_coherence(payload: dict[str, Any]) -> None:
    """Reject payloads where job_family and rule_type do not match."""
    family = payload.get("job_family", "")
    rule_type = payload.get("rule_type", "")

    expected_rule = VALID_FAMILY_RULE_PAIRS.get(family)
    if expected_rule is not None and rule_type != expected_rule:
        raise ProviderExtractionError(
            f"Incoherent provider output: job_family '{family}' "
            f"requires rule_type '{expected_rule}', got '{rule_type}'."
        )
