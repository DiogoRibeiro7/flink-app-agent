"""Structured output normalization for provider-backed extraction.

This module sits between the raw provider JSON and the strict spec
validation. It normalizes common provider variations and rejects output
that cannot be safely mapped to the internal spec model.

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

import math
from typing import Any

from .constants import ProviderExtractionError
from .spec import (
    ALLOWED_RULE_TYPE,
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
    SESSION_WINDOW_AGGREGATION_RULE_TYPE,
    WINDOWED_AGGREGATION_RULE_TYPE,
)

KNOWN_FIELDS: frozenset[str] = frozenset({
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

VALID_FAMILY_RULE_PAIRS: dict[str, frozenset[str]] = {
    JOB_FAMILY_KEYED_RULE: frozenset({ALLOWED_RULE_TYPE}),
    JOB_FAMILY_WINDOWED_AGGREGATION: frozenset(
        {
            WINDOWED_AGGREGATION_RULE_TYPE,
            SESSION_WINDOW_AGGREGATION_RULE_TYPE,
        }
    ),
}


def normalize_provider_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw provider payload into a shape ready for spec validation.

    Steps:
    1. Map aliased field names to canonical names
    2. Strip fields not in the required set
    3. Coerce types (string numbers to int)
    4. Validate family/rule_type coherence when enough information exists

    Raises:
        ProviderExtractionError: If the payload has incoherent family/rule_type
            values or values that cannot be coerced safely.
    """
    mapped = _apply_aliases(raw)
    filtered = _strip_unknown_fields(mapped)
    coerced = _coerce_types(filtered)
    _check_family_rule_coherence(coerced)
    return coerced


def _apply_aliases(raw: dict[str, Any]) -> dict[str, Any]:
    """Map aliased keys to canonical field names without hiding conflicts."""
    result: dict[str, Any] = {}
    sources: dict[str, str] = {}
    for key, value in raw.items():
        canonical = FIELD_ALIASES.get(key, key)
        if canonical in result and (
            isinstance(result[canonical], bool) != isinstance(value, bool)
            or result[canonical] != value
        ):
            previous_key = sources[canonical]
            raise ProviderExtractionError(
                f"Conflicting provider fields '{previous_key}' and '{key}' "
                f"both map to '{canonical}'."
            )
        if canonical not in result or key == canonical:
            result[canonical] = value
            sources[canonical] = key
    return result


def _strip_unknown_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove keys that are not in the known field set."""
    return {k: v for k, v in payload.items() if k in KNOWN_FIELDS}


def _coerce_types(payload: dict[str, Any]) -> dict[str, Any]:
    """Coerce field values to expected types where safe."""
    result = dict(payload)

    twm = result.get("time_window_minutes")
    if isinstance(twm, bool):
        raise ProviderExtractionError(
            "time_window_minutes must be an integer, got bool."
        )
    if isinstance(twm, str):
        try:
            result["time_window_minutes"] = int(twm)
        except ValueError as exc:
            raise ProviderExtractionError(
                f"time_window_minutes must be an integer, got '{twm}'."
            ) from exc
    elif isinstance(twm, float):
        if not math.isfinite(twm) or not twm.is_integer():
            raise ProviderExtractionError(
                f"time_window_minutes must be a finite whole number, got {twm}."
            )
        result["time_window_minutes"] = int(twm)

    for field in (
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
    ):
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

    allowed_rules = VALID_FAMILY_RULE_PAIRS.get(family)
    if allowed_rules is not None and rule_type not in allowed_rules:
        allowed_text = ", ".join(sorted(allowed_rules))
        raise ProviderExtractionError(
            f"Incoherent provider output: job_family '{family}' "
            f"requires rule_type in {{{allowed_text}}}, got '{rule_type}'."
        )
