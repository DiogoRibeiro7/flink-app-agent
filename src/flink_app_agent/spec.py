"""Strict internal specification model for the v0.1 agent."""

from __future__ import annotations

import re
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


FILESYSTEM_SAFE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
ALLOWED_RULE_TYPE = "two_events_within_window"


class FlinkJobSpec(BaseModel):
    """Validated internal specification for the single v0.1 Flink job family."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    supported_rule_types: ClassVar[set[str]] = {ALLOWED_RULE_TYPE}

    job_name: str = Field(description="Filesystem-safe name for the generated job.")
    source_topic: str = Field(description="Kafka source topic name.")
    sink_topic: str = Field(description="Kafka sink topic name.")
    key_by: str = Field(description="Stream key field name.")
    event_time_field: str = Field(description="Event-time field name.")
    input_event_name: str = Field(description="Input event class name.")
    output_event_name: str = Field(description="Output event class name.")
    rule_type: str = Field(description="Supported rule type for v0.1.")
    rule_condition: str = Field(description="Human-readable rule condition.")
    time_window_minutes: int = Field(gt=0, description="Positive time window length.")

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, value: str) -> str:
        """Normalize and validate the generated job name."""
        normalized = cls.normalize_job_name(value)
        if not normalized:
            raise ValueError("job_name must contain at least one letter or number.")
        if not FILESYSTEM_SAFE_NAME_PATTERN.fullmatch(normalized):
            raise ValueError(
                "job_name must be filesystem-safe and use only letters, numbers, dots, underscores, and hyphens."
            )
        return normalized

    @field_validator(
        "source_topic",
        "sink_topic",
        "key_by",
        "event_time_field",
        "input_event_name",
        "output_event_name",
        "rule_condition",
    )
    @classmethod
    def validate_not_empty(cls, value: str, info: ValidationInfo) -> str:
        """Reject blank required string fields."""
        if not value:
            raise ValueError(f"{info.field_name} must not be empty.")
        return value

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, value: str) -> str:
        """Allow only the single supported v0.1 rule type."""
        if value != ALLOWED_RULE_TYPE:
            raise ValueError(
                f"rule_type must be '{ALLOWED_RULE_TYPE}' for v0.1."
            )
        return value

    @classmethod
    def from_llm_payload(cls, payload: dict[str, Any]) -> "FlinkJobSpec":
        """Build a validated spec from a plain extracted payload."""
        return cls.model_validate(payload)

    @classmethod
    def demo(cls) -> "FlinkJobSpec":
        """Return a small demo spec for tests and examples."""
        return cls(
            job_name="fraud-alert-job",
            source_topic="payments",
            sink_topic="alerts",
            key_by="account_id",
            event_time_field="event_time",
            input_event_name="InputEvent",
            output_event_name="AlertEvent",
            rule_type=ALLOWED_RULE_TYPE,
            rule_condition="second payment occurs within 10 minutes",
            time_window_minutes=10,
        )

    def to_template_dict(self) -> dict[str, str]:
        """Return a flat placeholder dictionary for template substitution."""
        return {
            "JOB_NAME": self.job_name,
            "SOURCE_TOPIC": self.source_topic,
            "SINK_TOPIC": self.sink_topic,
            "KEY_BY": self.key_by,
            "EVENT_TIME_FIELD": self.event_time_field,
            "INPUT_EVENT_NAME": self.input_event_name,
            "OUTPUT_EVENT_NAME": self.output_event_name,
            "RULE_TYPE": self.rule_type,
            "RULE_CONDITION": self.rule_condition,
            "TIME_WINDOW_MINUTES": str(self.time_window_minutes),
        }

    @staticmethod
    def normalize_job_name(value: str) -> str:
        """Normalize a job name into a lowercase filesystem-safe identifier."""
        normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip().lower())
        normalized = re.sub(r"-{2,}", "-", normalized)
        return normalized.strip("-.")
