"""Structured specification model for the first agent version."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


FILESYSTEM_SAFE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
ALLOWED_RULE_TYPE = "two_events_within_window"


class FlinkJobSpec(BaseModel):
    """Strict Flink job specification for the first supported use case."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    job_name: str = Field(description="Filesystem-safe name for the generated job.")
    source_topic: str = Field(description="Kafka source topic name.")
    sink_topic: str = Field(description="Kafka sink topic name.")
    key_by: str = Field(description="Field used to key the Flink stream.")
    event_time_field: str = Field(description="Field representing event time.")
    input_event_name: str = Field(description="Name of the input event model.")
    output_event_name: str = Field(description="Name of the output event model.")
    rule_type: Literal["two_events_within_window"] = Field(
        description="Supported rule type for the first version."
    )
    rule_condition: str = Field(description="Human-readable rule condition.")
    time_window_minutes: int = Field(
        gt=0,
        description="Positive window length in minutes used by the rule.",
    )

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, value: str) -> str:
        """Ensure the job name is safe to use as a directory or file name."""
        if not FILESYSTEM_SAFE_NAME_PATTERN.fullmatch(value):
            raise ValueError(
                "job_name must be filesystem-safe and may contain only letters, numbers, dots, underscores, and hyphens."
            )
        return value

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
        """Reject blank values for required string fields."""
        if not value:
            raise ValueError(f"{info.field_name} must not be empty.")
        return value

    @classmethod
    def from_llm_payload(cls, payload: dict[str, Any]) -> "FlinkJobSpec":
        """Create a validated spec from a plain payload."""
        return cls.model_validate(payload)

    @classmethod
    def demo(cls) -> "FlinkJobSpec":
        """Create a small demo spec for local development and tests."""
        return cls(
            job_name="fraud-alert-job",
            source_topic="payments",
            sink_topic="alerts",
            key_by="account_id",
            event_time_field="event_time",
            input_event_name="PaymentEvent",
            output_event_name="AlertEvent",
            rule_type=ALLOWED_RULE_TYPE,
            rule_condition="second payment occurs within 10 minutes with amount > 5000",
            time_window_minutes=10,
        )

    def to_template_dict(self) -> dict[str, str]:
        """Return a plain string dictionary suitable for template substitution."""
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
