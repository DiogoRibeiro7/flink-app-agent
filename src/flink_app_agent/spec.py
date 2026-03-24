"""Structured specification model for the first agent version."""

from __future__ import annotations

import re
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


FILESYSTEM_SAFE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_RULE_TYPE = "two_events_within_window"
ALLOWED_AGGREGATION_TYPE = "count_by_key_window"


class FlinkJobSpec(BaseModel):
    """Strict Flink job specification for the first supported use case."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    supported_rule_types: ClassVar[set[str]] = {ALLOWED_RULE_TYPE, ALLOWED_AGGREGATION_TYPE}

    job_name: str = Field(description="Filesystem-safe name for the generated job.")
    source_topic: str = Field(description="Kafka source topic name.")
    sink_topic: str = Field(description="Kafka sink topic name.")
    key_by: str = Field(description="Field used to key the Flink stream.")
    event_time_field: str = Field(description="Field representing event time.")
    input_event_name: str = Field(description="Name of the input event model.")
    output_event_name: str = Field(description="Name of the output event model.")
    rule_type: str = Field(
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
        """Normalize and validate the generated job name."""
        normalized = cls.normalize_job_name(value)
        if not normalized:
            raise ValueError(
                "job_name must contain at least one letter or number."
            )
        if not FILESYSTEM_SAFE_NAME_PATTERN.fullmatch(normalized):
            raise ValueError(
                "job_name must be filesystem-safe and may contain only letters, numbers, dots, underscores, and hyphens."
            )
        return normalized

    @field_validator("source_topic", "sink_topic")
    @classmethod
    def validate_topic_name(cls, value: str, info: ValidationInfo) -> str:
        """Normalize and validate Kafka topic names."""
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{info.field_name} must not be empty.")
        return normalized

    @field_validator("key_by", "event_time_field")
    @classmethod
    def validate_identifier(cls, value: str, info: ValidationInfo) -> str:
        """Normalize and validate identifier-like field names."""
        normalized = cls.normalize_identifier(value)
        if not IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError(
                f"{info.field_name} must be a valid identifier using letters, numbers, and underscores only, and it must not start with a number."
            )
        return normalized

    @field_validator("output_event_name")
    @classmethod
    def validate_output_event_name(cls, value: str) -> str:
        """Normalize and validate the output event class name."""
        normalized = cls.normalize_class_name(value)
        if not normalized:
            raise ValueError("output_event_name must not be empty.")
        return normalized

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, value: str) -> str:
        """Ensure only supported rule types are accepted."""
        if value not in cls.supported_rule_types:
            supported_values = ", ".join(sorted(cls.supported_rule_types))
            raise ValueError(f"rule_type must be one of: {supported_values}.")
        return value

    @field_validator(
        "input_event_name",
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

    @classmethod
    def demo_windowed_aggregation(cls) -> "FlinkJobSpec":
        """Create a demo spec for the windowed aggregation template."""
        return cls(
            job_name="sensor-count-job",
            source_topic="sensor-events",
            sink_topic="sensor-counts",
            key_by="device_id",
            event_time_field="event_time",
            input_event_name="InputEvent",
            output_event_name="WindowedCount",
            rule_type=ALLOWED_AGGREGATION_TYPE,
            rule_condition="count events by device_id in 5 minute windows",
            time_window_minutes=5,
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

    @staticmethod
    def normalize_job_name(value: str) -> str:
        """Normalize a job name into a lowercase filesystem-safe identifier."""
        normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip().lower())
        normalized = re.sub(r"-{2,}", "-", normalized)
        return normalized.strip("-.")

    @staticmethod
    def normalize_identifier(value: str) -> str:
        """Normalize free text into a simple underscore-based identifier."""
        normalized = re.sub(r"[^A-Za-z0-9_]+", "_", value.strip())
        normalized = re.sub(r"_+", "_", normalized)
        return normalized.strip("_")

    @staticmethod
    def normalize_class_name(value: str) -> str:
        """Normalize free text into a Java-style PascalCase class name."""
        parts = re.findall(r"[A-Za-z0-9]+", value.strip())
        return "".join(part[:1].upper() + part[1:] for part in parts)
