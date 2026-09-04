"""Strict internal specification model for the multi-family local generation flow."""

from __future__ import annotations

import re
from typing import Any, ClassVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)


FILESYSTEM_SAFE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
JAVA_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")
ALLOWED_RULE_TYPE = "two_events_within_window"
WINDOWED_AGGREGATION_RULE_TYPE = "count_by_key_window"
SESSION_WINDOW_AGGREGATION_RULE_TYPE = "count_by_key_session_window"

JOB_FAMILY_KEYED_RULE = "keyed_temporal_rule"
JOB_FAMILY_WINDOWED_AGGREGATION = "windowed_aggregation"

JAVA_LONG_MAX = 9_223_372_036_854_775_807
MAX_TIME_WINDOW_MINUTES = JAVA_LONG_MAX // 60_000


class FlinkJobSpec(BaseModel):
    """Validated internal specification for the current multi-family Flink job flow."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    supported_rule_types: ClassVar[set[str]] = {
        ALLOWED_RULE_TYPE,
        WINDOWED_AGGREGATION_RULE_TYPE,
        SESSION_WINDOW_AGGREGATION_RULE_TYPE,
    }
    supported_job_families: ClassVar[set[str]] = {
        JOB_FAMILY_KEYED_RULE,
        JOB_FAMILY_WINDOWED_AGGREGATION,
    }

    job_family: str = Field(description="High-level job family for template selection.")
    job_name: str = Field(description="Filesystem-safe name for the generated job.")
    source_topic: str = Field(description="Kafka source topic name.")
    sink_topic: str = Field(description="Kafka sink topic name.")
    key_by: str = Field(description="Stream key field name.")
    event_time_field: str = Field(description="Event-time field name.")
    input_event_name: str = Field(description="Input event class name.")
    output_event_name: str = Field(description="Output event class name.")
    rule_type: str = Field(description="Supported rule type for the current template path.")
    rule_condition: str = Field(description="Human-readable rule condition.")
    time_window_minutes: int = Field(
        gt=0,
        le=MAX_TIME_WINDOW_MINUTES,
        description="Positive time window length that fits Java milliseconds.",
    )

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

    @field_validator("source_topic", "sink_topic", "rule_condition")
    @classmethod
    def validate_not_empty(cls, value: str, info: ValidationInfo) -> str:
        """Reject blank required string fields."""
        if not value:
            raise ValueError(f"{info.field_name} must not be empty.")
        return value

    @field_validator("key_by", "event_time_field")
    @classmethod
    def validate_identifier_fields(cls, value: str, info: ValidationInfo) -> str:
        """Normalize and validate identifier-like fields."""
        normalized = cls.normalize_identifier(value)
        if not normalized:
            raise ValueError(f"{info.field_name} must not be empty.")
        if not IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError(
                f"{info.field_name} must be a valid identifier using letters, numbers, and underscores, and it must not start with a number."
            )
        return normalized

    @field_validator("input_event_name", "output_event_name")
    @classmethod
    def validate_class_name_fields(cls, value: str, info: ValidationInfo) -> str:
        """Normalize and validate Java-style class name fields."""
        normalized = cls.normalize_class_name(value)
        if not normalized:
            raise ValueError(f"{info.field_name} must not be empty.")
        if not JAVA_IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError(f"{info.field_name} must be a valid Java identifier.")
        return normalized

    @field_validator("job_family")
    @classmethod
    def validate_job_family(cls, value: str) -> str:
        """Allow only the currently supported job families."""
        if value not in cls.supported_job_families:
            supported = ", ".join(sorted(cls.supported_job_families))
            raise ValueError(f"job_family must be one of: {supported}.")
        return value

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, value: str) -> str:
        """Allow only the currently supported rule types."""
        if value not in cls.supported_rule_types:
            supported_rule_types = ", ".join(sorted(cls.supported_rule_types))
            raise ValueError(f"rule_type must be one of: {supported_rule_types}.")
        return value

    @model_validator(mode="after")
    def validate_distinct_event_class_names(self) -> "FlinkJobSpec":
        """Require input and output model classes to remain distinct after normalization."""
        if self.input_event_name == self.output_event_name:
            raise ValueError(
                "input_event_name and output_event_name must be different Java class names."
            )
        return self

    @classmethod
    def from_llm_payload(cls, payload: dict[str, Any]) -> "FlinkJobSpec":
        """Build a validated spec from a plain extracted payload."""
        return cls.model_validate(payload)

    @classmethod
    def demo(cls) -> "FlinkJobSpec":
        """Return a small demo spec for tests and examples."""
        return cls(
            job_family=JOB_FAMILY_KEYED_RULE,
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

    @classmethod
    def demo_windowed_aggregation(cls) -> "FlinkJobSpec":
        """Return a small tumbling-window aggregation demo spec for tests and examples."""
        return cls(
            job_family=JOB_FAMILY_WINDOWED_AGGREGATION,
            job_name="sensor-events-count-job",
            source_topic="sensor-events",
            sink_topic="aggregated-events",
            key_by="device_id",
            event_time_field="ts",
            input_event_name="InputEvent",
            output_event_name="SensorEventsCount",
            rule_type=WINDOWED_AGGREGATION_RULE_TYPE,
            rule_condition="count events by device_id within 5 minutes",
            time_window_minutes=5,
        )

    @classmethod
    def demo_session_window_aggregation(cls) -> "FlinkJobSpec":
        """Return a keyed session-window aggregation demo spec."""
        return cls(
            job_family=JOB_FAMILY_WINDOWED_AGGREGATION,
            job_name="sensor-events-session-count-job",
            source_topic="sensor-events",
            sink_topic="session-counts",
            key_by="device_id",
            event_time_field="ts",
            input_event_name="InputEvent",
            output_event_name="SensorEventsCount",
            rule_type=SESSION_WINDOW_AGGREGATION_RULE_TYPE,
            rule_condition="count events by device_id using a 5-minute session gap",
            time_window_minutes=5,
        )

    def to_template_dict(self) -> dict[str, str]:
        """Return a flat placeholder dictionary for template substitution."""
        return {
            "JOB_FAMILY": self.job_family,
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
