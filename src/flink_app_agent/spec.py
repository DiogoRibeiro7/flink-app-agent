"""Structured models for Flink job generation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class FlinkJobSpec(BaseModel):
    """Structured definition for the supported Flink job template."""

    template_id: str = Field(
        default="flink_kafka_rule_job",
        description="The only supported template identifier.",
    )
    job_name: str = Field(description="Human-readable job name.")
    job_class_name: str = Field(description="Generated Java class name for the Flink job.")
    package_name: str = Field(
        default="com.example",
        description="Java package for the generated source files.",
    )
    input_topic: str = Field(description="Kafka input topic.")
    output_topic: str = Field(description="Kafka output topic.")
    consumer_group: str = Field(description="Kafka consumer group id.")
    key_field: str = Field(description="Input field used to key the stream.")
    rule_expression: str = Field(description="Boolean-like rule description for event filtering.")
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers used for both source and sink.",
    )
    input_schema_class: str = Field(
        default="InputEvent",
        description="Java model class for the input event.",
    )
    output_schema_class: str = Field(
        default="OutputEvent",
        description="Java model class for the output event.",
    )

    @field_validator("template_id")
    @classmethod
    def validate_template_id(cls, value: str) -> str:
        """Ensure only the single supported template is accepted."""
        if value != "flink_kafka_rule_job":
            raise ValueError("Only the 'flink_kafka_rule_job' template is supported.")
        return value

    @field_validator(
        "job_name",
        "job_class_name",
        "package_name",
        "input_topic",
        "output_topic",
        "consumer_group",
        "key_field",
        "rule_expression",
        "bootstrap_servers",
    )
    @classmethod
    def validate_not_empty(cls, value: str) -> str:
        """Reject blank string values for required text fields."""
        if not value.strip():
            raise ValueError("Field must not be empty.")
        return value

    @classmethod
    def from_llm_payload(cls, payload: dict[str, Any]) -> "FlinkJobSpec":
        """Build a validated spec from the LLM payload."""
        return cls.model_validate(payload)
