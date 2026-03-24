"""Tests for the Flink job spec model and stub extraction."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.llm import StubLLMClient
from flink_app_agent.spec import FlinkJobSpec


def test_stub_llm_extracts_basic_fields() -> None:
    """The stub extractor should populate the main fields from plain English."""
    client = StubLLMClient()
    payload = client.extract_spec(
        prompt="ignored",
        request=(
            "Create a Flink job named fraud detector that reads from topic payments, "
            "writes to topic alerts, keys by account_id, uses consumer group fraud-group, "
            "and emits an alert when amount > 5000."
        ),
    )

    spec = FlinkJobSpec.from_llm_payload(payload)

    assert spec.job_name == "fraud detector"
    assert spec.input_topic == "payments"
    assert spec.output_topic == "alerts"
    assert spec.key_field == "account_id"
    assert spec.consumer_group == "fraud-group"
    assert spec.rule_expression == "amount > 5000"
    assert spec.job_class_name == "FraudDetector"


def test_spec_rejects_invalid_template() -> None:
    """Only the single supported template should validate."""
    with pytest.raises(ValueError):
        FlinkJobSpec.from_llm_payload(
            {
                "template_id": "another_template",
                "job_name": "job",
                "job_class_name": "Job",
                "package_name": "com.example",
                "input_topic": "in",
                "output_topic": "out",
                "consumer_group": "group",
                "key_field": "userId",
                "rule_expression": "x > 0",
                "bootstrap_servers": "localhost:9092",
                "input_schema_class": "InputEvent",
                "output_schema_class": "OutputEvent",
            }
        )
