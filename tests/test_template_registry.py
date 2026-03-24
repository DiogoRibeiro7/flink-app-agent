"""Tests for the internal local template registry."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.spec import FlinkJobSpec
from flink_app_agent.template_registry import TemplateDefinition, TemplateRegistry, TemplateRegistryError


def test_template_registry_resolves_known_template_for_spec(tmp_path: Path) -> None:
    """A supported spec should resolve the registered Kafka rule template."""
    registry = TemplateRegistry.from_root(tmp_path)

    template = registry.resolve_for_spec(FlinkJobSpec.demo())

    assert template.template_id == "flink_kafka_rule_job"
    assert template.template_path == tmp_path / "flink_kafka_rule_job"
    assert template.description == "Kafka-to-Kafka keyed rule job with event-time processing."
    assert template.runtime == "Java / Apache Flink DataStream"


def test_template_registry_rejects_unknown_template_identifier(tmp_path: Path) -> None:
    """Unknown template identifiers should fail with a clear error."""
    registry = TemplateRegistry.from_root(tmp_path)

    with pytest.raises(TemplateRegistryError, match="Unknown template identifier"):
        registry.get("missing-template")


def test_template_registry_rejects_unsupported_rule_type(tmp_path: Path) -> None:
    """Specs with unsupported rule types should fail explicit resolution."""
    registry = TemplateRegistry(
        templates=(
            TemplateDefinition(
                template_id="flink_kafka_rule_job",
                template_path=tmp_path / "flink_kafka_rule_job",
                supported_rule_types=frozenset({"two_events_within_window"}),
                description="Kafka-to-Kafka keyed rule job with event-time processing.",
                runtime="Java / Apache Flink DataStream",
            ),
        )
    )
    unsupported_spec = FlinkJobSpec.model_construct(
        job_name="demo-job",
        source_topic="payments",
        sink_topic="alerts",
        key_by="account_id",
        event_time_field="event_time",
        input_event_name="InputEvent",
        output_event_name="AlertEvent",
        rule_type="unsupported_rule_type",
        rule_condition="demo",
        time_window_minutes=5,
    )

    with pytest.raises(TemplateRegistryError, match="supports rule_type"):
        registry.resolve_for_spec(unsupported_spec)
