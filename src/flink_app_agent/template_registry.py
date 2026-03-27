"""Small internal registry for local Flink project templates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .spec import (
    FlinkJobSpec,
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
)


class TemplateRegistryError(ValueError):
    """Raised when a template cannot be resolved for generation."""


@dataclass(frozen=True)
class TemplateDefinition:
    """Definition of one locally available generation template."""

    template_id: str
    template_path: Path
    job_family: str
    supported_rule_types: frozenset[str]
    description: str
    runtime: str


@dataclass(frozen=True)
class TemplateRegistry:
    """Registry for the small set of locally available templates."""

    templates: tuple[TemplateDefinition, ...]

    @classmethod
    def from_root(cls, templates_root: Path) -> "TemplateRegistry":
        """Build the current local template registry from the templates root."""
        return cls(
            templates=(
                TemplateDefinition(
                    template_id="flink_kafka_rule_job",
                    template_path=templates_root / "flink_kafka_rule_job",
                    job_family=JOB_FAMILY_KEYED_RULE,
                    supported_rule_types=frozenset({"two_events_within_window"}),
                    description="Kafka-to-Kafka keyed rule job with event-time processing.",
                    runtime="Java / Apache Flink DataStream",
                ),
                TemplateDefinition(
                    template_id="flink_windowed_aggregation_job",
                    template_path=templates_root / "flink_windowed_aggregation_job",
                    job_family=JOB_FAMILY_WINDOWED_AGGREGATION,
                    supported_rule_types=frozenset({"count_by_key_window"}),
                    description="Kafka-to-Kafka windowed aggregation job with event-time processing.",
                    runtime="Java / Apache Flink DataStream",
                ),
            )
        )

    def get(self, template_id: str) -> TemplateDefinition:
        """Return a template definition by identifier."""
        for template in self.templates:
            if template.template_id == template_id:
                return template
        raise TemplateRegistryError(f"Unknown template identifier: {template_id}")

    def resolve_for_spec(self, spec: FlinkJobSpec) -> TemplateDefinition:
        """Return the template definition that supports the given spec.

        Resolution matches on both job_family and rule_type. Both must agree
        with a registered template for resolution to succeed.
        """
        for template in self.templates:
            if (
                template.job_family == spec.job_family
                and spec.rule_type in template.supported_rule_types
            ):
                return template
        raise TemplateRegistryError(
            f"No registered template supports job_family "
            f"'{spec.job_family}' with rule_type '{spec.rule_type}'."
        )
