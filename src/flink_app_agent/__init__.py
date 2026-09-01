"""Top-level package for flink-app-agent."""

from .spec import (
    FlinkJobSpec,
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
    SESSION_WINDOW_AGGREGATION_RULE_TYPE,
)

__all__ = [
    "FlinkJobSpec",
    "JOB_FAMILY_KEYED_RULE",
    "JOB_FAMILY_WINDOWED_AGGREGATION",
    "SESSION_WINDOW_AGGREGATION_RULE_TYPE",
]
