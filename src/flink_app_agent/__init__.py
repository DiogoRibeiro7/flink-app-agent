"""Top-level package for flink-app-agent."""

from .spec import (
    FlinkJobSpec,
    JOB_FAMILY_KEYED_RULE,
    JOB_FAMILY_WINDOWED_AGGREGATION,
)

__all__ = [
    "FlinkJobSpec",
    "JOB_FAMILY_KEYED_RULE",
    "JOB_FAMILY_WINDOWED_AGGREGATION",
]
