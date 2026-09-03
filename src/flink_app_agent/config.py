"""Small configuration layer for extraction mode selection.

Resolution order for extractor mode:
1. Explicit ``extractor_type`` argument (from CLI ``--extractor`` flag)
2. ``FLINK_AGENT_EXTRACTOR`` environment variable
3. Default: ``"deterministic"``

When the resolved mode is ``"provider"``, the provider callable is loaded
from the ``FLINK_AGENT_PROVIDER_ENTRY_POINT`` environment variable using
the ``module.path:function_name`` format.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any

from .llm import ProviderCallable
from .request_taxonomy import REQUEST_CATEGORY_SUPPORTED

EXTRACTOR_ENV_VAR = "FLINK_AGENT_EXTRACTOR"
PROVIDER_ENTRY_POINT_ENV_VAR = "FLINK_AGENT_PROVIDER_ENTRY_POINT"
FALLBACK_ENV_VAR = "FLINK_AGENT_FALLBACK"
AMBIGUITY_POLICY_ENV_VAR = "FLINK_AGENT_AMBIGUITY_POLICY"

VALID_EXTRACTOR_MODES = ("deterministic", "provider")
VALID_FALLBACK_POLICIES = ("fail", "deterministic")
DEFAULT_EXTRACTOR_MODE = "deterministic"
DEFAULT_FALLBACK_POLICY = "fail"


class ConfigurationError(ValueError):
    """Raised when extraction mode configuration is invalid or incomplete."""


@dataclass(frozen=True)
class ExtractorConfig:
    """Resolved extraction configuration."""

    mode: str
    fallback: str = DEFAULT_FALLBACK_POLICY
    ambiguity_policy: str = "fail"
    call_provider: ProviderCallable | None = None


@dataclass(frozen=True)
class ExtractionOutcome:
    """Record of which extractor produced the spec and whether fallback occurred."""

    requested_mode: str
    fallback_policy: str
    extractor_used: str
    request_category: str = REQUEST_CATEGORY_SUPPORTED
    actual_path: tuple[str, ...] = ()
    fallback_triggered: bool = False
    fallback_reason: str | None = None
    provider_error: str | None = None
    provider_status: str | None = None
    provider_quality: str | None = None
    provider_quality_summary: str | None = None
    provider_quality_codes: tuple[str, ...] = ()
    provider_quality_findings: tuple["ProviderQualityFindingRecord", ...] = ()
    ambiguity_status: str = "clear"
    ambiguity_policy: str = "fail"
    ambiguity_policy_result: str = "clear"
    ambiguity_issue_codes: tuple[str, ...] = ()
    ambiguity_warning: str | None = None
    injected_defaults: tuple[str, ...] = ()
    ambiguity_findings: tuple["AmbiguityFinding", ...] = ()
    default_injections: tuple["DefaultInjection", ...] = ()
    interpretation_risk: str = "low"
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class AmbiguityFinding:
    """Compact structured summary of one ambiguity finding."""

    code: str
    severity: str
    message: str
    fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable ambiguity finding."""
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "fields": list(self.fields),
        }


@dataclass(frozen=True)
class DefaultInjection:
    """Structured record of one deterministic default applied by policy."""

    field: str
    value: Any
    reason: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable default-injection record."""
        return {
            "field": self.field,
            "value": self.value,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ProviderQualityFindingRecord:
    """Structured record of one provider-quality finding."""

    code: str
    message: str
    fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable provider-quality finding."""
        return {
            "code": self.code,
            "message": self.message,
            "fields": list(self.fields),
        }


def resolve_extractor_config(
    cli_extractor: str | None = None,
    cli_fallback: str | None = None,
    cli_ambiguity_policy: str | None = None,
) -> ExtractorConfig:
    """Resolve the effective extractor configuration.

    Args:
        cli_extractor: Explicit mode from the CLI ``--extractor`` flag.
            Takes precedence over environment variables when not None.
        cli_fallback: Explicit fallback policy from the CLI ``--fallback`` flag.
            Takes precedence over environment variables when not None.
        cli_ambiguity_policy: Explicit ambiguity policy from the CLI
            ``--ambiguity-policy`` flag. Takes precedence over environment
            variables when not None.

    Returns:
        A resolved ``ExtractorConfig`` ready for use.

    Raises:
        ConfigurationError: If the mode is invalid or provider configuration
            is missing or broken.
    """
    mode = _resolve_mode(cli_extractor)
    fallback = _resolve_fallback(cli_fallback)
    ambiguity_policy = _resolve_ambiguity_policy(cli_ambiguity_policy)
    if mode == "deterministic":
        return ExtractorConfig(
            mode=mode,
            fallback=fallback,
            ambiguity_policy=ambiguity_policy,
        )
    return ExtractorConfig(
        mode=mode,
        fallback=fallback,
        ambiguity_policy=ambiguity_policy,
        call_provider=_load_provider_callable(),
    )


def _resolve_mode(cli_extractor: str | None) -> str:
    """Determine the effective extractor mode."""
    if cli_extractor is not None:
        mode = cli_extractor
    else:
        mode = os.environ.get(EXTRACTOR_ENV_VAR, DEFAULT_EXTRACTOR_MODE)

    if mode not in VALID_EXTRACTOR_MODES:
        raise ConfigurationError(
            f"Invalid extractor mode '{mode}'. "
            f"Must be one of: {', '.join(VALID_EXTRACTOR_MODES)}."
        )
    return mode


def _resolve_fallback(cli_fallback: str | None) -> str:
    """Determine the effective fallback policy."""
    if cli_fallback is not None:
        policy = cli_fallback
    else:
        policy = os.environ.get(FALLBACK_ENV_VAR, DEFAULT_FALLBACK_POLICY)

    if policy not in VALID_FALLBACK_POLICIES:
        raise ConfigurationError(
            f"Invalid fallback policy '{policy}'. "
            f"Must be one of: {', '.join(VALID_FALLBACK_POLICIES)}."
        )
    return policy


def _resolve_ambiguity_policy(cli_ambiguity_policy: str | None) -> str:
    """Determine the effective ambiguity policy."""
    from .ambiguity_policy import (
        DEFAULT_AMBIGUITY_POLICY,
        VALID_AMBIGUITY_POLICIES,
    )

    if cli_ambiguity_policy is not None:
        policy = cli_ambiguity_policy
    else:
        policy = os.environ.get(
            AMBIGUITY_POLICY_ENV_VAR,
            DEFAULT_AMBIGUITY_POLICY,
        )

    if policy not in VALID_AMBIGUITY_POLICIES:
        raise ConfigurationError(
            f"Invalid ambiguity policy '{policy}'. "
            f"Must be one of: {', '.join(VALID_AMBIGUITY_POLICIES)}."
        )
    return policy


def _load_provider_callable() -> ProviderCallable:
    """Load the provider callable from the entry point environment variable."""
    entry_point = os.environ.get(PROVIDER_ENTRY_POINT_ENV_VAR)
    if not entry_point:
        raise ConfigurationError(
            f"Provider extractor requires {PROVIDER_ENTRY_POINT_ENV_VAR} "
            f"to be set (format: 'module.path:function_name')."
        )

    if ":" not in entry_point:
        raise ConfigurationError(
            f"Invalid entry point format '{entry_point}'. "
            f"Expected 'module.path:function_name'."
        )

    module_path, _, function_name = entry_point.partition(":")
    if not module_path or not function_name:
        raise ConfigurationError(
            f"Invalid entry point format '{entry_point}'. "
            f"Both module path and function name are required."
        )

    try:
        module = importlib.import_module(module_path)
    except Exception as exc:
        raise ConfigurationError(
            f"Cannot import provider module '{module_path}' for entry point "
            f"'{entry_point}': {exc}"
        ) from exc

    fn = getattr(module, function_name, None)
    if fn is None:
        raise ConfigurationError(
            f"Provider module '{module_path}' has no attribute '{function_name}'."
        )
    if not callable(fn):
        raise ConfigurationError(
            f"'{entry_point}' is not callable."
        )
    return fn
