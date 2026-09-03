"""Regression tests for CLI extractor configuration contracts."""

from __future__ import annotations

import pytest

from flink_app_agent import config
from flink_app_agent.config import ConfigurationError, resolve_extractor_config
from flink_app_agent.main import build_parser


def test_extractor_cli_default_does_not_mask_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Omitting --extractor must leave environment-mode resolution available."""
    args = build_parser().parse_args(["--request", "example", "--print-spec-only"])

    assert args.extractor is None

    monkeypatch.setenv(config.EXTRACTOR_ENV_VAR, "provider")
    monkeypatch.setenv(config.PROVIDER_ENTRY_POINT_ENV_VAR, "fake.provider:call")
    monkeypatch.setattr(config.importlib, "import_module", lambda _: type("Module", (), {"call": lambda *_: "{}"})())

    resolved = resolve_extractor_config(cli_extractor=args.extractor)

    assert resolved.mode == "provider"
    assert resolved.call_provider is not None


def test_provider_import_runtime_failure_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Import-time provider failures must surface as ConfigurationError."""
    monkeypatch.setenv(config.PROVIDER_ENTRY_POINT_ENV_VAR, "broken.provider:call")

    def fail_import(_: str) -> object:
        raise RuntimeError("provider initialization exploded")

    monkeypatch.setattr(config.importlib, "import_module", fail_import)

    with pytest.raises(ConfigurationError, match="broken.provider:call"):
        resolve_extractor_config(cli_extractor="provider")
