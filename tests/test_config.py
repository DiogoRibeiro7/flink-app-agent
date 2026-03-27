"""Tests for the extraction mode configuration layer."""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.config import (
    EXTRACTOR_ENV_VAR,
    PROVIDER_ENTRY_POINT_ENV_VAR,
    ConfigurationError,
    resolve_extractor_config,
)


def test_default_mode_is_deterministic() -> None:
    """With no CLI flag and no env var, the mode should be deterministic."""
    config = resolve_extractor_config(cli_extractor=None)

    assert config.mode == "deterministic"
    assert config.call_provider is None


def test_cli_flag_selects_deterministic() -> None:
    """An explicit CLI flag of 'deterministic' should resolve to deterministic."""
    config = resolve_extractor_config(cli_extractor="deterministic")

    assert config.mode == "deterministic"
    assert config.call_provider is None


def test_env_var_selects_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    """The env var should be able to select deterministic mode."""
    monkeypatch.setenv(EXTRACTOR_ENV_VAR, "deterministic")

    config = resolve_extractor_config(cli_extractor=None)

    assert config.mode == "deterministic"


def test_cli_flag_overrides_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI flag should take precedence over the environment variable."""
    monkeypatch.setenv(EXTRACTOR_ENV_VAR, "provider")

    config = resolve_extractor_config(cli_extractor="deterministic")

    assert config.mode == "deterministic"


def test_invalid_mode_from_env_var_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unrecognized mode from the env var should fail clearly."""
    monkeypatch.setenv(EXTRACTOR_ENV_VAR, "magic")

    with pytest.raises(ConfigurationError, match="Invalid extractor mode"):
        resolve_extractor_config(cli_extractor=None)


def test_invalid_mode_from_cli_fails() -> None:
    """An unrecognized mode from the CLI should fail clearly."""
    with pytest.raises(ConfigurationError, match="Invalid extractor mode"):
        resolve_extractor_config(cli_extractor="magic")


def test_provider_mode_without_entry_point_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider mode without the entry point env var should fail clearly."""
    monkeypatch.delenv(PROVIDER_ENTRY_POINT_ENV_VAR, raising=False)

    with pytest.raises(ConfigurationError, match="FLINK_AGENT_PROVIDER_ENTRY_POINT"):
        resolve_extractor_config(cli_extractor="provider")


def test_provider_mode_with_bad_entry_point_format_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry point without a colon separator should fail clearly."""
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "just_a_module")

    with pytest.raises(ConfigurationError, match="Invalid entry point format"):
        resolve_extractor_config(cli_extractor="provider")


def test_provider_mode_with_missing_module_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry point referencing a nonexistent module should fail clearly."""
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "nonexistent_module_xyz:fn")

    with pytest.raises(ConfigurationError, match="Cannot import provider module"):
        resolve_extractor_config(cli_extractor="provider")


def test_provider_mode_with_missing_function_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry point referencing a nonexistent function should fail clearly."""
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "json:nonexistent_function_xyz")

    with pytest.raises(ConfigurationError, match="has no attribute"):
        resolve_extractor_config(cli_extractor="provider")


def test_provider_mode_with_non_callable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry point referencing a non-callable attribute should fail clearly."""
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "json:__version__")

    with pytest.raises(ConfigurationError, match="not callable"):
        resolve_extractor_config(cli_extractor="provider")


def test_provider_mode_with_valid_entry_point(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A valid entry point should resolve to a callable config."""
    provider_module = tmp_path / "my_provider.py"
    provider_module.write_text(
        textwrap.dedent("""\
            def call_provider(request: str, prompt: str) -> str:
                return '{"result": "ok"}'
        """),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "my_provider:call_provider")

    config = resolve_extractor_config(cli_extractor="provider")

    assert config.mode == "provider"
    assert config.call_provider is not None
    assert callable(config.call_provider)


def test_env_var_selects_provider_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The env var should be able to select provider mode when entry point is set."""
    provider_module = tmp_path / "env_provider.py"
    provider_module.write_text(
        textwrap.dedent("""\
            def call_provider(request: str, prompt: str) -> str:
                return '{"result": "ok"}'
        """),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(EXTRACTOR_ENV_VAR, "provider")
    monkeypatch.setenv(PROVIDER_ENTRY_POINT_ENV_VAR, "env_provider:call_provider")

    config = resolve_extractor_config(cli_extractor=None)

    assert config.mode == "provider"
    assert config.call_provider is not None
