"""Fixture-driven end-to-end tests for the narrow local generation flow."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.config import ExtractorConfig
from flink_app_agent.generator import build_main_class_name
from flink_app_agent.constants import ProviderExtractionError
from flink_app_agent.llm import SpecParsingError
from flink_app_agent.main import build_generation_context, generate_project, review_project, write_report
from flink_app_agent.report import REPORT_FILENAME
from flink_app_agent.request_taxonomy import UnsupportedRequestError


FIXTURES_PATH = Path(__file__).resolve().parent / "fixtures" / "end_to_end_requests.json"
PROVIDER_FIXTURES_PATH = Path(__file__).resolve().parent / "fixtures" / "provider_end_to_end_requests.json"


def _load_fixture_pack() -> dict[str, list[dict[str, object]]]:
    """Load the end-to-end request fixtures from disk."""
    return json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))


FIXTURE_PACK = _load_fixture_pack()
VALID_REQUESTS = FIXTURE_PACK["valid_requests"]
INVALID_REQUESTS = FIXTURE_PACK["invalid_requests"]


def _load_provider_fixture_pack() -> dict[str, list[dict[str, object]]]:
    """Load the provider-backed end-to-end request fixtures from disk."""
    return json.loads(PROVIDER_FIXTURES_PATH.read_text(encoding="utf-8"))


PROVIDER_FIXTURE_PACK = _load_provider_fixture_pack()
PROVIDER_SUCCESS_REQUESTS = PROVIDER_FIXTURE_PACK["successful_requests"]
PROVIDER_FAILURE_REQUESTS = PROVIDER_FIXTURE_PACK["failing_requests"]


def _provider_windowed_aggregation_success(_: str, __: str) -> str:
    """Return a stable valid provider payload for the aggregation template."""
    return json.dumps(
        {
            "job_family": "windowed_aggregation",
            "job_name": "sensor-events-count-job",
            "source_topic": "sensor-events",
            "sink_topic": "aggregated-events",
            "key_by": "device_id",
            "event_time_field": "event_time",
            "input_event_name": "InputEvent",
            "output_event_name": "SensorEventsCount",
            "rule_type": "count_by_key_window",
            "rule_condition": "count events per device",
            "time_window_minutes": 5,
        }
    )


def _provider_invalid_json(_: str, __: str) -> str:
    """Return invalid JSON to exercise provider failure handling deterministically."""
    return "not json"


PROVIDER_CASES = {
    "windowed_aggregation_success": _provider_windowed_aggregation_success,
    "invalid_json": _provider_invalid_json,
}


def _build_extractor_config(fixture: dict[str, object]) -> ExtractorConfig:
    """Create an extractor config from one provider E2E fixture."""
    provider_case = fixture.get("provider_case")
    call_provider = None if provider_case is None else PROVIDER_CASES[str(provider_case)]
    return ExtractorConfig(
        mode=str(fixture["extractor"]),
        fallback=str(fixture["fallback"]),
        call_provider=call_provider,
    )


def _run_generation_pipeline(
    request: str,
    output_dir: Path,
    extractor_config: ExtractorConfig | None = None,
):
    """Run the same generation/review/report flow used by the CLI."""
    context = build_generation_context(
        request,
        output_dir,
        extractor_config=extractor_config,
    )
    context.generated_files = generate_project(context)
    context.review_result = review_project(context)
    context.report_path = write_report(context)
    context.review_result = review_project(context)
    context.report_path = write_report(context)
    return context


@pytest.mark.parametrize("fixture", VALID_REQUESTS, ids=[item["name"] for item in VALID_REQUESTS])
def test_valid_request_fixtures_cover_full_local_generation_flow(
    tmp_path: Path,
    fixture: dict[str, object],
) -> None:
    """Valid request fixtures should survive extraction, generation, review, and reporting."""
    request = str(fixture["request"])
    expected = fixture["expected"]
    output_dir = tmp_path / str(fixture["name"])

    context = build_generation_context(request, output_dir)
    context.generated_files = generate_project(context)
    context.review_result = review_project(context)
    context.report_path = write_report(context)
    context.review_result = review_project(context)
    context.report_path = write_report(context)

    readme_path = output_dir / "README.md"
    job_path = (
        output_dir
        / "src"
        / "main"
        / "java"
        / "com"
        / "example"
        / f"{build_main_class_name(context.spec.job_name)}.java"
    )
    report_path = output_dir / REPORT_FILENAME

    assert context.spec.job_family == expected["job_family"]
    assert context.spec.source_topic == expected["source_topic"]
    assert context.spec.sink_topic == expected["sink_topic"]
    assert context.spec.output_event_name == expected["output_event_name"]
    assert context.template.template_id == expected["template_id"]
    assert context.review_result is not None
    assert context.review_result.success is True
    assert report_path.exists()
    assert readme_path.exists()
    assert job_path.exists()


@pytest.mark.parametrize("fixture", INVALID_REQUESTS, ids=[item["name"] for item in INVALID_REQUESTS])
def test_invalid_request_fixtures_fail_clearly(fixture: dict[str, object], tmp_path: Path) -> None:
    """Invalid request fixtures should fail during extraction or validation with a clear error."""
    request = str(fixture["request"])
    expected_error = str(fixture["expected_error"])
    output_dir = tmp_path / str(fixture["name"])

    with pytest.raises((SpecParsingError, UnsupportedRequestError, ValueError), match=expected_error):
        build_generation_context(request, output_dir)


@pytest.mark.parametrize(
    "fixture",
    PROVIDER_SUCCESS_REQUESTS,
    ids=[item["name"] for item in PROVIDER_SUCCESS_REQUESTS],
)
def test_provider_backed_request_fixtures_cover_full_generation_flow(
    tmp_path: Path,
    fixture: dict[str, object],
) -> None:
    """Provider-backed E2E fixtures should cover success and fallback generation paths."""
    request = str(fixture["request"])
    expected = fixture["expected"]
    output_dir = tmp_path / str(fixture["name"])

    context = _run_generation_pipeline(
        request=request,
        output_dir=output_dir,
        extractor_config=_build_extractor_config(fixture),
    )

    readme_path = output_dir / "README.md"
    job_path = (
        output_dir
        / "src"
        / "main"
        / "java"
        / "com"
        / "example"
        / f"{build_main_class_name(context.spec.job_name)}.java"
    )
    report_path = output_dir / REPORT_FILENAME
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert context.spec.job_family == expected["job_family"]
    assert context.spec.source_topic == expected["source_topic"]
    assert context.spec.sink_topic == expected["sink_topic"]
    assert context.spec.output_event_name == expected["output_event_name"]
    assert context.template.template_id == expected["template_id"]
    assert context.review_result is not None
    assert context.review_result.success is True
    assert report_path.exists()
    assert readme_path.exists()
    assert job_path.exists()
    assert report_payload["pipeline_status"] == "passed"
    assert report_payload["selected_template"] == expected["template_id"]
    assert report_payload["parsed_spec_summary"]["source_topic"] == expected["source_topic"]
    assert report_payload["extraction_outcome"] == expected["extraction_outcome"]


@pytest.mark.parametrize(
    "fixture",
    PROVIDER_FAILURE_REQUESTS,
    ids=[item["name"] for item in PROVIDER_FAILURE_REQUESTS],
)
def test_provider_backed_request_fixtures_fail_without_misleading_report(
    fixture: dict[str, object],
    tmp_path: Path,
) -> None:
    """Provider-backed failure fixtures should stop clearly before report generation."""
    request = str(fixture["request"])
    expected_error = str(fixture["expected_error"])
    output_dir = tmp_path / str(fixture["name"])

    with pytest.raises((ProviderExtractionError, SpecParsingError, UnsupportedRequestError, ValueError), match=expected_error):
        _run_generation_pipeline(
            request=request,
            output_dir=output_dir,
            extractor_config=_build_extractor_config(fixture),
        )

    assert not (output_dir / REPORT_FILENAME).exists()
