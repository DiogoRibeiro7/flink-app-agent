"""Fixture-driven end-to-end tests for the narrow local generation flow."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_app_agent.generator import build_main_class_name
from flink_app_agent.llm import SpecParsingError
from flink_app_agent.main import build_generation_context, generate_project, review_project, write_report
from flink_app_agent.report import REPORT_FILENAME


FIXTURES_PATH = Path(__file__).resolve().parent / "fixtures" / "end_to_end_requests.json"


def _load_fixture_pack() -> dict[str, list[dict[str, object]]]:
    """Load the end-to-end request fixtures from disk."""
    return json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))


FIXTURE_PACK = _load_fixture_pack()
VALID_REQUESTS = FIXTURE_PACK["valid_requests"]
INVALID_REQUESTS = FIXTURE_PACK["invalid_requests"]


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

    with pytest.raises((SpecParsingError, ValueError), match=expected_error):
        build_generation_context(request, output_dir)
