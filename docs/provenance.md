# Interpretation Provenance

v0.7 expands the report from basic pipeline status into a compact interpretation-provenance record.

The goal is to make it clear how the repository arrived at a validated spec, especially when provider-backed extraction, fallback, ambiguity, or defaults were involved.

## Core Fields

The `extraction_outcome` block in `generation_report.json` records:

- `selected_mode`
- `fallback_policy`
- `actual_path`
- `fallback_occurred`
- `fallback_reason`
- `provider_status`
- `provider_quality`
- `provider_quality_summary`
- `provider_quality_findings`
- `interpretation_risk`
- `ambiguity_status`
- `ambiguity_policy`
- `ambiguity_policy_result`
- `ambiguity_findings`
- `defaults_injected`
- `warnings`
- `errors`

The top-level report also includes `request_category` so downstream tooling can distinguish invalid, ambiguous, and unsupported failures.

## How To Read It

Some common patterns:

- straightforward deterministic run:
  `actual_path = ["deterministic"]`, `fallback_occurred = false`, `ambiguity_status = "clear"`
- provider-backed success:
  `actual_path = ["provider"]`, `provider_quality = "acceptable"`
- provider fallback:
  `actual_path = ["provider", "deterministic"]`, `fallback_occurred = true`, `fallback_reason` explains why
- low-risk ambiguity continued with defaults:
  `ambiguity_policy = "minor_defaults"`, `ambiguity_policy_result = "used_safe_defaults"`, `defaults_injected` is non-empty

`interpretation_risk` is intentionally coarse. It is currently either low or elevated, depending on whether fallback, provider ambiguity, or safe defaults were involved.

## Failure Reports

If a run stops before generation completes, the repository can still produce a failure-shaped report model.

That keeps the same provenance structure while setting:

- `pipeline_status = "failed_before_generation"`
- `failure_stage`
- `failure_reason`

This is useful for tooling that needs one report contract for both success and pre-generation failure cases.

## Design Intent

These fields are not telemetry and not confidence theater.

They are there to answer concrete questions:

- Which extractor path actually produced the spec?
- Did the provider path degrade to deterministic fallback?
- Was the request clear, ambiguous, invalid, or unsupported?
- Were any values injected by policy?
- Should this run be treated as straightforward or higher-risk by downstream tooling?
