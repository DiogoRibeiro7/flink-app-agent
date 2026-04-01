# Repair, Review, Verification, And Report

The repository includes a deterministic repair loop, structural review, optional compile verification, and a machine-readable report artifact.

## Repair

`src/flink_app_agent/repair.py` defines:

- `RepairResult`
- `DeterministicRepairer`

The repair loop runs before the structural review. It applies a small set of safe, auditable fixes:

- trailing placeholder-only line removal
- missing final newline addition
- trailing whitespace cleanup

The loop runs at most 3 passes and stops early when no new repairs are found. All repairs are recorded and reported.

## Review

`src/flink_app_agent/review.py` defines:

- `ReviewResult`
- `StructuralReviewer`

The review step is file-based only. It does not compile Java, run Maven, or execute Flink.

Current checks include:

- output directory exists
- generated README exists
- generated main Flink job file exists
- generation report exists
- no unresolved placeholders remain in generated text files
- configured source topic appears in key generated files
- configured sink topic appears in key generated files
- family-specific function file exists (e.g. `RuleProcessFunction.java` for keyed rule, `WindowedCountProcessWindowFunction.java` for windowed aggregation)
- job family value appears in generated README

It may also record a warning if the generated Java test scaffold is missing.

`ReviewResult` records:

- `passed_checks`
- `failed_checks`
- `warnings`
- `overall_status`

## Compile Verification

`src/flink_app_agent/verification.py` defines:

- `VerificationResult`
- `CompileVerifier`

Compile verification is opt-in via the `--verify` CLI flag. It runs `mvn compile` on the generated project and captures the result. It does not run tests, package, or deploy.

If Maven is not available on PATH, verification is skipped gracefully with a clear reason.

## Generation Report

`src/flink_app_agent/report.py` writes `generation_report.json` into the generated project directory.

The report captures:

- original request text
- request category
- job family
- parsed spec summary
- selected template identifier
- output directory
- generated file count
- generated file list
- extraction provenance
- overall pipeline status
- repair pass summary (repairs applied, passes run)
- structural check summary
- compile verification summary (if attempted)
- warnings

The extraction provenance block is intentionally compact. It records:

- selected extraction mode
- fallback policy
- actual extraction path used
- whether fallback occurred
- fallback reason when relevant
- provider availability status when relevant
- provider quality details when relevant
- interpretation risk
- ambiguity status
- ambiguity policy
- ambiguity policy result
- ambiguity findings
- default injections
- extraction warning summary
- extraction error summary

## Pipeline Status

The `pipeline_status` field in the report summarizes the overall outcome:

- `passed` — review passed, verification passed (or not attempted)
- `passed_with_warnings` — review passed with warnings, no verification
- `failed` — review failed
- `review_passed_compile_failed` — review passed but compile verification failed

## Why All Four Exist

The repair loop fixes obvious safe issues before the review checks structure.

The review checks that the generated project is structurally complete.

The compile verification optionally proves the generated Java compiles.

The JSON report preserves the full pipeline state for scripting and automation.

In v0.7 that includes enough extraction provenance to distinguish:

- a normal deterministic run
- a successful provider-backed run
- a provider-backed run that degraded to deterministic fallback
- a run that continued under a narrow safe-default policy
- a run that failed before generation because the request was invalid, ambiguous, or unsupported
