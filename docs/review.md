# Review And Report

The repository includes a deterministic post-generation review and a machine-readable report artifact.

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

During the normal CLI flow, the repository also allows a very small repair step for safe cases only. Today that repair step removes trailing placeholder-only lines from generated text files when the intent is unambiguous.

`ReviewResult` records:

- `passed_checks`
- `failed_checks`
- `warnings`
- `repairs`
- `overall_status`

## Generation Report

`src/flink_app_agent/report.py` writes `generation_report.json` into the generated project directory.

The report captures:

- original request text
- job family
- parsed spec summary
- selected template identifier
- output directory
- generated file count
- generated file list
- structural check summary
- warnings

## Why Both Exist

The terminal output is aimed at a developer running the CLI directly.

The JSON report is aimed at:

- quick inspection after a run
- scripting around generated projects
- preserving the exact result of one generation pass without scraping terminal output

Neither the review nor the report proves that the generated Flink project is correct at runtime. They only summarize deterministic structural checks on local output.
