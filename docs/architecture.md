# Architecture

`flink-app-agent` is organized as a short local pipeline with explicit multi-family support. The system remains local-first: deterministic extraction is the default, while provider-backed extraction is optional and still forced through the same internal contracts.

## Main Flow

The default CLI path in `src/flink_app_agent/main.py` performs:

1. request parsing
2. extraction path selection (`deterministic` or `provider`)
3. payload normalization and spec validation
4. template resolution
5. template rendering
6. deterministic repair loop
7. structural review
8. optional compile-only verification (with `--verify`)
9. generation report writing
10. terminal summary output

The same entry point also supports two inspection-only modes:

- `--print-spec-only`
- `--print-template-info`

Those modes still parse and validate the request, but stop before file generation.

The CLI also exposes:

- `--extractor deterministic|provider`
- `--fallback fail|deterministic`

The summary output reports both the requested extractor mode and the actual extraction path used.

## Extraction Boundary

`src/flink_app_agent/llm.py` already separates the extraction pipeline into small interfaces:

- `PromptRepository`
- `RequestPreprocessor`
- `SpecPayloadExtractor`
- `SpecValidator`
- `SpecExtractor`

The active implementations are:

- `FilePromptRepository`
- `SimpleRequestPreprocessor`
- `DeterministicSpecPayloadExtractor`
- `ProviderSpecPayloadExtractor`
- `PydanticSpecValidator`

Deterministic extraction remains the default path.

Provider-backed extraction is available behind an injectable `ProviderCallable`. That path is still constrained:

- the raw provider response must parse as JSON
- the JSON is normalized through `provider_normalizer.py`
- the normalized payload must validate as a `FlinkJobSpec`
- on provider failure, the run either fails explicitly or falls back to deterministic extraction when configured to do so

No provider SDK is wired into the repository by default. The codebase only defines the boundary that a local integration can plug into.

## Generation Boundary

Generation is split across a few focused modules:

- `template_registry.py`
  Resolves template metadata for a validated spec by matching both `job_family` and `rule_type`.
- `generator.py`
  Copies the selected template and renders placeholders in safe text files.
- `generation_context.py`
  Carries the small amount of state shared across generation, review, reporting, and extraction provenance.

The current registry contains two active template definitions:

- `flink_kafka_rule_job`
- `flink_windowed_aggregation_job`

## Repair Boundary

After generation:

- `repair.py` runs a deterministic multi-pass repair loop for safe fixups
- Repair strategies: trailing placeholder removal, missing final newline, trailing whitespace
- The loop is idempotent and stops early when no new repairs are found

## Review And Reporting Boundary

After repair:

- `review.py` performs deterministic file-based structural checks
- `report.py` serializes the full pipeline state into `generation_report.json`

The review step is intentionally lightweight. It checks project structure and obvious rendering problems, but it does not compile Java or run Flink.

The report includes extraction provenance as well as pipeline status. In practice that means the artifact can show:

- selected extraction mode
- actual extraction path used
- whether fallback occurred
- provider availability status when relevant
- extraction warning and error summaries

## Verification Boundary

After review:

- `verification.py` optionally runs `mvn compile` on the generated project
- Verification is opt-in via `--verify` and requires Maven on PATH
- If Maven is not available, verification is skipped gracefully
- The result is captured in the generation report
