# Architecture

`flink-app-agent` is organized as a short local pipeline with explicit multi-family support.

## Main Flow

The default CLI path in `src/flink_app_agent/main.py` performs:

1. request parsing
2. spec validation
3. template resolution
4. template rendering
5. deterministic repair loop
6. structural review
7. optional compile-only verification (with `--verify`)
8. generation report writing
9. terminal summary output

The same entry point also supports two inspection-only modes:

- `--print-spec-only`
- `--print-template-info`

Those modes still parse and validate the request, but stop before file generation.

## Extraction Boundary

`src/flink_app_agent/llm.py` already separates the extraction pipeline into small interfaces:

- `PromptRepository`
- `RequestPreprocessor`
- `SpecPayloadExtractor`
- `SpecValidator`
- `SpecExtractor`

The active implementation is deterministic:

- `FilePromptRepository`
- `SimpleRequestPreprocessor`
- `DeterministicSpecPayloadExtractor`
- `PydanticSpecValidator`

`OpenAISpecPayloadExtractor` exists only as a skeleton. It is not active and does not call any provider.

## Generation Boundary

Generation is split across a few focused modules:

- `template_registry.py`
  Resolves template metadata for a validated spec by matching both `job_family` and `rule_type`.
- `generator.py`
  Copies the selected template and renders placeholders in safe text files.
- `generation_context.py`
  Carries the small amount of state shared across generation, review, and report writing.

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

## Verification Boundary

After review:

- `verification.py` optionally runs `mvn compile` on the generated project
- Verification is opt-in via `--verify` and requires Maven on PATH
- If Maven is not available, verification is skipped gracefully
- The result is captured in the generation report
