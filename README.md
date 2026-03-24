# flink-app-agent

`flink-app-agent` is a small Python CLI that turns a narrow plain-English Flink job request into:

1. a validated internal specification
2. a generated Java Flink project from one local template
3. a deterministic structural review summary

## Project Purpose

The purpose of `flink-app-agent` is to provide a small, explicit foundation for Flink job scaffolding.

The repository is not trying to solve general code generation or open-ended natural-language planning. It focuses on a short local path from request text to a validated spec and a generated starter project.

## Why The Scope Is Still Intentionally Narrow

The scope remains narrow in `v0.2` so the moving parts stay easy to inspect and test:

- one CLI entry point
- one strict spec model
- one deterministic extractor
- one local template
- one local generator
- one small structural review step

This keeps the project understandable while improving the existing flow incrementally rather than adding new product surfaces.

## What Changed From v0.1 To v0.2

Compared with `v0.1`, `v0.2` adds:

- broader deterministic request parsing for a few more phrasing variants
- stricter spec normalization and validation
- safer template rendering with unresolved-placeholder detection
- clearer template documentation and Java flow structure
- a deterministic post-generation structural check
- a realistic local end-to-end test

The architecture is still the same basic shape: parse -> validate -> generate -> structurally check.

## Current Architecture Flow

The current flow is:

1. `main.py` reads `--request` and `--output`
2. `llm.py` parses the request with a deterministic rule-based extractor
3. `spec.py` validates and normalizes the resulting `FlinkJobSpec`
4. `generator.py` copies the local template into the output directory
5. `generator.py` renders placeholders in safe text files and renames common template files
6. `review.py` runs a small structural check on generated output
7. `main.py` prints a stable summary for scripting and terminal use

## Supported Request Patterns

The extractor remains deterministic and supports only a small set of phrasing variants around the same job shape.

Supported source-topic phrasing includes:

- `Read from Kafka <topic>`
- `Consume <topic> from Kafka`
- `Build a Flink job reading <topic>`
- `Read topic <topic>`

Supported key-field phrasing includes:

- `key by <field>`
- `keyed by <field>`
- `keying by <field>`
- `group by <field>`

Supported output-event phrasing includes:

- `emit <EVENT> within <N> minutes`
- `emit <EVENT> events within <N> minutes`
- `writing <EVENT> within <N> minutes`
- `write <EVENT> within <N> minutes`

The supported job shape is still one keyed Kafka rule job with one event-time field and one time window in minutes.

## Safe Defaults

The extractor applies a few fixed defaults:

- `sink_topic`: `inferred-events`
- `event_time_field`: `ts`
- `input_event_name`: `InputEvent`
- `rule_type`: `two_events_within_window`
- `job_name`: derived from the output event name
- `rule_condition`: generated from the output event name and time window

## Generation Flow

Generation stays local and deterministic:

1. `generator.py` uses the single local template directory `templates/flink_kafka_rule_job`
2. the template is copied into the requested output directory
3. placeholders are built from `FlinkJobSpec`
4. only known text file extensions are rendered
5. unresolved placeholders raise a clear error
6. common Java template files are renamed to match the resolved spec
7. the generator returns the full generated file list

Supported placeholders are:

- `{{JOB_NAME}}`
- `{{SOURCE_TOPIC}}`
- `{{SINK_TOPIC}}`
- `{{KEY_BY}}`
- `{{EVENT_TIME_FIELD}}`
- `{{INPUT_EVENT_NAME}}`
- `{{OUTPUT_EVENT_NAME}}`
- `{{RULE_TYPE}}`
- `{{RULE_CONDITION}}`
- `{{TIME_WINDOW_MINUTES}}`

## Structural Check Flow

After generation, `review.py` runs a small deterministic structural check.

It currently checks:

- output directory exists
- generated README exists
- generated main Flink job file exists
- no unresolved placeholders remain in generated text files

It returns:

- `passed_checks`
- `failed_checks`
- `warnings`

This is not a compiler step, not a linting system, and not a semantic review.

## Repository Structure

```text
flink-app-agent/
├── README.md
├── pyproject.toml
├── src/
│   └── flink_app_agent/
│       ├── __init__.py
│       ├── generator.py
│       ├── llm.py
│       ├── main.py
│       ├── review.py
│       ├── spec.py
│       ├── utils.py
│       └── prompts/
│           ├── extract_spec.md
│           └── generate_code.md
├── templates/
│   └── flink_kafka_rule_job/
└── tests/
    ├── test_end_to_end.py
    ├── test_generator.py
    ├── test_llm.py
    ├── test_main.py
    ├── test_review.py
    └── test_spec.py
```

## Example Request

```text
Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes
```

## Example Spec Summary

For the request above, the current extractor produces:

```json
{
  "job_name": "bedout-job",
  "source_topic": "sensor-events",
  "sink_topic": "inferred-events",
  "key_by": "user_id",
  "event_time_field": "ts",
  "input_event_name": "InputEvent",
  "output_event_name": "BedOut",
  "rule_type": "two_events_within_window",
  "rule_condition": "emit BedOut when two keyed events match within 20 minutes",
  "time_window_minutes": 20
}
```

## Example CLI Run

Install dependencies:

```bash
poetry install
```

Run the CLI:

```bash
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
```

Equivalent module form:

```bash
poetry run python -m flink_app_agent.main \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
```

Example output summary:

```text
Parsed spec summary:
{
  "job_name": "bedout-job",
  "source_topic": "sensor-events",
  "sink_topic": "inferred-events",
  "key_by": "user_id",
  "event_time_field": "ts",
  "input_event_name": "InputEvent",
  "output_event_name": "BedOut",
  "rule_type": "two_events_within_window",
  "rule_condition": "emit BedOut when two keyed events match within 20 minutes",
  "time_window_minutes": 20
}

Chosen template: flink_kafka_rule_job
Generation target: out
Generated files count: 7
Structural review summary: 4 passed, 0 failed, 0 warnings
```

## Example Generated Project Structure

```text
out/
├── README.md
├── pom.xml
└── src/
    ├── main/java/com/example/
    │   ├── BedoutJob.java
    │   ├── functions/RuleProcessFunction.java
    │   └── model/
    │       ├── BedOut.java
    │       └── InputEvent.java
    └── test/java/com/example/
        └── RuleProcessFunctionTest.java
```

## Testing Instructions

Run the full local Python test suite:

```bash
poetry run pytest
```

The current suite covers:

- spec validation and normalization
- deterministic extraction variants
- generator rendering and failure cases
- CLI success and failure output
- structural review checks
- one realistic end-to-end local flow

## Current Limitations

The implementation is still intentionally limited.

- no external LLM integration
- no multiple templates
- no multiple job families
- no Java compilation or Flink execution
- no Kafka connectivity checks
- no web server, database, or Docker support
- request understanding is still based on a small set of regex patterns
- sink topic and some other fields still rely on fixed defaults rather than user-controlled extraction

## Next Likely Developments After v0.2

Reasonable next steps after `v0.2` are:

- broaden the deterministic extractor carefully without losing readability
- add stronger compatibility checks between spec values and generation
- make the structural review slightly more informative
- add a second template only after the current single-template path remains stable
- introduce a real provider-backed extractor later behind the current interface

## v0.2 Boundaries

This README now reflects the actual `v0.2` implementation:

- one strict spec model
- one deterministic multi-variant extractor
- one local Flink template
- one local generator
- one deterministic structural review step
- one CLI flow
- no external model calls
