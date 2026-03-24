# flink-app-agent

`flink-app-agent` is a small Python CLI that turns a narrow plain-English Flink job request into:

1. a validated internal specification
2. a generated Java Flink project from one local template

## Project Purpose

The purpose of `flink-app-agent` v0.1 is to provide the smallest useful foundation for Flink job scaffolding.

It does not try to solve general code generation or broad natural-language understanding. It only proves a simple path from request text to a validated spec and then to a generated starter project.

## Why The Scope Is Intentionally Narrow

The v0.1 scope is deliberately small so the core pieces stay explicit and easy to inspect:

- one CLI entry point
- one strict spec model
- one deterministic extractor stub
- one local Flink template
- local filesystem generation only

This keeps the repository understandable while the basic interfaces are still being established.

## What v0.1 Can Do

Version `v0.1` can:

- accept a narrow plain-English Flink job request
- extract a validated `FlinkJobSpec`
- apply a few fixed defaults
- copy one local Flink Java template
- replace placeholders in text files
- rename common template Java files
- print the parsed spec and generated file list from the CLI

The supported job shape is a single Kafka-to-Kafka keyed rule job with event-time handling and a `KeyedProcessFunction`.

## What v0.1 Cannot Do

Version `v0.1` does not:

- call a real LLM or external API
- support multiple templates
- support multiple Flink job families
- support broad natural-language phrasing
- run a review step after generation
- compile or execute the generated Java project
- provide a web server, database, Docker setup, or deployment flow

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
│       ├── spec.py
│       ├── utils.py
│       └── prompts/
│           ├── extract_spec.md
│           └── generate_code.md
├── templates/
│   └── flink_kafka_rule_job/
└── tests/
    ├── test_generator.py
    ├── test_llm.py
    ├── test_main.py
    └── test_spec.py
```

## Architecture Flow

The current flow is:

1. `main.py` reads `--request` and `--output`
2. `llm.py` parses the request with a deterministic stub extractor
3. `spec.py` validates and normalizes the resulting `FlinkJobSpec`
4. `generator.py` copies the local template into the output directory
5. `generator.py` replaces supported placeholders in text files and renames common template files
6. `main.py` prints the parsed spec and generated file list

## Example Request

```text
Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes
```

## Example Resulting Spec

For the request above, the current stub produces:

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

## Example Generation Command

Install dependencies:

```bash
poetry install
```

Run the CLI:

```bash
poetry run python -m flink_app_agent.main \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
```

The Poetry script entry point is also available:

```bash
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
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

Run the focused v0.1 tests:

```bash
poetry run pytest tests/test_spec.py tests/test_llm.py tests/test_generator.py tests/test_main.py
```

Run the full Python test suite currently in the repository:

```bash
poetry run pytest
```

## Next Expected Developments

The next likely steps after `v0.1` are:

- broaden the extractor carefully beyond the one supported wording pattern
- improve generator validation and unresolved placeholder handling
- add a second template only after the single-template path is stable
- introduce a real provider-backed extraction implementation later without changing the full flow

## v0.1 Boundaries

This README reflects the actual v0.1 boundaries in the codebase:

- one spec model
- one deterministic extractor stub
- one local Flink template
- one CLI flow
- no review step
- no real LLM integration
