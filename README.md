# flink-app-agent

`flink-app-agent` is a small Python project that turns a narrow plain-English Flink job request into:

1. a validated internal specification
2. a generated Java Flink project from a local template

The repository is intentionally limited. It is meant to make the current generation flow explicit, deterministic, and easy to inspect before any real LLM integration or deployment logic is added.

## Project Goal

The goal is to prove a small end-to-end workflow for Flink job scaffolding:

- accept a plain-English request
- extract a strict `FlinkJobSpec`
- render a generated project from one local template

This is not a general Flink code generator. It is a narrow scaffold around a few supported job shapes.

## Intentionally Narrow Scope

The scope stays small on purpose:

- Python CLI only
- deterministic extraction only
- local filesystem generation only
- one local template checked into the repository
- no UI
- no database
- no Docker
- no external LLM call yet
- no review step yet

The narrow scope keeps the codebase readable while the core boundaries are still being established.

## Current Architecture

The current implementation is organized into four small layers:

- `main.py`
  Reads CLI arguments, runs extraction and generation, then prints a summary.
- `llm.py`
  Contains the prompt loader, deterministic stub extractor, and a placeholder adapter for future provider-backed extraction.
- `spec.py`
  Defines `FlinkJobSpec` and performs strict validation and normalization with Pydantic.
- `generator.py`
  Copies the local template, replaces placeholders in safe text files, renames template classes, and returns the generated file list.

## Extraction Flow

The extraction path is:

1. `main.py` calls the default extraction interface from `llm.py`
2. the extraction prompt can be loaded from `src/flink_app_agent/prompts/extract_spec.md`
3. the deterministic stub extractor parses the request into a raw payload
4. `spec.py` validates and normalizes that payload into `FlinkJobSpec`

The parser is still rule-based. It does not call an external LLM or SDK.

## Generation Flow

The generation path is:

1. `generator.py` uses the single local template directory
2. the template directory is copied to the output directory
3. placeholders are built from `FlinkJobSpec`
4. only safe text files are rendered
5. common template class files are renamed to match the generated spec
6. the generator returns the list of generated files

Supported placeholders currently are:

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

## Supported Job Family

The current implementation supports one narrow family only:

1. Kafka-to-Kafka keyed rule job
   `rule_type = "two_events_within_window"`

## Repository Structure

```text
flink-app-agent/
├── README.md
├── ROADMAP.md
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

## Example Requests

Example request:

```text
Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes
```

## Example Generated Specs

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

## Example Generated Outputs

Example output tree:

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

Example aggregation output tree:

```text
out/
├── README.md
├── pom.xml
└── src/
    ├── main/java/com/example/
    │   ├── SensorEventsWindowedCountJob.java
    │   ├── functions/WindowedCountProcessWindowFunction.java
    │   └── model/
    │       ├── InputEvent.java
    │       └── WindowedCount.java
    └── test/java/com/example/
        └── WindowedCountProcessWindowFunctionTest.java
```

## Running The CLI

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

The CLI currently prints:

- the parsed spec
- the generated file list

## Running Tests

Run the full suite:

```bash
poetry run pytest
```

Run a focused subset:

```bash
poetry run pytest tests/test_spec.py tests/test_llm.py tests/test_generator.py tests/test_main.py
```

## Limitations

The current implementation remains intentionally incomplete.

- extraction is deterministic and regex-based
- only one narrow template exists
- only one small request wording is supported
- generated Java code is scaffold code, not production-ready application code
- Kafka parsing and serialization in templates are simplified
- no package-name customization
- no external LLM integration
- no Java compilation from the Python flow
- no review step yet
- no Maven or Flink execution in tests
