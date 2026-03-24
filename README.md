# flink-app-agent

`flink-app-agent` is a small Python project that converts a narrow plain-English Flink job request into:

1. a validated internal specification
2. a generated Java Flink project copied from a local template

The repository is intentionally limited to two narrow job families:

- a Kafka-to-Kafka keyed rule job
- a Kafka-to-Kafka windowed count aggregation job

## Purpose

The project exists to make the first step of Flink application scaffolding explicit and testable.
It does not try to solve general code generation, template management, deployment, or runtime configuration.

The current implementation is built around three ideas:

- parse a request into a strict `FlinkJobSpec`
- keep generation local and deterministic
- keep the template visible and editable in the repository

## Why The Scope Is Small

The scope is deliberately constrained so the repository stays understandable while the basic workflow is still changing.

Current choices that keep the project small:

- one spec model
- one deterministic parser instead of a real LLM call
- two local template directories
- one CLI entry point
- no UI, database, Docker setup, or deployment logic

## Current Architecture

The current flow is:

1. `main.py` reads a plain-English request and an output path from the CLI
2. `llm.py` loads `extract_spec.md` and uses a deterministic stub parser
3. `spec.py` validates the resulting `FlinkJobSpec` with Pydantic
4. `generator.py` selects a local template based on the validated spec
5. `generator.py` copies the selected template, replaces supported placeholders, and renames template class files

The parser is not an actual LLM client yet. It only accepts a restricted request pattern and raises a clear parsing error for unsupported inputs.

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
│   ├── flink_kafka_rule_job/
│   └── flink_windowed_aggregation_job/
└── tests/
    ├── test_generator.py
    ├── test_llm.py
    └── test_spec.py
```

## Example Request

```text
Read from Kafka source topic sensor-events, sink topic alerts,
key by user_id, emit BED_OUT within 20 minutes
```

## Example Generated Spec

Given the keyed-rule request above, the current stub parser produces a spec equivalent to:

```json
{
  "job_name": "bedout-job",
  "source_topic": "sensor-events",
  "sink_topic": "alerts",
  "key_by": "user_id",
  "event_time_field": "event_time",
  "input_event_name": "InputEvent",
  "output_event_name": "BedOut",
  "rule_type": "two_events_within_window",
  "rule_condition": "emit BedOut when two keyed events match within 20 minutes",
  "time_window_minutes": 20
}
```

Example windowed aggregation request:

```text
Build a Flink aggregation job that reads sensor-events, groups by user_id,
counts events in 5 minute windows, and writes WindowedCount to sensor-counts.
```

Expected windowed aggregation spec:

```json
{
  "job_name": "sensor-events-windowed-count-job",
  "source_topic": "sensor-events",
  "sink_topic": "sensor-counts",
  "key_by": "user_id",
  "event_time_field": "event_time",
  "input_event_name": "InputEvent",
  "output_event_name": "WindowedCount",
  "rule_type": "count_by_key_window",
  "rule_condition": "count events by user_id in 5 minute windows",
  "time_window_minutes": 5
}
```

## End-To-End Example

One realistic request used in the test suite is:

```text
Build a Kafka job named sensor occupancy alerts with source topic sensor-events,
sink topic occupancy-alerts, key by room_id, event time field event_ts,
and emit BED_OUT within 20 minutes.
```

Expected spec:

```json
{
  "job_name": "sensor-occupancy-alerts",
  "source_topic": "sensor-events",
  "sink_topic": "occupancy-alerts",
  "key_by": "room_id",
  "event_time_field": "event_ts",
  "input_event_name": "InputEvent",
  "output_event_name": "BedOut",
  "rule_type": "two_events_within_window",
  "rule_condition": "emit BedOut when two keyed events match within 20 minutes",
  "time_window_minutes": 20
}
```

Sample generated tree:

```text
out/
├── README.md
├── pom.xml
└── src/
    ├── main/java/com/example/
    │   ├── SensorOccupancyAlertsJob.java
    │   ├── functions/RuleProcessFunction.java
    │   └── model/
    │       ├── BedOut.java
    │       └── InputEvent.java
    └── test/java/com/example/
        └── RuleProcessFunctionTest.java
```

## How Generation Works

`generator.py` performs local filesystem operations only.

It:

- validates the template directory and output path
- copies the selected template directory into the requested output directory
- replaces placeholders in safe text files only
- skips non-text files by extension
- renames `JobTemplate.java`, `InputEvent.java`, and `OutputEvent.java`
- returns the list of generated files

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

## Running The CLI

Install dependencies:

```bash
poetry install
```

Run the CLI:

```bash
poetry run python -m flink_app_agent.main \
  --request "Read from Kafka source topic sensor-events, sink topic alerts, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
```

The CLI prints:

- the parsed spec before generation
- the output directory
- the list of generated files

## Running Tests

Run the full Python test suite:

```bash
poetry run pytest
```

Run a smaller subset:

```bash
poetry run pytest tests/test_spec.py tests/test_llm.py tests/test_generator.py tests/test_end_to_end.py
```

## Current Limitations

The current implementation is intentionally incomplete.

- No external LLM integration
- The parser only supports a restricted request pattern
- Only two narrow templates exist
- Template selection is explicit but only based on the supported rule types
- The Java template is a scaffold, not a production-ready Flink application
- Kafka parsing in the template is simplified
- Package names and richer Java project customization are not supported
- The CLI does not yet persist the parsed spec separately
- No end-to-end Java build verification is executed from Python

## Development Roadmap

Reasonable next steps for this repository are:

1. Replace the deterministic parser in `llm.py` with a real LLM-backed extraction step
2. Align template generation and template internals more tightly around generated class names
3. Add explicit spec-to-template compatibility checks
4. Add CLI tests
5. Add more templates only after the current two-template path is stable
6. Add optional Java build verification for generated output

## Extending The Template System Later

If template support grows beyond the current single-template setup, the smallest extension path is:

1. add a `template_id` field back into the validated spec
2. introduce a template registry that maps ids to local template directories
3. define placeholder compatibility rules per template
4. keep placeholder replacement separate from request parsing
5. add template-level tests that verify generated file names and unresolved placeholders

That keeps the current structure intact while allowing more than one project shape later.
