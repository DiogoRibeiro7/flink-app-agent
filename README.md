# flink-app-agent

`flink-app-agent` is a small Python project that turns a narrow plain-English Flink job request into:

1. a validated internal specification
2. a generated Java Flink project from a local template
3. a lightweight post-generation review result

The repository is intentionally limited. It is meant to make the current generation flow explicit, deterministic, and easy to inspect before any real LLM integration or deployment logic is added.

## Project Goal

The goal is to prove a small end-to-end workflow for Flink job scaffolding:

- accept a plain-English request
- extract a strict `FlinkJobSpec`
- choose a compatible local template
- render a generated project
- review the result for obvious structural issues

This is not a general Flink code generator. It is a narrow scaffold around a few supported job shapes.

## Intentionally Narrow Scope

The scope stays small on purpose:

- Python CLI only
- deterministic extraction only
- local filesystem generation only
- local templates checked into the repository
- two supported template families only
- no UI
- no database
- no Docker
- no deployment workflow
- no external LLM call yet

The narrow scope keeps the codebase readable while the core boundaries are still being established.

## Current Architecture

The current implementation is organized into five small layers:

- `main.py`
  Reads CLI arguments, runs extraction, generation, and review, then prints a summary.
- `llm.py`
  Contains the extraction pipeline, prompt loading, request preprocessing, the deterministic stub extractor, and a placeholder adapter for a future provider-backed extractor.
- `spec.py`
  Defines `FlinkJobSpec` and performs strict validation and normalization with Pydantic.
- `generator.py`
  Selects a template, copies it locally, replaces placeholders in safe text files, renames template classes, and returns the generated file list.
- `review.py`
  Performs a deterministic review of the generated output and can repair trivial trailing placeholder-only lines.

## Extraction Flow

The extraction path is:

1. `main.py` calls the default extraction interface from `llm.py`
2. the request is normalized by the request preprocessor
3. the extraction prompt is loaded from `src/flink_app_agent/prompts/extract_spec.md`
4. the deterministic stub extractor parses the request into a raw payload
5. `spec.py` validates and normalizes that payload into `FlinkJobSpec`

The parser is still rule-based. It does not call an external LLM or SDK.

## Generation Flow

The generation path is:

1. `generator.py` selects a template from the template catalog using `spec.rule_type`
2. the selected template directory is copied to the output directory
3. placeholders are built from `FlinkJobSpec`
4. only safe text files are rendered
5. unresolved placeholders cause generation failure
6. common template class files are renamed to match the generated spec
7. the generator returns the list of generated files

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

## Review Flow

After generation, `review.py` performs a small deterministic review.

It checks at least:

- output directory exists
- README exists
- main Flink job file exists
- no unresolved placeholders remain in safe text files
- configured source and sink topics appear in the generated README
- configured source and sink topics appear in the generated main job file

It returns a structured result with:

- passed checks
- failed checks
- warnings
- repairs

The current repair behavior is intentionally small. It can remove trailing lines that contain only unresolved placeholder markers.

## Supported Job Families

Two narrow families are currently supported:

1. Kafka-to-Kafka keyed rule job
   `rule_type = "two_events_within_window"`
2. Kafka-to-Kafka windowed count aggregation job
   `rule_type = "count_by_key_window"`

Template selection is explicit. The generator does not assume a single template implicitly anymore.

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
│       ├── review.py
│       ├── spec.py
│       ├── utils.py
│       └── prompts/
│           ├── extract_spec.md
│           └── generate_code.md
├── templates/
│   ├── flink_kafka_rule_job/
│   └── flink_windowed_aggregation_job/
└── tests/
    ├── test_end_to_end.py
    ├── test_generator.py
    ├── test_llm.py
    ├── test_review.py
    └── test_spec.py
```

## Example Requests

Keyed rule example:

```text
Build a Kafka job named sensor occupancy alerts with source topic sensor-events,
sink topic occupancy-alerts, key by room_id, event time field event_ts,
and emit BED_OUT within 20 minutes.
```

Windowed aggregation example:

```text
Build a Flink aggregation job that reads sensor-events, groups by user_id,
counts events in 5 minute windows, and writes WindowedCount to sensor-counts.
```

## Example Generated Specs

Keyed rule request:

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

Windowed aggregation request:

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

## Example Generated Outputs

Example keyed-rule output tree:

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
poetry run python -m flink_app_agent.main \
  --request "Read from Kafka source topic sensor-events, sink topic alerts, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
```

The CLI currently prints:

- the parsed spec
- the generated output directory
- the generated file list
- the post-generation review summary

## Running Tests

Run the full suite:

```bash
poetry run pytest
```

Run a focused subset:

```bash
poetry run pytest tests/test_spec.py tests/test_llm.py tests/test_generator.py tests/test_review.py tests/test_end_to_end.py
```

## Limitations

The current implementation remains intentionally incomplete.

- extraction is deterministic and regex-based
- only two narrow template families exist
- only a small set of request phrasings is supported
- template selection is driven only by supported `rule_type` values
- generated Java code is scaffold code, not production-ready application code
- Kafka parsing and serialization in templates are simplified
- no package-name customization
- no external LLM integration
- no Java compilation from the Python flow
- no Maven or Flink execution in tests
- no deployment, packaging, or runtime environment support yet

The next staged work is documented in [ROADMAP.md](/C:/Users/diogo/work_code/flink-app-agent/ROADMAP.md).
