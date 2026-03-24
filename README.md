# flink-app-agent

`flink-app-agent` is a small internal-style CLI that turns a narrow plain-English Flink job request into:

1. a validated internal spec
2. a generated Flink Java project from one local template
3. a deterministic structural review and JSON generation report

The repository is intentionally small. It is meant to make one path from request text to generated project explicit and testable, not to cover broad Flink design space or open-ended code generation.

## Scope

Current scope is deliberately narrow:

- one CLI entry point
- one strict spec model
- one deterministic extractor
- one registered real template
- one local generator
- one deterministic review step
- one JSON report artifact

The project does not try to infer many job families, manage infrastructure, or hide the generation process behind a larger service.

## Pipeline Overview

The current pipeline is:

1. `main.py` parses CLI arguments
2. `llm.py` preprocesses the request, loads the extraction prompt, and runs the deterministic extractor
3. `spec.py` validates and normalizes the resulting `FlinkJobSpec`
4. `template_registry.py` resolves the local template for the spec
5. `generator.py` copies the template, renders placeholders in safe text files, and returns generated paths
6. `review.py` runs lightweight file-based checks on the generated project
7. `report.py` writes `generation_report.json`
8. `main.py` prints a concise summary

Deeper implementation notes live in:

- [Architecture](docs/architecture.md)
- [Spec Model](docs/spec.md)
- [Templates](docs/templates.md)
- [Review And Report](docs/review.md)

## Repository Structure

```text
flink-app-agent/
├── README.md
├── ROADMAP.md
├── docs/
│   ├── architecture.md
│   ├── review.md
│   ├── spec.md
│   └── templates.md
├── pyproject.toml
├── src/
│   └── flink_app_agent/
│       ├── __init__.py
│       ├── generation_context.py
│       ├── generator.py
│       ├── llm.py
│       ├── main.py
│       ├── report.py
│       ├── review.py
│       ├── spec.py
│       ├── template_registry.py
│       ├── utils.py
│       └── prompts/
│           ├── extract_spec.md
│           └── generate_code.md
├── templates/
│   └── flink_kafka_rule_job/
└── tests/
```

## Quickstart

Install dependencies:

```bash
poetry install
```

Run the default generation flow:

```bash
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out
```

Useful inspection-only modes:

```bash
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --print-spec-only
```

```bash
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --print-template-info
```

## Example Request

```text
Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes
```

## Example Output

For that request, the current extractor produces a spec like:

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

Typical CLI summary:

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
Generation report: out\generation_report.json
Structural review summary: passed, 8 passed, 0 failed, 0 warnings
```

Example generated project shape:

```text
out/
├── README.md
├── generation_report.json
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

## Limitations

Current limitations are explicit:

- no external LLM or API calls
- no web service or database
- no Java compile or runtime verification
- no Docker or deployment workflow
- one real template only
- one supported rule type only
- request parsing is still deterministic and pattern-based
- some fields are filled through fixed defaults rather than user-controlled extraction

## Roadmap

Planned next steps are tracked in [ROADMAP.md](ROADMAP.md). The current direction is to keep the pipeline small while strengthening extraction boundaries, validation, rendering, and verification incrementally.
