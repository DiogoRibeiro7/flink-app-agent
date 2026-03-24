# flink-app-agent

`flink-app-agent` is a small Python CLI that turns a plain-English Flink job request into:

1. a structured job specification
2. a generated Flink project produced from a single built-in template

The first version is intentionally narrow. It supports one template only: a Kafka-to-Kafka keyed rule job for Apache Flink.

## Current limitations

- No real LLM integration yet
- No UI
- No database
- No Docker support
- No multi-template selection
- The natural-language extraction is a deterministic stub with simple heuristics
- The generated Flink project is a starter scaffold, not a production-ready job

## What the CLI does

The CLI accepts a natural-language request and an output directory. It will:

1. load the extraction prompt
2. ask the stub LLM interface for a structured payload
3. validate that payload with Pydantic
4. copy the Java template into the output directory
5. replace placeholders in the copied template
6. write the resolved spec as `job_spec.json`

## Example request

```text
Create a Flink job named fraud detector that reads from topic payments,
writes to topic alerts, keys by account_id, uses consumer group fraud-group,
and emits an alert when amount > 5000.
```

## Example usage

```bash
poetry install
poetry run flink-app-agent \
  --request "Create a Flink job named fraud detector that reads from topic payments, writes to topic alerts, keys by account_id, uses consumer group fraud-group, and emits an alert when amount > 5000." \
  --output-dir ./generated/fraud-detector
```

## Output

The command creates:

- a generated Flink Maven project
- a `job_spec.json` file with the structured spec used for generation

## Development

```bash
poetry install
poetry run pytest
```

## Project structure

```text
flink-app-agent/
├── README.md
├── pyproject.toml
├── src/
│   └── flink_app_agent/
│       ├── __init__.py
│       ├── main.py
│       ├── spec.py
│       ├── generator.py
│       ├── llm.py
│       ├── prompts/
│       │   ├── extract_spec.md
│       │   └── generate_code.md
│       └── utils.py
├── templates/
│   └── flink_kafka_rule_job/
└── tests/
```
