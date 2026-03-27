# flink-app-agent

`flink-app-agent` is a small internal-style CLI that turns a narrow plain-English Flink job request into:

1. a validated internal spec
2. a generated Flink Java project from a small local template set
3. a deterministic repair loop, structural review, optional compile verification, and a JSON generation report

The repository is intentionally small. It is meant to make one path from request text to generated project explicit and testable, not to cover broad Flink design space or open-ended code generation.

Deterministic extraction remains the default. Provider-backed extraction is available as an optional path behind the same extraction interface, but it is still constrained by strict normalization, spec validation, and local template selection.

## Scope

Current scope is deliberately narrow:

- one CLI entry point
- one strict spec model with explicit job family support
- one deterministic extractor recognizing two job families
- one optional provider-backed extraction path selected through the CLI or environment
- two registered real templates (keyed temporal rule and windowed aggregation)
- one local generator with family-aware placeholder rendering
- one deterministic repair loop for safe fixups (trailing placeholders, whitespace, newlines)
- one deterministic review step with family-specific structural checks
- one optional compile-only verification step (requires Maven)
- one JSON report artifact with pipeline status, extraction provenance, repair pass, and verification results

The project does not try to infer many job families, manage infrastructure, or hide the generation process behind a larger service.

## Pipeline Overview

The current pipeline is:

1. `main.py` parses CLI arguments
2. `llm.py` preprocesses the request, loads the extraction prompt, and runs either the deterministic extractor or the optional provider-backed extractor
3. provider-backed payloads are normalized in `provider_normalizer.py`, then all payloads go through the same `spec.py` validation path
4. `template_registry.py` resolves the local template for the spec
5. `generator.py` copies the template, renders placeholders in safe text files, and returns generated paths
6. `repair.py` runs a deterministic repair loop for safe fixups
7. `review.py` runs lightweight file-based structural checks on the generated project
8. `verification.py` optionally runs `mvn compile` on the generated project (with `--verify`)
9. `report.py` writes `generation_report.json` with full pipeline status and extraction provenance
10. `main.py` prints a concise summary

Deeper implementation notes live in:

- [Architecture](docs/architecture.md)
- [Extraction Modes](docs/extraction.md)
- [Provider Extraction Boundary](docs/provider.md)
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
│   ├── extraction.md
│   ├── provider.md
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
│       ├── repair.py
│       ├── report.py
│       ├── review.py
│       ├── spec.py
│       ├── verification.py
│       ├── template_registry.py
│       ├── utils.py
│       └── prompts/
│           ├── extract_spec.md
│           └── generate_code.md
├── templates/
│   ├── flink_kafka_rule_job/
│   └── flink_windowed_aggregation_job/
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

Run with the optional provider-backed extraction path:

```bash
export FLINK_AGENT_PROVIDER_ENTRY_POINT="my_provider:call_provider"
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out \
  --extractor provider
```

Allow deterministic fallback if provider extraction fails:

```bash
export FLINK_AGENT_PROVIDER_ENTRY_POINT="my_provider:call_provider"
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out \
  --extractor provider \
  --fallback deterministic
```

Run with optional compile verification (requires Maven on PATH):

```bash
poetry run flink-app-agent \
  --request "Read from Kafka sensor-events, key by user_id, emit BED_OUT within 20 minutes" \
  --output ./out \
  --verify
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

Also supported in the current narrow scope:

```text
Read from Kafka sensor-events, group by device_id, count events within 5 minutes
```

## Example Output

For that request, the current extractor produces a spec like:

```json
{
  "job_family": "keyed_temporal_rule",
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
  "job_family": "keyed_temporal_rule",
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

Requested extractor: deterministic
Extraction path: deterministic
Fallback occurred: no
Job family: keyed_temporal_rule
Chosen template: flink_kafka_rule_job
Generation target: out
Generated files count: 7
Generation report: out\generation_report.json
Repair pass: 1 passes, 0 repairs
Structural review summary: passed, 10 passed, 0 failed, 0 warnings
```

If provider-backed extraction is selected, the summary also shows the requested mode and the actual path used. A fallback run looks like:

```text
Requested extractor: provider
Extraction path: provider -> deterministic
Fallback occurred: yes
Fallback reason: ProviderExtractionError: Provider returned invalid JSON: ...
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

- no built-in remote provider integration
- no web service or database
- no Flink runtime execution
- no Docker or deployment workflow
- compile verification is opt-in and requires Maven on PATH
- only two job families (keyed temporal rule and windowed aggregation)
- only two supported rule types
- deterministic parsing is still pattern-based and remains the default
- provider-backed extraction is optional and only supported through an injected callable boundary
- provider-backed output is accepted only if it survives normalization and strict spec validation
- fallback is limited to deterministic extraction when explicitly configured
- repairs are intentionally limited to safe text cleanup only (no model-based patching)
- some fields are filled through fixed defaults rather than user-controlled extraction
- no joins, enrichment, or sessionization families yet

## Roadmap

Planned next steps are tracked in [ROADMAP.md](ROADMAP.md). The current direction is to keep the pipeline small while strengthening extraction boundaries, validation, rendering, and verification incrementally.
