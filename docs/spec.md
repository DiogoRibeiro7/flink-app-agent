# Spec Model

The internal spec is defined in `src/flink_app_agent/spec.py` as `FlinkJobSpec`.

## Fields

The current model is intentionally small:

- `job_family`
- `job_name`
- `source_topic`
- `sink_topic`
- `key_by`
- `event_time_field`
- `input_event_name`
- `output_event_name`
- `rule_type`
- `rule_condition`
- `time_window_minutes`

## Validation And Normalization

Validation is strict because the spec is the handoff into template generation.

### `job_family`

- must be one of the supported job families:
  - `keyed_temporal_rule`
  - `windowed_aggregation`
- determines high-level template selection alongside `rule_type`

### `job_name`

- normalized into a filesystem-safe lowercase name
- rejects values that collapse to an empty name

### Topic fields

- `source_topic` and `sink_topic` are trimmed
- empty values are rejected

### Identifier-like fields

- `key_by`
- `event_time_field`

These are normalized into simple underscore-based identifiers and then validated.

### Class-name fields

- `input_event_name`
- `output_event_name`

These are normalized into PascalCase class names.

### Rule fields

- `rule_type` currently allows:
  - `two_events_within_window`
  - `count_by_key_window`
- `rule_condition` must be non-empty

### Window field

- `time_window_minutes` must be strictly positive

## Why The Spec Is Strict

The generator expects the spec to already be safe to substitute into filenames, Java identifiers, and template text. Rejecting or normalizing invalid values in `spec.py` keeps generation simpler and more predictable.

## Template Mapping

`FlinkJobSpec.to_template_dict()` returns the flat placeholder map used by `generator.py`:

- `JOB_FAMILY`
- `JOB_NAME`
- `SOURCE_TOPIC`
- `SINK_TOPIC`
- `KEY_BY`
- `EVENT_TIME_FIELD`
- `INPUT_EVENT_NAME`
- `OUTPUT_EVENT_NAME`
- `RULE_TYPE`
- `RULE_CONDITION`
- `TIME_WINDOW_MINUTES`
