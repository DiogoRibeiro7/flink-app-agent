# Templates

Template resolution and rendering are small explicit subsystems.

## Template Registry

`src/flink_app_agent/template_registry.py` defines:

- `TemplateDefinition`
- `TemplateRegistry`
- `TemplateRegistryError`

Each template definition records:

- template identifier
- template path
- job family
- supported rule types
- short description
- runtime

The current registry contains three active real templates:

- `flink_kafka_rule_job`
- `flink_windowed_aggregation_job`
- `flink_session_window_aggregation_job`

They support:

- `two_events_within_window`
- `count_by_key_window`
- `count_by_key_session_window`

## Template Selection

`TemplateRegistry.resolve_for_spec(spec)` chooses a template by checking whether the spec's `job_family` and `rule_type` both match a registered template.

This keeps template lookup explicit and lets unsupported specs fail before rendering starts. The two aggregation rules share the `windowed_aggregation` family but resolve to different templates so tumbling and session semantics cannot be mixed accidentally.

## Rendering

`src/flink_app_agent/generator.py` handles local project generation.

The generator:

1. validates the template path and output destination
2. copies the template directory
3. builds a centralized placeholder map from `FlinkJobSpec`
4. renders only known text file types
5. detects unresolved placeholders
6. renames common Java template files
7. returns the generated file list

Rendering is intentionally simple. It uses direct placeholder replacement rather than a larger templating system.

## Placeholder Set

The current templates use:

- `{{JOB_FAMILY}}`
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

## Current Template Shape

`templates/flink_kafka_rule_job/` provides the keyed temporal-rule starter with a `KeyedProcessFunction` scaffold.

`templates/flink_windowed_aggregation_job/` provides keyed counts over `TumblingEventTimeWindows`.

`templates/flink_session_window_aggregation_job/` provides keyed counts over `EventTimeSessionWindows`; `TIME_WINDOW_MINUTES` is interpreted as the inactivity gap.

All three templates include a Maven build, README, job entrypoint, simplified models, and a small Java test scaffold.
