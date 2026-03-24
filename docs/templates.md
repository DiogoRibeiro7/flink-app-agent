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
- supported rule types
- short description
- runtime

The current registry contains one active real template:

- `flink_kafka_rule_job`

It supports:

- `two_events_within_window`

## Template Selection

`TemplateRegistry.resolve_for_spec(spec)` chooses a template by checking whether the spec's `rule_type` is supported by a registered template.

This keeps template lookup explicit and lets unsupported specs fail before rendering starts.

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

The current template uses:

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

`templates/flink_kafka_rule_job/` is a minimal Java Flink starter with:

- Maven build file
- template README
- Flink job entrypoint
- input and output models
- `KeyedProcessFunction` scaffold
- small Java test scaffold
