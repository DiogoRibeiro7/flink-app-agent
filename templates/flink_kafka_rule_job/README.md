# {{JOB_NAME}}

This project was generated from the single `flink_kafka_rule_job` template.

It is a small v0.2 Flink DataStream scaffold that shows:

- Kafka source wiring for `{{SOURCE_TOPIC}}`
- event-time extraction from `{{EVENT_TIME_FIELD}}`
- watermark assignment
- `keyBy({{KEY_BY}})`
- a `KeyedProcessFunction` implementing `{{RULE_TYPE}}`
- Kafka sink wiring for `{{SINK_TOPIC}}`

## What The Generated Job Does

The generated job currently:

- reads raw string events from Kafka topic `{{SOURCE_TOPIC}}`
- parses them into `{{INPUT_EVENT_NAME}}`
- extracts event time from `{{EVENT_TIME_FIELD}}`
- applies bounded-out-of-orderness watermarks
- keys the stream by `{{KEY_BY}}`
- runs `RuleProcessFunction` for the fixed rule type `{{RULE_TYPE}}`
- emits `{{OUTPUT_EVENT_NAME}}` records to Kafka topic `{{SINK_TOPIC}}`

## Injected Placeholders

The generator replaces these values:

- `{{JOB_NAME}}`: job and Maven artifact name
- `{{SOURCE_TOPIC}}`: Kafka source topic
- `{{SINK_TOPIC}}`: Kafka sink topic
- `{{KEY_BY}}`: key field used before the process function
- `{{EVENT_TIME_FIELD}}`: event-time field used for timestamps
- `{{INPUT_EVENT_NAME}}`: input event model class
- `{{OUTPUT_EVENT_NAME}}`: output event model class
- `{{RULE_TYPE}}`: current rule type
- `{{RULE_CONDITION}}`: readable rule description
- `{{TIME_WINDOW_MINUTES}}`: keyed matching window

## What Is Still Scaffolding

- Input parsing is intentionally simple and map-based.
- Kafka bootstrap servers are hardcoded to `localhost:9092`.
- The rule logic is deliberately small and only demonstrates the current generated rule shape.
- Output serialization uses a small hand-built JSON string.

## What Is Intended To Be Real Generated Code

- The Flink job structure and control flow
- Source and sink wiring
- Event-time extraction and watermark assignment
- The `keyBy` and `KeyedProcessFunction` boundary
- The minimal keyed state and timer structure in the process function
