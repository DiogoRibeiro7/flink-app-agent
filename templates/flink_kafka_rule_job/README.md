# {{JOB_NAME}}

This project was generated from the single `flink_kafka_rule_job` template.

It is a small v0.1 Flink DataStream scaffold that shows:

- Kafka source wiring for `{{SOURCE_TOPIC}}`
- event-time extraction from `{{EVENT_TIME_FIELD}}`
- watermark assignment
- `keyBy({{KEY_BY}})`
- a `KeyedProcessFunction` implementing `{{RULE_TYPE}}`
- Kafka sink wiring for `{{SINK_TOPIC}}`

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

## Notes

- This is a starter template, not a production framework.
- Input parsing is intentionally simple.
- State and timers are shown in the process function, but the business logic remains minimal.
- `localhost:9092` is used as a placeholder Kafka bootstrap server.
