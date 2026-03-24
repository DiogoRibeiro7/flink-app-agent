# {{JOB_NAME}}

This project was generated from the single built-in `flink_kafka_rule_job` template in
`flink-app-agent`.

It is a small starter for a Kafka-to-Kafka Flink DataStream job that:

- reads events from `{{SOURCE_TOPIC}}`
- extracts event time from `{{EVENT_TIME_FIELD}}`
- assigns watermarks
- partitions the stream with `keyBy({{KEY_BY}})`
- applies a `KeyedProcessFunction`
- writes derived `{{OUTPUT_EVENT_NAME}}` events to `{{SINK_TOPIC}}`

## Injected Values

The generator replaced these placeholders when this project was created:

- `{{JOB_NAME}}`: generated job and artifact name
- `{{SOURCE_TOPIC}}`: Kafka source topic
- `{{SINK_TOPIC}}`: Kafka sink topic
- `{{KEY_BY}}`: key field used before the process function
- `{{EVENT_TIME_FIELD}}`: event-time field used for timestamps and timers
- `{{INPUT_EVENT_NAME}}`: generated input event model class
- `{{OUTPUT_EVENT_NAME}}`: generated output event model class
- `{{RULE_TYPE}}`: currently supported rule type
- `{{RULE_CONDITION}}`: human-readable rule description
- `{{TIME_WINDOW_MINUTES}}`: keyed matching window

## Generated Structure

- `src/main/java/com/example/JobTemplate.java`
  Main Flink job wiring: source, timestamp extraction, watermark strategy, `keyBy`, process function, and sink.
- `src/main/java/com/example/model/InputEvent.java`
  Minimal input event parser and accessors.
- `src/main/java/com/example/model/OutputEvent.java`
  Minimal output event model and serializer.
- `src/main/java/com/example/functions/RuleProcessFunction.java`
  Keyed process function skeleton with state and timer hooks.
- `src/test/java/com/example/RuleProcessFunctionTest.java`
  Lightweight process-function test scaffold.

## Notes

- Kafka parsing is intentionally simplified.
- State and timer handling are shown, but business rule behavior is still scaffolding.
- `localhost:9092` is used as a placeholder bootstrap server in the generated code.
