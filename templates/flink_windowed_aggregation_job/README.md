# {{JOB_NAME}}

This project was generated from the `flink_windowed_aggregation_job` template in
`flink-app-agent`.

It is a small starter for a Kafka-to-Kafka Flink DataStream windowed aggregation job that:

- reads events from `{{SOURCE_TOPIC}}`
- extracts event time from `{{EVENT_TIME_FIELD}}`
- assigns watermarks
- partitions the stream with `keyBy({{KEY_BY}})`
- performs a windowed count aggregation over `{{TIME_WINDOW_MINUTES}}` minutes
- writes `{{OUTPUT_EVENT_NAME}}` events to `{{SINK_TOPIC}}`

## Injected Values

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

## Notes

- This template supports one aggregation family only: count by key over an event-time tumbling window.
- Parsing and serialization remain intentionally simplified.
- `localhost:9092` is used as a placeholder bootstrap server in the generated code.
