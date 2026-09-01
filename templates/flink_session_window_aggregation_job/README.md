# {{JOB_NAME}}

Job family: `{{JOB_FAMILY}}`

This project was generated from the `flink_session_window_aggregation_job` template in
`flink-app-agent`.

It is a starter for a Kafka-to-Kafka Flink DataStream aggregation job that:

- reads events from `{{SOURCE_TOPIC}}`
- extracts event time from `{{EVENT_TIME_FIELD}}`
- assigns watermarks
- partitions the stream with `keyBy({{KEY_BY}})`
- groups keyed events into event-time sessions separated by `{{TIME_WINDOW_MINUTES}}` minutes of inactivity
- counts events per session
- writes `{{OUTPUT_EVENT_NAME}}` events to `{{SINK_TOPIC}}`

## Injected Values

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

## Notes

- `{{TIME_WINDOW_MINUTES}}` is the event-time session inactivity gap, not a fixed window width.
- Parsing and serialization remain intentionally simplified.
- `localhost:9092` is used as a placeholder bootstrap server in the generated code.
