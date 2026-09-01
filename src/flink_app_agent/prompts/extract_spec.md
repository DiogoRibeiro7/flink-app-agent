# Extract Flink Job Spec

Convert a plain-English Flink job request into a JSON object matching `FlinkJobSpec`.

Return exactly these fields as a JSON object:

- `job_family`: one of `keyed_temporal_rule` or `windowed_aggregation`
- `job_name`: filesystem-safe lowercase hyphenated name
- `source_topic`: Kafka source topic
- `sink_topic`: Kafka sink topic
- `key_by`: field to partition the stream by
- `event_time_field`: field used for event-time extraction
- `input_event_name`: PascalCase input event class name
- `output_event_name`: PascalCase output event class name
- `rule_type`: one of `two_events_within_window`, `count_by_key_window`, or `count_by_key_session_window`
- `rule_condition`: human-readable rule description
- `time_window_minutes`: positive integer duration in minutes

Current repository scope:

- one Kafka source topic
- one key field
- one event-time duration in minutes
- one of three narrow rule shapes:
  - `keyed_temporal_rule` family with `two_events_within_window`
  - `windowed_aggregation` family with `count_by_key_window` for tumbling event-time windows
  - `windowed_aggregation` family with `count_by_key_session_window`, where `time_window_minutes` is the inactivity gap

If information is missing, use only the safe defaults explicitly defined by the application.
