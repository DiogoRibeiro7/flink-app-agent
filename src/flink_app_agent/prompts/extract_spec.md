# Extract Flink Job Spec

Future model-backed behavior:

Convert a plain-English Flink job request into a validated `FlinkJobSpec`.

Return exactly these fields:

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

Current repository scope:

- one Kafka source topic
- one key field
- one time window in minutes
- one of two narrow job shapes:
  - keyed rule event emission with `two_events_within_window`
  - windowed count aggregation with `count_by_key_window`

If information is missing, use only the safe defaults explicitly defined by the application.
