# Extract Flink Job Spec

Future model behavior for v0.1:

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

Current v0.1 scope:

- one Kafka source topic
- one key field
- one emitted output event
- one time window in minutes
- one supported rule type: `two_events_within_window`

If information is missing, use the safe v0.1 defaults only where explicitly allowed by the application.
