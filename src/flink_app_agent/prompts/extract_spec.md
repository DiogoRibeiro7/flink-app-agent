# Extract Flink Job Spec

Convert a natural-language Flink job request into the internal `FlinkJobSpec`.

Return only the following fields:

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

Current scope rules:

- Support Kafka-based jobs only
- Support only `rule_type = "two_events_within_window"`
- `job_name` must be filesystem-safe
- Topics must not be empty
- `time_window_minutes` must be positive

If the request is ambiguous, normalize it into the smallest valid spec that fits the first-version template.
