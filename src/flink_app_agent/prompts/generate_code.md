# Generate Flink Code

Generate a Flink project from a validated `FlinkJobSpec`.

The generated project should:

- read events from a Kafka source topic
- assign keys with the configured `key_by` field
- use the configured `event_time_field` for event-time logic
- implement the `two_events_within_window` rule pattern
- emit the configured output event to the Kafka sink topic

Keep the output small, explicit, and consistent with the current single-template project structure.
