# Generate Flink Code

Future model-backed behavior:

Generate a Flink project from a validated `FlinkJobSpec`.

The generated project should:

- read from the configured Kafka source topic
- key the stream by the configured `key_by` field
- use `event_time_field` for event-time handling
- apply the rule shape implied by `rule_type`
- emit the configured output event to the Kafka sink topic

Keep the generated code small, explicit, and aligned with the selected local template.
