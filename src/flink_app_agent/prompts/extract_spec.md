# Extract Flink Job Spec

You are given a natural-language request for a Flink job.

Extract a structured payload with these fields:

- `template_id`
- `job_name`
- `job_class_name`
- `package_name`
- `input_topic`
- `output_topic`
- `consumer_group`
- `key_field`
- `rule_expression`
- `bootstrap_servers`
- `input_schema_class`
- `output_schema_class`

The only valid `template_id` is `flink_kafka_rule_job`.
