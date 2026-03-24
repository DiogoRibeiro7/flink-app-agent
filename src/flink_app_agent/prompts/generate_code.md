# Generate Flink Code

Use the structured spec to fill the single supported Flink project template.

The generated project should:

- read JSON-like input events from Kafka
- key the stream by the requested field
- apply a simple keyed rule process function
- emit a compact output event to Kafka
