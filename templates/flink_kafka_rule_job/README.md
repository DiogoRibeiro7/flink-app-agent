# {{JOB_NAME}}

This directory is a reusable Flink template for a Kafka-to-Kafka keyed rule job.
It is intentionally small and meant to be filled by `flink-app-agent`, not used as a
finished production application.

## Injected Placeholders

The generator replaces the following placeholders in text files:

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

## Template Notes

- `JobTemplate.java` defines the Flink DataStream job, Kafka source and sink wiring,
  and watermark assignment.
- `InputEvent.java` and `OutputEvent.java` are placeholder event models with simple
  parsing and serialization helpers.
- `RuleProcessFunction.java` shows where keyed state and timer logic belong.
- `RuleProcessFunctionTest.java` is a lightweight scaffold around the process function.

## Generated Values In This Template

- Source topic: `{{SOURCE_TOPIC}}`
- Sink topic: `{{SINK_TOPIC}}`
- Key field: `{{KEY_BY}}`
- Event time field: `{{EVENT_TIME_FIELD}}`
- Input event name: `{{INPUT_EVENT_NAME}}`
- Output event name: `{{OUTPUT_EVENT_NAME}}`
- Rule type: `{{RULE_TYPE}}`
- Rule condition: `{{RULE_CONDITION}}`
- Time window minutes: `{{TIME_WINDOW_MINUTES}}`
