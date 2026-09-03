package com.example;

import com.example.functions.RuleProcessFunction;
import com.example.model.{{INPUT_EVENT_NAME}};
import com.example.model.{{OUTPUT_EVENT_NAME}};
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Minimal generated Flink job for {{JOB_NAME}}.
 *
 * <p>This template keeps the full flow visible: Kafka source, event-time handling, watermark
 * assignment, keyBy, keyed processing, and Kafka sink output.
 */
final class JobTemplate {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Duration ALLOWED_LATENESS = Duration.ofSeconds(30);

    private JobTemplate() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = buildSource();
        KafkaSink<String> sink = buildSink();

        // 1. Read raw events from Kafka.
        DataStream<String> rawEvents = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "{{JOB_NAME}}-source");

        // 2. Parse the raw payload into the generated input event model.
        DataStream<{{INPUT_EVENT_NAME}}> inputEvents =
                rawEvents
                        .map({{INPUT_EVENT_NAME}}::fromRaw)
                        .assignTimestampsAndWatermarks(buildWatermarkStrategy());

        // 3. Reject records without the configured key before keyed state is created.
        DataStream<{{INPUT_EVENT_NAME}}> keyedCandidates =
                inputEvents.filter(event -> !event.getField("{{KEY_BY}}").isBlank());

        // 4. Key the stream and apply the generated keyed rule logic.
        DataStream<{{OUTPUT_EVENT_NAME}}> outputEvents =
                keyedCandidates
                        .keyBy(event -> event.getField("{{KEY_BY}}"))
                        .process(new RuleProcessFunction(
                                "{{RULE_TYPE}}",
                                "{{RULE_CONDITION}}",
                                "{{EVENT_TIME_FIELD}}",
                                {{TIME_WINDOW_MINUTES}}L));

        // 5. Serialize the generated output event and write it back to Kafka.
        outputEvents
                .map({{OUTPUT_EVENT_NAME}}::toJson)
                .sinkTo(sink);

        env.execute("{{JOB_NAME}}");
    }

    private static KafkaSource<String> buildSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("{{SOURCE_TOPIC}}")
                .setGroupId("{{JOB_NAME}}-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static KafkaSink<String> buildSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("{{SINK_TOPIC}}")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();
    }

    private static WatermarkStrategy<{{INPUT_EVENT_NAME}}> buildWatermarkStrategy() {
        // Watermarks are intentionally simple in v0.2. The field name is injected by the generator.
        return WatermarkStrategy
                .<{{INPUT_EVENT_NAME}}>forBoundedOutOfOrderness(ALLOWED_LATENESS)
                .withTimestampAssigner(new EventTimeAssigner());
    }

    /**
     * Read the configured event-time field from the input record.
     *
     * <p>If parsing fails, the record timestamp is used as a fallback so the generated starter
     * stays simple and coherent.
     */
    private static final class EventTimeAssigner
            implements SerializableTimestampAssigner<{{INPUT_EVENT_NAME}}> {

        @Override
        public long extractTimestamp({{INPUT_EVENT_NAME}} element, long recordTimestamp) {
            return element.getEventTimeMillis("{{EVENT_TIME_FIELD}}", recordTimestamp);
        }
    }
}
