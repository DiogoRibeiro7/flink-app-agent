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
 * Generated starter job for {{JOB_NAME}}.
 *
 * <p>The placeholder values in this file are injected by `flink-app-agent`. The generated code
 * keeps the control flow explicit so the user can see where Kafka I/O, timestamp extraction,
 * watermarking, keying, and keyed rule evaluation happen.
 */
final class JobTemplate {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Duration WATERMARK_OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    private JobTemplate() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = buildSource();
        KafkaSink<String> sink = buildSink();

        DataStream<String> rawEvents = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "{{JOB_NAME}}-source");

        DataStream<{{INPUT_EVENT_NAME}}> parsedEvents = rawEvents.map({{INPUT_EVENT_NAME}}::fromRaw);

        WatermarkStrategy<{{INPUT_EVENT_NAME}}> watermarkStrategy =
                WatermarkStrategy
                        .<{{INPUT_EVENT_NAME}}>forBoundedOutOfOrderness(WATERMARK_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(new EventTimestampAssigner());

        DataStream<{{OUTPUT_EVENT_NAME}}> outputEvents = parsedEvents
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(event -> event.getField("{{KEY_BY}}"))
                .process(new RuleProcessFunction(
                        "{{RULE_TYPE}}",
                        "{{RULE_CONDITION}}",
                        "{{EVENT_TIME_FIELD}}",
                        {{TIME_WINDOW_MINUTES}}L));

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

    /**
     * Extract the event time configured through `{{EVENT_TIME_FIELD}}`.
     *
     * <p>If the field cannot be parsed, the record timestamp is used as a fallback so the
     * generated starter remains runnable.
     */
    private static final class EventTimestampAssigner
            implements SerializableTimestampAssigner<{{INPUT_EVENT_NAME}}> {

        @Override
        public long extractTimestamp({{INPUT_EVENT_NAME}} element, long recordTimestamp) {
            return element.getEventTimeMillis("{{EVENT_TIME_FIELD}}", recordTimestamp);
        }
    }
}
