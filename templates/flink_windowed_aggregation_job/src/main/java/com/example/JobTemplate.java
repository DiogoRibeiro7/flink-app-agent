package com.example;

import com.example.functions.WindowedCountProcessWindowFunction;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Generated starter job for {{JOB_NAME}}.
 *
 * <p>This template shows the windowed aggregation family. Placeholder values are injected by
 * `flink-app-agent`.
 */
final class JobTemplate {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private JobTemplate() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("{{SOURCE_TOPIC}}")
                .setGroupId("{{JOB_NAME}}-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("{{SINK_TOPIC}}")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        WatermarkStrategy<{{INPUT_EVENT_NAME}}> watermarkStrategy =
                WatermarkStrategy
                        .<{{INPUT_EVENT_NAME}}>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner(new EventTimestampAssigner());

        SingleOutputStreamOperator<{{OUTPUT_EVENT_NAME}}> aggregatedEvents = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "{{JOB_NAME}}-source")
                .map({{INPUT_EVENT_NAME}}::fromRaw)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(event -> event.getField("{{KEY_BY}}"))
                .window(TumblingEventTimeWindows.of(Time.minutes({{TIME_WINDOW_MINUTES}})))
                .process(new WindowedCountProcessWindowFunction("{{RULE_TYPE}}", "{{RULE_CONDITION}}"));

        aggregatedEvents
                .map({{OUTPUT_EVENT_NAME}}::toJson)
                .sinkTo(sink);

        env.execute("{{JOB_NAME}}");
    }

    private static final class EventTimestampAssigner
            implements SerializableTimestampAssigner<{{INPUT_EVENT_NAME}}> {

        @Override
        public long extractTimestamp({{INPUT_EVENT_NAME}} element, long recordTimestamp) {
            return element.getEventTimeMillis("{{EVENT_TIME_FIELD}}", recordTimestamp);
        }
    }
}
