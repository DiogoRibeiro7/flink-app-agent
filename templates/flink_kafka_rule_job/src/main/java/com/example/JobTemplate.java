package {{PACKAGE_NAME}};

import {{PACKAGE_NAME}}.functions.RuleProcessFunction;
import {{PACKAGE_NAME}}.model.InputEvent;
import {{PACKAGE_NAME}}.model.OutputEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class {{JOB_CLASS_NAME}} {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("{{BOOTSTRAP_SERVERS}}")
                .setTopics("{{INPUT_TOPIC}}")
                .setGroupId("{{CONSUMER_GROUP}}")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("{{BOOTSTRAP_SERVERS}}")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("{{OUTPUT_TOPIC}}")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        DataStream<String> sourceStream = env.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "{{JOB_NAME}}-source");

        DataStream<InputEvent> parsedStream = sourceStream.map(InputEvent::fromJson);

        DataStream<OutputEvent> outputStream = parsedStream
                .keyBy(event -> event.getKey("{{KEY_FIELD}}"))
                .process(new RuleProcessFunction("{{RULE_EXPRESSION}}"));

        outputStream
                .map(OutputEvent::toJson)
                .sinkTo(sink);

        env.execute("{{JOB_NAME}}");
    }
}
