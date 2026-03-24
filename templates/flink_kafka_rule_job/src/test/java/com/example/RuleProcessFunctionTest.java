package com.example;

import com.example.functions.RuleProcessFunction;
import com.example.model.{{INPUT_EVENT_NAME}};
import com.example.model.{{OUTPUT_EVENT_NAME}};
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Small scaffold test for the generated keyed rule function.
 *
 * <p>This is not a full integration test. It verifies that two events for the same key inside the
 * configured window can produce an output record.
 */
public class RuleProcessFunctionTest {

    @Test
    void emitsOutputWhenTwoEventsArriveWithinWindow() throws Exception {
        RuleProcessFunction function = new RuleProcessFunction(
                "{{RULE_TYPE}}",
                "{{RULE_CONDITION}}",
                "{{EVENT_TIME_FIELD}}",
                {{TIME_WINDOW_MINUTES}}L);
        KeyedProcessOperator<String, {{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}> operator =
                new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<String, {{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        event -> event.getField("{{KEY_BY}}"),
                        Types.STRING);

        harness.open();
        harness.processElement(
                new {{INPUT_EVENT_NAME}}(
                        Map.of("{{KEY_BY}}", "acct-1", "{{EVENT_TIME_FIELD}}", "1000"),
                        "{{KEY_BY}}=acct-1,{{EVENT_TIME_FIELD}}=1000"),
                1_000L);
        harness.processElement(
                new {{INPUT_EVENT_NAME}}(
                        Map.of("{{KEY_BY}}", "acct-1", "{{EVENT_TIME_FIELD}}", "2000"),
                        "{{KEY_BY}}=acct-1,{{EVENT_TIME_FIELD}}=2000"),
                2_000L);

        Assertions.assertFalse(harness.extractOutputValues().isEmpty());

        StreamRecord<{{OUTPUT_EVENT_NAME}}> record =
                (StreamRecord<{{OUTPUT_EVENT_NAME}}>) harness.getOutput().peek();
        Assertions.assertNotNull(record);
        Assertions.assertTrue(record.getValue().toJson().contains("{{RULE_TYPE}}"));
        Assertions.assertTrue(record.getValue().toJson().contains("{{RULE_CONDITION}}"));
    }
}
