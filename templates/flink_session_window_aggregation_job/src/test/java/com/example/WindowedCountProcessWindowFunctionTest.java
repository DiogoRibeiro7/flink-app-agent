package com.example;

import com.example.model.{{OUTPUT_EVENT_NAME}};
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WindowedCountProcessWindowFunctionTest {

    @Test
    void outputEventSerializesAggregationMetadata() {
        {{OUTPUT_EVENT_NAME}} event = new {{OUTPUT_EVENT_NAME}}(
                "device-1",
                0L,
                300000L,
                4L,
                "{{RULE_TYPE}}",
                "{{RULE_CONDITION}}");

        String payload = event.toJson();

        Assertions.assertTrue(payload.contains("\"count\":4"));
        Assertions.assertTrue(payload.contains("{{RULE_TYPE}}"));
        Assertions.assertTrue(payload.contains("{{RULE_CONDITION}}"));
    }

    @Test
    void outputEventEscapesJsonStringValues() {
        {{OUTPUT_EVENT_NAME}} event = new {{OUTPUT_EVENT_NAME}}(
                "device-\"1\\west\nline",
                0L,
                300000L,
                1L,
                "{{RULE_TYPE}}",
                "quoted \"description\"\\path\nnext");

        String payload = event.toJson();

        Assertions.assertTrue(payload.contains("device-\\\"1\\\\west\\nline"));
        Assertions.assertTrue(payload.contains("quoted \\\"description\\\"\\\\path\\nnext"));
    }
}
