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
}
