package {{PACKAGE_NAME}};

import {{PACKAGE_NAME}}.functions.RuleProcessFunction;
import {{PACKAGE_NAME}}.model.InputEvent;
import {{PACKAGE_NAME}}.model.OutputEvent;
import java.util.Map;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuleProcessFunctionTest {

    @Test
    void emitsAnOutputEvent() throws Exception {
        RuleProcessFunction function = new RuleProcessFunction("{{RULE_EXPRESSION}}");
        KeyedProcessOperator<String, InputEvent, OutputEvent> operator =
                new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<String, InputEvent, OutputEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        event -> event.getKey("{{KEY_FIELD}}"),
                        org.apache.flink.api.common.typeinfo.Types.STRING);

        harness.open();
        harness.processElement(new InputEvent(Map.of("{{KEY_FIELD}}", "abc", "raw", "value")), 0L);

        Assertions.assertFalse(harness.extractOutputValues().isEmpty());
    }
}
