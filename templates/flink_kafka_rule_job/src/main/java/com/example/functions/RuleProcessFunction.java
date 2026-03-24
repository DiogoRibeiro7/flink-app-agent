package {{PACKAGE_NAME}}.functions;

import {{PACKAGE_NAME}}.model.InputEvent;
import {{PACKAGE_NAME}}.model.OutputEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class RuleProcessFunction extends KeyedProcessFunction<String, InputEvent, OutputEvent> {
    private final String ruleExpression;

    public RuleProcessFunction(String ruleExpression) {
        this.ruleExpression = ruleExpression;
    }

    @Override
    public void processElement(
            InputEvent value,
            KeyedProcessFunction<String, InputEvent, OutputEvent>.Context ctx,
            Collector<OutputEvent> out) {
        String key = ctx.getCurrentKey();

        // The first version keeps rule evaluation intentionally simple.
        if (!value.getRaw().isBlank()) {
            out.collect(new OutputEvent(key, "MATCH", "Rule matched: " + ruleExpression));
        }
    }
}
