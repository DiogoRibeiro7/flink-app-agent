package com.example.functions;

import com.example.model.{{INPUT_EVENT_NAME}};
import com.example.model.{{OUTPUT_EVENT_NAME}};
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class RuleProcessFunction
        extends KeyedProcessFunction<String, {{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}> {

    private final String ruleType;
    private final String ruleCondition;
    private final String eventTimeField;
    private final long timeWindowMinutes;

    private transient ValueState<Long> firstSeenEventTime;

    public RuleProcessFunction(
            String ruleType,
            String ruleCondition,
            String eventTimeField,
            long timeWindowMinutes) {
        this.ruleType = ruleType;
        this.ruleCondition = ruleCondition;
        this.eventTimeField = eventTimeField;
        this.timeWindowMinutes = timeWindowMinutes;
    }

    @Override
    public void open(Configuration parameters) {
        firstSeenEventTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("first-seen-event-time", Long.class));
    }

    @Override
    public void processElement(
            {{INPUT_EVENT_NAME}} value,
            KeyedProcessFunction<String, {{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}>.Context ctx,
            Collector<{{OUTPUT_EVENT_NAME}}> out) throws Exception {
        long eventTime = value.getEventTimeMillis(eventTimeField, ctx.timestamp() == null ? 0L : ctx.timestamp());
        Long firstSeen = firstSeenEventTime.value();

        if (firstSeen == null) {
            firstSeenEventTime.update(eventTime);

            // Register a cleanup timer for the stored first event.
            ctx.timerService().registerEventTimeTimer(eventTime + windowMillis());
            return;
        }

        if (eventTime - firstSeen <= windowMillis()) {
            out.collect(new {{OUTPUT_EVENT_NAME}}(
                    ctx.getCurrentKey(),
                    ruleType,
                    ruleCondition,
                    eventTime));
        }

        // Keep the latest event as the new candidate for the next match window.
        firstSeenEventTime.update(eventTime);
        ctx.timerService().registerEventTimeTimer(eventTime + windowMillis());
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<String, {{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}>.OnTimerContext ctx,
            Collector<{{OUTPUT_EVENT_NAME}}> out) throws Exception {
        Long firstSeen = firstSeenEventTime.value();
        if (firstSeen != null && firstSeen + windowMillis() <= timestamp) {
            // This is the place to clear keyed state once the matching window has expired.
            firstSeenEventTime.clear();
        }
    }

    private long windowMillis() {
        return timeWindowMinutes * 60_000L;
    }
}
