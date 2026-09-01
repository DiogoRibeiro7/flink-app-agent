package com.example.functions;

import com.example.model.{{INPUT_EVENT_NAME}};
import com.example.model.{{OUTPUT_EVENT_NAME}};
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Minimal keyed rule process function for `{{RULE_TYPE}}`.
 *
 * <p>This v0.1 scaffold shows where keyed state and event-time timers belong. The matching logic is
 * intentionally simple: the second event for the same key inside `{{TIME_WINDOW_MINUTES}}` minutes
 * emits one `{{OUTPUT_EVENT_NAME}}`.
 */
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
        // Keyed state belongs here: one slot per stream key for the last candidate event time.
        firstSeenEventTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("first-seen-event-time", Long.class));
    }

    @Override
    public void processElement(
            {{INPUT_EVENT_NAME}} value,
            KeyedProcessFunction<String, {{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}>.Context ctx,
            Collector<{{OUTPUT_EVENT_NAME}}> out) throws Exception {
        long eventTime = value.getEventTimeMillis(
                eventTimeField,
                ctx.timestamp() == null ? 0L : ctx.timestamp());
        Long firstSeen = firstSeenEventTime.value();

        if (firstSeen == null) {
            // 1. No prior candidate for this key. Store event-time state and register cleanup.
            firstSeenEventTime.update(eventTime);
            ctx.timerService().registerEventTimeTimer(eventTime + windowMillis());
            return;
        }

        if (eventTime - firstSeen <= windowMillis()) {
            // 2. A second matching event arrived inside the configured window. Emit output here.
            out.collect(new {{OUTPUT_EVENT_NAME}}(
                    ctx.getCurrentKey(),
                    ruleType,
                    ruleCondition,
                    eventTime));
        }

        // 3. Keep the latest event as the active candidate and move the cleanup timer forward.
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
            // Event-time cleanup logic belongs here. Stale keyed state is cleared on timer firing.
            firstSeenEventTime.clear();
        }
    }

    private long windowMillis() {
        return timeWindowMinutes * 60_000L;
    }
}
