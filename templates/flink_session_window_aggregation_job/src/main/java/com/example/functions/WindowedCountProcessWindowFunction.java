package com.example.functions;

import com.example.model.{{INPUT_EVENT_NAME}};
import com.example.model.{{OUTPUT_EVENT_NAME}};
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Minimal process window function for keyed window-count aggregation.
 */
public class WindowedCountProcessWindowFunction extends ProcessWindowFunction<
        {{INPUT_EVENT_NAME}},
        {{OUTPUT_EVENT_NAME}},
        String,
        TimeWindow> {

    private final String aggregationType;
    private final String aggregationDescription;

    public WindowedCountProcessWindowFunction(String aggregationType, String aggregationDescription) {
        this.aggregationType = aggregationType;
        this.aggregationDescription = aggregationDescription;
    }

    @Override
    public void process(
            String key,
            ProcessWindowFunction<{{INPUT_EVENT_NAME}}, {{OUTPUT_EVENT_NAME}}, String, TimeWindow>.Context context,
            Iterable<{{INPUT_EVENT_NAME}}> elements,
            Collector<{{OUTPUT_EVENT_NAME}}> out) {
        long count = 0L;
        for ({{INPUT_EVENT_NAME}} ignored : elements) {
            count += 1L;
        }

        out.collect(new {{OUTPUT_EVENT_NAME}}(
                key,
                context.window().getStart(),
                context.window().getEnd(),
                count,
                aggregationType,
                aggregationDescription));
    }
}
