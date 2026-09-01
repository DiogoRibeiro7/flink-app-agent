package com.example.model;

public class {{OUTPUT_EVENT_NAME}} {
    private final String key;
    private final long windowStart;
    private final long windowEnd;
    private final long count;
    private final String aggregationType;
    private final String aggregationDescription;

    public {{OUTPUT_EVENT_NAME}}(
            String key,
            long windowStart,
            long windowEnd,
            long count,
            String aggregationType,
            String aggregationDescription) {
        this.key = key;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.aggregationType = aggregationType;
        this.aggregationDescription = aggregationDescription;
    }

    public String toJson() {
        return String.format(
                "{\"key\":\"%s\",\"windowStart\":%d,\"windowEnd\":%d,\"count\":%d,\"aggregationType\":\"%s\",\"aggregationDescription\":\"%s\"}",
                key,
                windowStart,
                windowEnd,
                count,
                aggregationType,
                aggregationDescription);
    }
}
