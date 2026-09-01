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
                escapeJson(key),
                windowStart,
                windowEnd,
                count,
                escapeJson(aggregationType),
                escapeJson(aggregationDescription));
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (character < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) character));
                    } else {
                        escaped.append(character);
                    }
            }
        }
        return escaped.toString();
    }
}
