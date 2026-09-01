package com.example.model;

/**
 * Minimal output event model for {{OUTPUT_EVENT_NAME}}.
 *
 * <p>The generated process function emits this object after a keyed match within the configured
 * window. The structure is intentionally small so it can be replaced later.
 */
public class {{OUTPUT_EVENT_NAME}} {
    private final String key;
    private final String ruleType;
    private final String ruleCondition;
    private final long matchedAt;

    public {{OUTPUT_EVENT_NAME}}(String key, String ruleType, String ruleCondition, long matchedAt) {
        this.key = key;
        this.ruleType = ruleType;
        this.ruleCondition = ruleCondition;
        this.matchedAt = matchedAt;
    }

    public String toJson() {
        return String.format(
                "{\"key\":\"%s\",\"ruleType\":\"%s\",\"ruleCondition\":\"%s\",\"matchedAt\":%d}",
                escapeJson(key),
                escapeJson(ruleType),
                escapeJson(ruleCondition),
                matchedAt);
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
