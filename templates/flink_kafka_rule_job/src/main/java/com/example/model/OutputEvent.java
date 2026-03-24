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
                key,
                ruleType,
                ruleCondition,
                matchedAt);
    }
}
