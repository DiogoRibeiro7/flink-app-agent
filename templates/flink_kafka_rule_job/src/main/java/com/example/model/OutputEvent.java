package com.example.model;

/**
 * Placeholder output event for {{OUTPUT_EVENT_NAME}}.
 *
 * <p>The generated job emits this model after the keyed rule has matched. The structure is small
 * on purpose so it is easy to replace with a domain-specific schema later.
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
