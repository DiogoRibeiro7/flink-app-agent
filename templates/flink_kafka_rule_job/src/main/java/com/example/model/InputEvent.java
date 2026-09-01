package com.example.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Minimal input event model for {{INPUT_EVENT_NAME}}.
 *
 * <p>The v0.1 template uses a small map-backed payload so the generated project is easy to read.
 * Replace this parser with a real schema-aware implementation later.
 */
public class {{INPUT_EVENT_NAME}} {
    private final Map<String, String> fields;
    private final String rawPayload;

    public {{INPUT_EVENT_NAME}}(Map<String, String> fields, String rawPayload) {
        this.fields = new HashMap<>(fields);
        this.rawPayload = rawPayload;
    }

    /**
     * Parse a very small comma-separated `key=value` payload.
     */
    public static {{INPUT_EVENT_NAME}} fromRaw(String rawPayload) {
        Map<String, String> parsedFields = new HashMap<>();
        for (String pair : rawPayload.split(",")) {
            String[] tokens = pair.split("=", 2);
            if (tokens.length == 2) {
                parsedFields.put(tokens[0].trim(), tokens[1].trim());
            }
        }
        return new {{INPUT_EVENT_NAME}}(parsedFields, rawPayload);
    }

    public String getField(String fieldName) {
        return fields.getOrDefault(fieldName, "");
    }

    public long getEventTimeMillis(String fieldName, long fallbackTimestamp) {
        String value = fields.get(fieldName);
        if (value == null || value.isBlank()) {
            return fallbackTimestamp;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return fallbackTimestamp;
        }
    }

    public Map<String, String> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    public String getRawPayload() {
        return rawPayload;
    }
}
