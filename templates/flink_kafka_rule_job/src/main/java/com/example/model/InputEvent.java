package com.example.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Placeholder input event for {{INPUT_EVENT_NAME}}.
 *
 * <p>The generated template uses a minimal parser so the wiring stays understandable. Replace this
 * with a real schema-aware parser when integrating with actual Kafka payloads.
 */
public class {{INPUT_EVENT_NAME}} {
    private final Map<String, String> fields;
    private final String rawPayload;

    public {{INPUT_EVENT_NAME}}(Map<String, String> fields, String rawPayload) {
        this.fields = new HashMap<>(fields);
        this.rawPayload = rawPayload;
    }

    /**
     * Parse a simple comma-separated `key=value` payload into a map-backed event.
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
