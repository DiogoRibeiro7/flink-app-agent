package {{PACKAGE_NAME}}.model;

import java.util.HashMap;
import java.util.Map;

public class InputEvent {
    private final Map<String, String> fields;

    public InputEvent(Map<String, String> fields) {
        this.fields = fields;
    }

    public static InputEvent fromJson(String raw) {
        Map<String, String> parsed = new HashMap<>();
        parsed.put("raw", raw);
        return new InputEvent(parsed);
    }

    public String getKey(String fieldName) {
        return fields.getOrDefault(fieldName, fields.getOrDefault("raw", ""));
    }

    public String getRaw() {
        return fields.getOrDefault("raw", "");
    }
}
