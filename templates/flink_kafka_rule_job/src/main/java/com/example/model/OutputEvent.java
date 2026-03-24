package {{PACKAGE_NAME}}.model;

public class OutputEvent {
    private final String key;
    private final String status;
    private final String message;

    public OutputEvent(String key, String status, String message) {
        this.key = key;
        this.status = status;
        this.message = message;
    }

    public String toJson() {
        return String.format(
                "{\"key\":\"%s\",\"status\":\"%s\",\"message\":\"%s\"}",
                key,
                status,
                message);
    }
}
