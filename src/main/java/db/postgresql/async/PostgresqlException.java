package db.postgresql.async;

import java.util.Map;
import java.util.stream.Collectors;

public class PostgresqlException extends RuntimeException {

    private final Map<NoticeType,String> error;

    public Map<NoticeType,String> getError() {
        return error;
    }
    
    public PostgresqlException(final Map<NoticeType,String> error) {
        this.error = error;
    }

    @Override
    public String getMessage() {
        return error.entrySet().stream()
            .map((e) -> e.getKey() + " " + e.getValue())
            .collect(Collectors.joining("\n"));
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
