package db.postgresql.async;

import java.util.Map;

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
        final StringBuilder sb = new StringBuilder(error.size() * 96);
        for(Map.Entry<NoticeType,String> entry : error.entrySet()) {
            sb.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
        }

        return sb.substring(0, sb.length() - 1);
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
