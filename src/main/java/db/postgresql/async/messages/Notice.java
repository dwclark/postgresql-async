package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.EnumMap;
import java.util.Collections;

import db.postgresql.async.NoticeType;
import db.postgresql.async.PostgresqlException;

public class Notice extends Response {

    private final Map<NoticeType,String> messages;
    public Map<NoticeType,String> getMessages() { return messages; }

    public Notice(final ByteBuffer buffer) {
        super(buffer);

        final Map<NoticeType,String> map = new EnumMap<>(NoticeType.class);
        byte byteType;
        while((byteType = buffer.get()) != NULL) {
            map.put(NoticeType.find(byteType), ascii(buffer));
        }

        this.messages = Collections.unmodifiableMap(map);
    }

    public PostgresqlException toException() {
        return new PostgresqlException(messages);
    }

    public String getCode() {
        return messages.get(NoticeType.Code);
    }

    public boolean isSuccess() {
        return getCode().startsWith("00");
    }

    public boolean isWarning() {
        return getCode().startsWith("01") || getCode().startsWith("02");
    }

    public boolean isError() {
        return !isSuccess() && !isWarning();
    }
}
