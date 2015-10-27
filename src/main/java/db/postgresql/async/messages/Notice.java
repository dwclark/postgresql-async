package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collections;

public class Notice extends Response {

    private final Map<NoticeType,String> messages;
    public Map<NoticeType,String> getMessages() { return messages; }

    public Notice(final ByteBuffer buffer) {
        super(buffer);

        final Map<NoticeType,String> map = new LinkedHashMap<>();
        byte byteType;
        while((byteType = buffer.get()) != NULL) {
            map.put(NoticeType.find(byteType), ascii(buffer));
        }

        this.messages = Collections.unmodifiableMap(map);
    }
}
