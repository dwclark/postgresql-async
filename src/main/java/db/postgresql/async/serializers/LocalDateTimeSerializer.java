package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.PostgresDateTime.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalDateTimeSerializer instance = new LocalDateTimeSerializer();

    public Class<LocalDateTime> getType() { return LocalDateTime.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.timestamp");
    }

    public LocalDateTime fromString(final String str) {
        return LocalDateTime.parse(str + "000", DATE);
    }

    public String toString(final LocalDateTime val) {
        return val == null ? null : val.format(DATE);
    }

    public LocalDateTime read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }

        if(format == BINARY) {
            return toLocalDateTime(buffer.getLong());
        }
        else {
            buffer.position(buffer.position() - 4);
            return fromString(bufferToString(buffer));
        }
    }

    public void write(final ByteBuffer buffer, final LocalDateTime date, final Format format) {
        if(date == null) {
            putNull(buffer);
            return;
        }

        if(format == BINARY) {
            putWithSize(buffer, (b) -> b.putLong(toTimestamp(date)));
        }
        else {
            putWithSize(buffer, (b) -> stringToBuffer(b, toString(date)));
        }
    }
}
