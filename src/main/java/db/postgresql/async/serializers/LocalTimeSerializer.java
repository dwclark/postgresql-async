package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import static db.postgresql.async.serializers.PostgresDateTime.*;

public class LocalTimeSerializer extends Serializer<LocalTime> {

    private static final String STR = "HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalTimeSerializer instance = new LocalTimeSerializer();

    public Class<LocalTime> getType() { return LocalTime.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.time");
    }
    
    public LocalTime fromString(final String str) {
        return LocalTime.parse(str + "000", DATE);
    }

    public String toString(final LocalTime val) {
        return val == null ? null : val.format(DATE);
    }

    public LocalTime read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }

        if(format == BINARY) {
            return toLocalTime(buffer.getLong());
        }
        else {
            buffer.position(buffer.position() - 4);
            return LocalTime.parse(bufferToString(buffer) + "000", DATE);
        }
    }
    
    public void write(final ByteBuffer buffer, final LocalTime time, final Format format) {
        if(time == null) {
            putNull(buffer);
            return;
        }

        if(format == BINARY) {
            putWithSize(buffer, (b) -> b.putLong(toTime(time)));
        }
        else {
            putWithSize(buffer, (b) -> stringToBuffer(b, time.format(DATE)));
        }
    }
}
