package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import static db.postgresql.async.serializers.PostgresDateTime.*;

public class LocalDateSerializer extends Serializer<LocalDate> {

    private static final String STR = "uuuu-MM-dd";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public Class<LocalDate> getType() { return LocalDate.class; }

    @Override
    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.date");
    }

    public static final LocalDateSerializer instance = new LocalDateSerializer();
    
    public LocalDate fromString(final String str) {
        return LocalDate.parse(str, DATE);
    }
    
    public String toString(final LocalDate val) {
        return (val == null) ? null : val.format(DATE);
    }

    public LocalDate read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == BINARY) {
            return toLocalDate((long) buffer.getInt());
        }
        else {
            buffer.position(buffer.position() - 4);
            return LocalDate.parse(bufferToString(buffer), DATE);
        }
    }

    public void write(final ByteBuffer buffer, final LocalDate date, final Format format) {
        if(date == null) {
            putNull(buffer);
            return;
        }
        
        if(format == BINARY) {
            putWithSize(buffer, (b) -> b.putInt((int) toDay(date)));
        }
        else {
            putWithSize(buffer, (b) -> stringToBuffer(b, date.format(DATE)));
        }
    }
}
