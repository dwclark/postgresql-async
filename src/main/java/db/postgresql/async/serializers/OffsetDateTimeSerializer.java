package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class OffsetDateTimeSerializer extends Serializer<OffsetDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final OffsetDateTimeSerializer instance = new OffsetDateTimeSerializer();
    
    public Class<OffsetDateTime> getType() { return OffsetDateTime.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.timestamptz");
    }

    public OffsetDateTime fromString(final String str) {
        final int index = str.lastIndexOf('-');
        return OffsetDateTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
    }

    public String toString(final OffsetDateTime val) {
        return (val == null) ? null : val.format(DATE);
    }

    public OffsetDateTime read(final ByteBuffer buffer, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final OffsetDateTime bits, final Format format) {
        throw new UnsupportedOperationException();
    }
}
