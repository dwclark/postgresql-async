package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class OffsetTimeSerializer extends Serializer<OffsetTime> {
    
    private static final String STR = "HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final OffsetTimeSerializer instance = new OffsetTimeSerializer();
    
    public Class<OffsetTime> getType() { return OffsetTime.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.timetz");
    }

    public OffsetTime fromString(final String str) {
        final int index = str.lastIndexOf('-');
        return OffsetTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
    }

    public String toString(final OffsetTime val) {
        return val == null ? null : val.format(DATE);
    }

    public OffsetTime read(final ByteBuffer buffer, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final OffsetTime ot, final Format format) {
        throw new UnsupportedOperationException();
    }
}
