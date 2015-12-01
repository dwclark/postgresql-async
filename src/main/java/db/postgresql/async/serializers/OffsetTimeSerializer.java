package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.OffsetTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

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
g
    public String toString(final OffsetTime val) {
        return val == null ? null : val.format(DATE);
    }

    public OffsetTime read(final ByteBuffer buffer, final Format format) {
        if(buffer.getInt() == -1) {
            return null;
        }
        else {
            if(format == BINARY) {
                final LocalTime lt = LocalTime.ofNanoOfDay(buffer.getLong() * 1000L);
                final ZoneOffset offset = ZoneOffset.ofTotalSeconds(buffer.getInt());
                return OffsetTime.of(lt, offset);
            }
            else {
                buffer.position(buffer.position() - 4);
                return fromString(bufferToString(buffer));
            }
        }
    }

    public void write(final ByteBuffer buffer, final OffsetTime ot, final Format format) {
        if(ot == null) {
            putNull(buffer);
        }
        else {
            if(format == BINARY) {
                putWithSize(buffer, (b) -> {
                        b.putLong(ot.toLocalTime().toNanoOfDay() / 1000L);
                        b.putInt(ot.getOffset().getTotalSeconds()); });
            }
            else {
                putWithSize(buffer, (b) -> stringToBuffer(b, ot.format(DATE)));
            }
        }
    }
}
