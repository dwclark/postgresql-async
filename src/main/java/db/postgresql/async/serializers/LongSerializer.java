package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class LongSerializer extends Serializer<Long> {

    public static final LongSerializer instance = new LongSerializer();

    public Class<Long> getType() { return Long.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.int8");
    }

    private long binary(final ByteBuffer buffer) {
        return buffer.getInt() == -1 ? 0L : buffer.getLong();
    }

    private long text(final ByteBuffer buffer) {
        final String str = bufferToString(buffer);
        return str == null ? 0L : Long.parseLong(str);
    }
    
    public Long read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        else {
            buffer.position(buffer.position() - 4);
            return format == BINARY ? binary(buffer) : text(buffer);
        }
    }

    public long readPrimitive(final ByteBuffer buffer, final Format format) {
        return format == BINARY ? binary(buffer) : text(buffer);
    }

    private void binary(final ByteBuffer buffer, final long val) {
        putWithSize(buffer, (b) -> b.putLong(val));
    }

    private void text(final ByteBuffer buffer, final long val) {
        putWithSize(buffer, (b) -> stringToBuffer(b, Long.toString(val)));
    }
    
    public void writePrimitive(final ByteBuffer buffer, final long val, final Format format) {
        if(format == BINARY) {
            binary(buffer, val);
        }
        else {
            text(buffer, val);
        }
    }

    public void write(final ByteBuffer buffer, final Long val, final Format format) {
        if(val == null) {
            putNull(buffer);
        }
        else {
            writePrimitive(buffer, val, format);
        }
    }
}
