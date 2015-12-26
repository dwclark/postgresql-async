package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class BooleanSerializer extends Serializer<Boolean> {

    public static final byte T = (byte) 't';
    public static final byte F = (byte) 'f';

    public static final BooleanSerializer instance = new BooleanSerializer();

    public Class<Boolean> getType() { return Boolean.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.bool");
    }
    
    private boolean binary(final ByteBuffer buffer) {
        final byte val = buffer.get();
        return val == 1 ? true : false;
    }

    private boolean text(final ByteBuffer buffer) {
        final byte val = buffer.get();
        return val == T ? true : false;
    }
    
    public boolean readPrimitive(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return false;
        }
        else {
            return (format == BINARY) ? binary(buffer) : text(buffer);
        }
    }

    public Boolean read(final ByteBuffer buffer, final Format format) {
        if(buffer.getInt() == -1) {
            return null;
        }
        else {
            return (format == BINARY) ? binary(buffer) : text(buffer);
        }
    }

    public void writePrimitive(final ByteBuffer buffer, final boolean val, final Format format) {
        if(format == BINARY) {
            putWithSize(buffer, (b) -> b.put((byte) (val ? 1 : 0)));
        }
        else {
            putWithSize(buffer, (b) -> b.put(val ? T : F));
        }
    }

    public void write(final ByteBuffer buffer, final Boolean val, final Format format) {
        if(val == null) {
            putNull(buffer);
        }
        else {
            writePrimitive(buffer, val, format);
        }
    }
}
