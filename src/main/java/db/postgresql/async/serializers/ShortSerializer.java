package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class ShortSerializer extends Serializer<Short> {

    public static final ShortSerializer instance = new ShortSerializer();

    public Class<Short> getType() { return Short.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.int2");
    }
    
    private short binary(final ByteBuffer buffer) {
        return buffer.getInt() == -1 ? (short) 0 : buffer.getShort();
    }

    private short text(final ByteBuffer buffer) {
        final String str = bufferToString(buffer);
        return str == null ? (short) 0 : Short.parseShort(str);
    }
    
    public short readPrimitive(final ByteBuffer buffer, final Format format) {
        return format == BINARY ? binary(buffer) : text(buffer);
    }

    public Short read(final ByteBuffer buffer, final Format format) {
        if(buffer.getInt() == -1) {
            return null;
        }
        else {
            buffer.position(buffer.position() - 4);
            return readPrimitive(buffer, format);
        }
    }

    private void binary(final ByteBuffer buffer, final short val) {
        putWithSize(buffer, (b) -> b.putShort(val));
    }

    private void text(final ByteBuffer buffer, final short val) {
        putWithSize(buffer, (b) -> stringToBuffer(b, Short.toString(val)));
    }

    public void writePrimitive(final ByteBuffer buffer, final short val, final Format format) {
        if(format == BINARY) {
            binary(buffer, val);
        }
        else {
            text(buffer, val);
        }
    }

    public void write(final ByteBuffer buffer, final Short val, final Format format) {
        if(val == null) {
            putNull(buffer);
        }
        else {
            writePrimitive(buffer, val, format);
        }
    }
}
