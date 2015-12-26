package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class IntegerSerializer extends Serializer<Integer> {

    private IntegerSerializer() { }
    
    public static final IntegerSerializer instance = new IntegerSerializer();

    public Class<Integer> getType() { return Integer.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.int4");
    }

    private int binary(final ByteBuffer buffer) {
        return buffer.getInt() == -1 ? 0 : buffer.getInt();
    }

    private int text(final ByteBuffer buffer) {
        final String str = bufferToString(buffer);
        return str == null ? 0 : Integer.parseInt(str);
    }
    
    public Integer read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        else {
            buffer.position(buffer.position() - 4);
            return format == BINARY ? binary(buffer) : text(buffer);
        }
    }

    public int readPrimitive(final ByteBuffer buffer, final Format format) {
        return format == BINARY ? binary(buffer) : text(buffer);
    }

    private void binary(final ByteBuffer buffer, final int val) {
        putWithSize(buffer, (b) -> b.putInt(val));
    }

    private void text(final ByteBuffer buffer, final int val) {
        putWithSize(buffer, (b) -> stringToBuffer(buffer, Integer.toString(val)));
    }
    
    public void write(final ByteBuffer buffer, final Integer val, final Format format) {
        if(val == null) {
            putNull(buffer);
        }
        else {
            writePrimitive(buffer, val, format);
        }
    }

    public void writePrimitive(final ByteBuffer buffer, final int val, final Format format) {
        if(format == BINARY) {
            binary(buffer, val);
        }
        else {
            text(buffer, val);
        }
    }
}
