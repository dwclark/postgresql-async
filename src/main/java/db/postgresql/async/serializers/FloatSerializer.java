package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.serializers.SerializationContext.*;
import static db.postgresql.async.buffers.BufferOps.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class FloatSerializer extends Serializer<Float> {

    public static final FloatSerializer instance = new FloatSerializer();

    public Class<Float> getType() { return Float.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.float4");
    }

    private float binary(final ByteBuffer buffer) {
        final int size = buffer.getInt();
        return size == -1 ? 0f : buffer.getFloat();
    }

    private float text(final ByteBuffer buffer) {
        final String str = bufferToString(buffer);
        return str == null ? 0f : Float.parseFloat(str);
    }
    
    public float readPrimitive(final ByteBuffer buffer, final Format format) {
        return format == BINARY ? binary(buffer) : text(buffer);
    }

    public Float read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        else {
            buffer.position(buffer.position() - 4);
            return readPrimitive(buffer, format);
        }
    }

    private void binary(final ByteBuffer buffer, final float val) {
        putWithSize(buffer, (b) -> b.putFloat(val));
    }

    private void text(final ByteBuffer buffer, final float val) {
        putWithSize(buffer, (b) -> stringToBuffer(b, Float.toString(val)));
    }
    
    public void writePrimitive(final ByteBuffer buffer, final float val, final Format format) {
        if(format == BINARY) {
            binary(buffer, val);
        }
        else {
            text(buffer, val);
        }
    }

    public void write(final ByteBuffer buffer, final Float val, final Format format) {
        if(val == null) {
            putNull(buffer);
        }
        else {
            writePrimitive(buffer, val, format);
        }
    }
}
