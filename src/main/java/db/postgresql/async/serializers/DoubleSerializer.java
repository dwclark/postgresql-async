package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.serializers.SerializationContext.*;
import static db.postgresql.async.buffers.BufferOps.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class DoubleSerializer extends Serializer<Double> {

    public static final DoubleSerializer instance = new DoubleSerializer();

    public Class<Double> getType() { return Double.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.float8");
    }

    private double binary(final ByteBuffer buffer) {
        final int size = buffer.getInt();
        return size == -1 ? 0d : buffer.getDouble();
    }

    private double text(final ByteBuffer buffer) {
        final String str = bufferToString(buffer);
        return str == null ? 0d : Double.parseDouble(str);
    }

    public double readPrimitive(final ByteBuffer buffer, final Format format) {
        return format == BINARY ? binary(buffer) : text(buffer);
    }

    public Double read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        else {
            buffer.position(buffer.position() - 4);
            return format == BINARY ? binary(buffer) : text(buffer);
        }
    }

    private void binary(final ByteBuffer buffer, final double val) {
        putWithSize(buffer, (b) -> b.putDouble(val));
    }

    private void text(final ByteBuffer buffer, final double val) {
        putWithSize(buffer, (b) -> stringToBuffer(buffer, Double.toString(val)));
    }
    
    public void writePrimitive(final ByteBuffer buffer, final double val, final Format format) {
        if(format == BINARY) {
            binary(buffer, val);
        }
        else {
            text(buffer, val);
        }
    }

    public void write(final ByteBuffer buffer, final Double val, final Format format) {
        if(val == null) {
            putNull(buffer);
        }
        else {
            writePrimitive(buffer, val, format);
        }
    }
}
