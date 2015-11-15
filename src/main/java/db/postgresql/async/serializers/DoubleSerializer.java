package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.serializers.SerializationContext.*;

public class DoubleSerializer extends Serializer<Double> {

    public static final DoubleSerializer instance = new DoubleSerializer();

    public Class<Double> getType() { return Double.class; }

    @Override
    public Class getArrayType() { return double.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.float8");
    }

    @Override
    public void place(final Object ary, final int index, final String val) {
        Array.setDouble(ary, index, Double.parseDouble(val));
    }

    public Double fromString(final String str) {
        return Double.valueOf(str);
    }

    public String toString(final Double val) {
        return (val == null) ? null : val.toString();
    }
    
    public double readPrimitive(final ByteBuffer buffer, final int size) {
        return isNull(size) ? 0.0d : Double.valueOf(bufferToString(buffer, size));
    }

    public Double read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : readPrimitive(buffer, size);
    }

    public void writePrimitive(final ByteBuffer buffer, final double val) {
        stringToBuffer(buffer, Double.toString(val));
    }

    public void write(final ByteBuffer buffer, final Double val) {
        writePrimitive(buffer, val);
    }
}
