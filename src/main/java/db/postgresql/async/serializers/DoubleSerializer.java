package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import db.postgresql.async.pginfo.PgType;
import java.lang.reflect.Array;
import static db.postgresql.async.serializers.SerializationContext.*;

public class DoubleSerializer extends Serializer<Double> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("float8").oid(701).arrayId(1022).build();

    public static final DoubleSerializer instance = new DoubleSerializer();

    public Class<Double> getType() { return Double.class; }
    public Class getArrayType() { return double.class; }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
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
