package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import db.postgresql.async.pginfo.PgType;
import java.lang.reflect.Array;
import static db.postgresql.async.serializers.SerializationContext.*;

public class FloatSerializer extends Serializer<Float> {
    
    public static final PgType PGTYPE =
        new PgType.Builder().name("float4").oid(700).arrayId(1021).build();

    public static final FloatSerializer instance = new FloatSerializer();

    public Class<Float> getType() { return Float.class; }
    public Class getArrayType() { return float.class; }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setFloat(ary, index, Float.parseFloat(val));
    }
    
    public Float fromString(final String str) {
        return Float.valueOf(str);
    }

    public String toString(final Float val) {
        return (null == val) ? null : val.toString();
    }
    
    public float readPrimitive(final ByteBuffer buffer, final int size) {
        return isNull(size) ? 0.0f : Float.valueOf(bufferToString(buffer, size));
    }

    public Float read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : readPrimitive(buffer, size);
    }
    
    public void writePrimitive(final ByteBuffer buffer, final float val) {
        stringToBuffer(buffer, Float.toString(val));
    }

    public void write(final ByteBuffer buffer, final Float val) {
        writePrimitive(buffer, val);
    }
}
