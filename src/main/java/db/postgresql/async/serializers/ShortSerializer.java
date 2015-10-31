package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import db.postgresql.async.pginfo.PgType;
import java.lang.reflect.Array;
import static db.postgresql.async.serializers.SerializationContext.*;

public class ShortSerializer extends Serializer<Short> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("int2").oid(21).arrayId(1005).build();

    public static final ShortSerializer instance = new ShortSerializer();

    public Class<Short> getType() { return Short.class; }
    public Class getArrayType() { return short.class; }
    
    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setShort(ary, index, Short.parseShort(val));
    }
    
    public Short fromString(final String str) {
        return Short.valueOf(str);
    }

    public String toString(final Short val) {
        return val.toString();
    }
    
    public short readPrimitive(final ByteBuffer buffer, final int size) {
        if(isNull(size)) {
            return 0;
        }
        else {
            return Short.parseShort(bufferToString(buffer, size));
        }
    }

    public Short read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : readPrimitive(buffer, size);
    }

    public void writePrimitive(final ByteBuffer buffer, final short val) {
        stringToBuffer(buffer, Short.toString(val));
    }

    public void write(final ByteBuffer buffer, final Short val) {
        writePrimitive(buffer, val);
    }
}
