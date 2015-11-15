package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static db.postgresql.async.serializers.SerializationContext.*;

public class ShortSerializer extends Serializer<Short> {

    public static final ShortSerializer instance = new ShortSerializer();

    public Class<Short> getType() { return Short.class; }
    public Class getArrayType() { return short.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.int2");
    }
    
    @Override
    public void place(final Object ary, final int index, final String val) {
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
