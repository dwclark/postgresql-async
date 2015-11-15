package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;

import static db.postgresql.async.serializers.SerializationContext.*;

public class LongSerializer extends Serializer<Long> {

    public static final LongSerializer instance = new LongSerializer();

    public Class<Long> getType() { return Long.class; }
    public Class getArrayType() { return long.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.int8");
    }

    @Override
    public void place(final Object ary, final int index, final String val) {
        Array.setLong(ary, index, Long.parseLong(val));
    }

    public Long fromString(final String str) {
        return Long.valueOf(str);
    }

    public String toString(final Long val) {
        return val == null ? null : val.toString();
    }
    
    public Long read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : readPrimitive(buffer, size);
    }

    public long readPrimitive(final ByteBuffer buffer, final int size) {
        if(isNull(size)) {
            return 0L;
        }
        else {
            return Long.parseLong(bufferToString(buffer, size));
        }
    }

    public void writePrimitive(final ByteBuffer buffer, final long val) {
        stringToBuffer(buffer, Long.toString(val));
    }

    public void write(final ByteBuffer buffer, final Long val) {
        writePrimitive(buffer, val);
    }
}
