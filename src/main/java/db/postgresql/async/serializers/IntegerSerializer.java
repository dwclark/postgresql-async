package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId("int4")
public class IntegerSerializer extends Serializer<Integer> {

    private IntegerSerializer() { }
    
    public static final IntegerSerializer instance = new IntegerSerializer();

    public Class<Integer> getType() { return Integer.class; }
    
    public Class getArrayType() { return int.class; }

    @Override
    public void place(final Object ary, final int index, final String val) {
        Array.setInt(ary, index, Integer.parseInt(val));
    }

    public Integer fromString(final String str) {
        return Integer.valueOf(str);
    }

    public String toString(final Integer val) {
        return val == null ? null : val.toString();
    }
    
    public Integer read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : readPrimitive(buffer, size);
    }

    public int readPrimitive(final ByteBuffer buffer, final int size) {
        if(isNull(size)) {
            return 0;
        }
        else {
            return Integer.parseInt(bufferToString(buffer, size));
        }
    }

    public void write(final ByteBuffer buffer, final Integer val) {
        if(val != null) {
            writePrimitive(buffer, val);
        }
    }

    public void writePrimitive(final ByteBuffer buffer, final int val) {
        stringToBuffer(buffer, Integer.toString(val));
    }
}
