package db.postgresql.async.serializers;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class BooleanSerializer extends Serializer<Boolean> {

    public static final byte T = (byte) 't';
    public static final byte F = (byte) 'f';

    public static final BooleanSerializer instance = new BooleanSerializer();

    @Override
    public Class getArrayType() { return boolean.class; }

    public Class<Boolean> getType() { return Boolean.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.bool");
    }
    
    @Override
    public void place(final Object ary, final int index, final String val) {
        Array.setBoolean(ary, index, val.charAt(0) == T ? true : false);
    }

    public Boolean fromString(final String str) {
        return str.charAt(0) == T;
    }

    public String toString(final Boolean val) {
        return val ? "t" : "f";
    }
    
    public boolean readPrimitive(final ByteBuffer buffer, final int size) {
        if(isNull(size)) {
            return false;
        }
        
        return (buffer.get() == T) ? true : false;
    }

    public Boolean read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : readPrimitive(buffer, size);
    }

    public void writePrimitive(final ByteBuffer buffer, final boolean val) {
        if(val) {
            buffer.put(T);
        }
        else {
            buffer.put(F);
        }
    }
    
    public void write(final ByteBuffer buffer, final Boolean val) {
        writePrimitive(buffer, val);
    }
}
