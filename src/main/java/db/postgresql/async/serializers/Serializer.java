package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.lang.reflect.Array;
import db.postgresql.async.serializers.parsers.ArrayParser;
import static db.postgresql.async.serializers.SerializationContext.*;

public abstract class Serializer<T> {

    public final boolean isNull(final int size) {
        return size == -1;
    }
    
    //basic information for java type identification
    public abstract Class<T> getType();
    public Class getArrayType() { return getType(); }

    //serialization for udt types
    public abstract T fromString(String str);
    public abstract String toString(T o);

    //serialization for data row
    public T read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : fromString(bufferToString(buffer, size));
    }

    public void write(final ByteBuffer buffer, final T o) {
        stringToBuffer(buffer, toString(o));
    }

    public void place(final Object ary, final int index, final String val) {
        Array.set(ary, index, fromString(val));
    }
    
    public Object array(final ByteBuffer buffer, final int size, final char delimiter) {
        return new ArrayParser(bufferToString(buffer, size), this, delimiter).toArray();
    }
}
