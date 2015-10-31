package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.lang.reflect.Array;

public abstract class Serializer<T> {

    public static final int NULL_LENGTH = -1;

    public static boolean isNull(final int size) {
        return size == NULL_LENGTH;
    }
    
    //basic information for java type identification
    public abstract Class<T> getType();
    public Class getArrayType() { return getType(); }

    //serialization for udt types
    public abstract T fromString(String str);
    public abstract String toString(T o);

    //serialization for data row
    public abstract T read(ByteBuffer buffer, int size);
    public abstract void write(ByteBuffer buffer, T val);

    public void putArray(final Object ary, final int index, final String val) {
        Array.set(ary, index, fromString(val));
    }
    
    public Object readArray(final ByteBuffer buffer, final int size, final char delimiter) {
        throw new UnsupportedOperationException();
        //return new ArrayParser(str(stream, size), this, delimiter).toArray();
    }
}
