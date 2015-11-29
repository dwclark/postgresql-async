package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.lang.reflect.Array;
import java.util.List;
import db.postgresql.async.serializers.parsers.ArrayParser;
import static db.postgresql.async.serializers.SerializationContext.*;
import db.postgresql.async.messages.Format;

public abstract class Serializer<T> {

    public final boolean isNull(final int size) {
        return size == -1;
    }
    
    //basic information for java type identification
    public abstract Class<T> getType();
    public abstract List<String> getPgNames();
    public Class getArrayType() { return getType(); }

    //serialization for udt types
    public T fromString(String str) { throw new UnsupportedOperationException(); }
    public String toString(T o) { throw new UnsupportedOperationException(); }

    //serialization for data row
    public abstract T read(final ByteBuffer buffer, final Format format);
    public abstract void write(final ByteBuffer buffer, final T o, final Format format);

    public void place(final Object ary, final int index, final String val) {
        Array.set(ary, index, fromString(val));
    }
    
    public Object array(final ByteBuffer buffer, final int size, final char delimiter) {
        //return new ArrayParser(bufferToString(buffer, size), this, delimiter).toArray();
        throw new UnsupportedOperationException();
    }
}
