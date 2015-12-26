package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.lang.reflect.Array;
import java.util.List;
import static db.postgresql.async.serializers.SerializationContext.*;
import db.postgresql.async.messages.Format;
import db.postgresql.async.types.ArrayInfo;

public abstract class Serializer<T> {

    public final boolean isNull(final int size) {
        return size == -1;
    }
    
    //basic information for java type identification
    public abstract Class<T> getType();
    public abstract List<String> getPgNames();

    //serialization for data row
    public abstract T read(final ByteBuffer buffer, final Format format);
    public abstract void write(final ByteBuffer buffer, final T o, final Format format);
}
