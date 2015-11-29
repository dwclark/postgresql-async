package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class EnumSerializer<E extends Enum<E>> extends Serializer<E> {
    
    private final Class<E> enumClass;
    private final String pgName;
    
    public EnumSerializer(final Class<E> enumClass, final String pgName) {
        this.enumClass = enumClass;
        this.pgName = pgName;
    }
    
    public Class<E> getType() { return enumClass; }
    
    public List<String> getPgNames() {
        return Collections.singletonList(pgName);
    }
    
    public E fromString(final String str) {
        return Enum.valueOf(enumClass, str);
    }
    
    public String toString(final E val) {
        return val.name();
    }

    public E read(final ByteBuffer buffer, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final E e, final Format format) {
        throw new UnsupportedOperationException();
    }
}
