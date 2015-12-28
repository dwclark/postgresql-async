package db.postgresql.async;

import java.nio.ByteBuffer;

public class Mapping {
    
    @FunctionalInterface
    public interface Reader {
        Object read(int size, ByteBuffer buffer, int oid);
    }
    
    @FunctionalInterface
    public interface Writer {
        void write(ByteBuffer buffer, Object o);
    }

    public final Class type;
    public final String name;
    public final Writer writer;
    public final Reader reader;

    public Mapping(final Class type, final String name, final Writer writer, final Reader reader) {
        this.type = type;
        this.name = name;
        this.writer = writer;
        this.reader = reader;
    }

    public static Mapping map(final Class type, final String name, final Writer writer, final Reader reader) {
        return new Mapping(type, name, writer, reader);
    }
}
