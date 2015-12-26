package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class StringSerializer extends Serializer<String> {

    private StringSerializer() { }

    public static final StringSerializer instance = new StringSerializer();

    public Class<String> getType() { return String.class; }

    public List<String> getPgNames() {
        return Arrays.asList("pg_catalog.text", "pg_catalog.varchar", "pg_catalog.xml",
                             "pg_catalog.json", "pg_catalog.char", "pg_catalog.bpchar");
    }

    public String read(final ByteBuffer buffer, final Format format) {
        return bufferToString(buffer);
    }

    public void write(final ByteBuffer buffer, final String s, final Format format) {
        putWithSize(buffer, (b) -> stringToBuffer(b, s));
    }
}
