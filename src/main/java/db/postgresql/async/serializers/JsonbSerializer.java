package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Jsonb;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class JsonbSerializer extends Serializer<Jsonb> {

    private JsonbSerializer() { }

    public static final JsonbSerializer instance = new JsonbSerializer();

    public Class<Jsonb> getType() { return Jsonb.class; }
    public Class getArrayType() { return Jsonb.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.jsonb");
    }

    public Jsonb read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Jsonb((int) buffer.get(), bufferToString(size - 1, buffer));
        }
        else {
            throw new UnsupportedOperationException("Text format not supported for jsonb type");
        }
    }

    public void write(final ByteBuffer buffer, final Jsonb j, final Format format) {
        if(j == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> {
                    b.put((byte) j.getVersion());
                    stringToBuffer(b, j.getJson());
                });
        }
        else {
            throw new UnsupportedOperationException("Text format not supported for jsonb type");
        }
    }
}
