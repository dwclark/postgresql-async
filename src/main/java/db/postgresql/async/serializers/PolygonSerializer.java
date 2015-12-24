package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Polygon;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class PolygonSerializer extends Serializer<Polygon> {

    private PolygonSerializer() { }

    public static final PolygonSerializer instance = new PolygonSerializer();

    public Class<Polygon> getType() { return Polygon.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.box");
    }

    public Polygon read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Polygon(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of Polygon not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Polygon val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of Polygon not supported");
        }
    }
}
