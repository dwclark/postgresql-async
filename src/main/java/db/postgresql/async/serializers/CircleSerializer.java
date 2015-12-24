package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Circle;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class CircleSerializer extends Serializer<Circle> {

    private CircleSerializer() { }

    public static final CircleSerializer instance = new CircleSerializer();

    public Class<Circle> getType() { return Circle.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.circle");
    }

    public Circle read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Circle(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of Circle not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Circle val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of Circle not supported");
        }
    }
}
