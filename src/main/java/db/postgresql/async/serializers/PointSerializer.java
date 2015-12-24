package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Point;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class PointSerializer extends Serializer<Point> {

    private PointSerializer() { }

    public static final PointSerializer instance = new PointSerializer();

    public Class<Point> getType() { return Point.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.point");
    }

    public Point read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Point(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of Point not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Point val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of Point not supported");
        }
    }
}
