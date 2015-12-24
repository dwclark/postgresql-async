package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Line;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class LineSerializer extends Serializer<Line> {

    private LineSerializer() { }

    public static final LineSerializer instance = new LineSerializer();

    public Class<Line> getType() { return Line.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.line");
    }

    public Line read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Line(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of Line not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Line val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of Line not supported");
        }
    }
}
