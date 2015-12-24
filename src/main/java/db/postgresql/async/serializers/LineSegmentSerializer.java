package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.LineSegment;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class LineSegmentSerializer extends Serializer<LineSegment> {

    private LineSegmentSerializer() { }

    public static final LineSegmentSerializer instance = new LineSegmentSerializer();

    public Class<LineSegment> getType() { return LineSegment.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.lseg");
    }

    public LineSegment read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new LineSegment(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of LineSegment not supported");
        }
    }

    public void write(final ByteBuffer buffer, final LineSegment val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of LineSegment not supported");
        }
    }
}
