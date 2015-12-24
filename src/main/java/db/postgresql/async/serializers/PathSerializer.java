package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Path;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class PathSerializer extends Serializer<Path> {

    private PathSerializer() { }

    public static final PathSerializer instance = new PathSerializer();

    public Class<Path> getType() { return Path.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.path");
    }

    public Path read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Path(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of Path not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Path val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of Path not supported");
        }
    }
}
