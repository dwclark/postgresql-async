package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.Box;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class BoxSerializer extends Serializer<Box> {

    private BoxSerializer() { }

    public static final BoxSerializer instance = new BoxSerializer();

    public Class<Box> getType() { return Box.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.box");
    }

    public Box read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Box(buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of Box not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Box val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of Box not supported");
        }
    }
}
