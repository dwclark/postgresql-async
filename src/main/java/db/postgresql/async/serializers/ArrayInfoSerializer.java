package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.types.ArrayInfo;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class ArrayInfoSerializer extends Serializer<ArrayInfo> {

    private ArrayInfoSerializer() { }

    public static final ArrayInfoSerializer instance = new ArrayInfoSerializer();

    public Class<ArrayInfo> getType() { return ArrayInfo.class; }

    public List<String> getPgNames() {
        return Collections.emptyList(); //handles all array types
    }

    public ArrayInfo read(final ByteBuffer buffer, final Format format) {
        return read(buffer, Object.class, format);
    }

    public ArrayInfo read(final ByteBuffer buffer, final Class type, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new ArrayInfo(buffer, type);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of arrays not supported");
        }
    }

    public void write(final ByteBuffer buffer, final ArrayInfo val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of arrays not supported");
        }
    }
}
