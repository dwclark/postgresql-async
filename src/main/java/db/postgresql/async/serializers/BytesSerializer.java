package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Collections;
import static db.postgresql.async.buffers.BufferOps.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class BytesSerializer extends Serializer<byte[]> {

    public static final BytesSerializer instance = new BytesSerializer();

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.bytea");
    }
    
    public Class<byte[]> getType() { return byte[].class; }

    private byte[] binary(final ByteBuffer buffer) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        else {
            byte[] ret = new byte[size];
            buffer.get(ret);
            return ret;
        }
    }

    private byte[] text(final ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }
    
    public byte[] read(final ByteBuffer buffer, final Format format) {
        return format == BINARY ? binary(buffer) : text(buffer);
    }

    private void binary(final ByteBuffer buffer, final byte[] bytes) {
        putWithSize(buffer, (b) -> buffer.put(bytes));
    }

    private void text(final ByteBuffer buffer, final byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final byte[] bytes, final Format format) {
        if(format == BINARY) {
            binary(buffer, bytes);
        }
        else {
            text(buffer, bytes);
        }
    }
}
