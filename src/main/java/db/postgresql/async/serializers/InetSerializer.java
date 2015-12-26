package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import db.postgresql.async.types.Inet;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class InetSerializer extends Serializer<Inet> {

    public static final InetSerializer instance = new InetSerializer();
    
    public Class<Inet> getType() { return Inet.class; }

    public List<String> getPgNames() {
        return Arrays.asList("pg_catalog.inet", "pg_catalog.cidr");
    }
    
    public Inet read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }

        if(format == Format.BINARY) {
            return new Inet(buffer);
        }
        else {
            buffer.position(buffer.position() - 4);
            return Inet.fromString(bufferToString(buffer));
        }
    }

    public void write(final ByteBuffer buffer, final Inet inet, final Format format) {
        if(inet == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> inet.toBuffer(b));
        }
        else {
            putWithSize(buffer, (b) -> stringToBuffer(b, inet.toString()));
        }
    }
}
