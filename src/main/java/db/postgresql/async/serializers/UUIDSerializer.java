package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class UUIDSerializer extends Serializer<UUID> {

    public static final UUIDSerializer instance = new UUIDSerializer();
    
    public Class<UUID> getType() { return UUID.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.uuid");
    }

    public UUID read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.TEXT) {
            buffer.position(buffer.position() - 4);
            return UUID.fromString(bufferToString(buffer));
        }
        else {
            return new UUID(buffer.getLong(), buffer.getLong());
        }
    }

    public void write(final ByteBuffer buffer, final UUID u, final Format format) {
        if(u == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.TEXT) {
            putWithSize(buffer, (b) -> stringToBuffer(b, u.toString()));
        }
        else {
            putWithSize(buffer, (b) -> b.putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()));
        }
    }
}
