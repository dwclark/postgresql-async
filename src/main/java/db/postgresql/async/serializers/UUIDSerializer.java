package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import java.nio.ByteBuffer;
import java.util.UUID;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId("uuid")
public class UUIDSerializer extends Serializer<UUID> {

    public static final UUIDSerializer instance = new UUIDSerializer();
    
    public Class<UUID> getType() { return UUID.class; }

    public UUID fromString(final String str) {
        return UUID.fromString(str);
    }

    public String toString(final UUID val) {
        return val == null ? null : val.toString();
    }
}
