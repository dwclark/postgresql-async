package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.UUID;
import static db.postgresql.async.serializers.SerializationContext.*;

public class UUIDSerializer extends Serializer<UUID> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("uuid").oid(2950).arrayId(2951).build();

    public static final UUIDSerializer instance = new UUIDSerializer();
    
    public Class<UUID> getType() { return UUID.class; }

    public UUID fromString(final String str) {
        return UUID.fromString(str);
    }

    public String toString(final UUID val) {
        return val == null ? null : val.toString();
    }
}
