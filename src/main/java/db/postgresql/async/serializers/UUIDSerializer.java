package db.postgresql.async.serializers;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class UUIDSerializer extends Serializer<UUID> {

    public static final UUIDSerializer instance = new UUIDSerializer();
    
    public Class<UUID> getType() { return UUID.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.uuid");
    }

    public UUID fromString(final String str) {
        return UUID.fromString(str);
    }

    public String toString(final UUID val) {
        return val == null ? null : val.toString();
    }
}
